-module(producer).

-export([make_producers/3,
	 make_producers/4,
	 make_producers/5,
	 route_steady_timestamp_rate_source/4,
	 route_steady_timestamp_rate_source/5,
	 steady_timestamp_rate_source/4,
	 route_timestamp_rate_source/4,
	 route_timestamp_rate_source/5,
	 timestamp_rate_source/4,
	 constant_rate_source/3,
	 constant_rate_source/4,
	 route_constant_rate_source/4,
	 route_constant_rate_source/5,
	 dumper/2,
	 interleave_heartbeats/3,
	 interleave_heartbeats/4,
	 interleave_heartbeats_generator/3,
	 interleave_heartbeats_generator/4,
	 list_generator/1,
	 file_generator/2,
	 file_generator_with_heartbeats/5,
	 generator_to_list/1
	]).

-include("type_definitions.hrl").

%%%
%%% This module contains code that will be usually used by producer nodes
%%% 
-spec make_producers([{msg_generator(), impl_tag(), integer()}], configuration(), topology()) -> ok.
make_producers(InputGens, Configuration, Topology) ->
    make_producers(InputGens, Configuration, Topology, constant).

-spec make_producers([{msg_generator(), impl_tag(), integer()}], configuration(), 
		     topology(), producer_type()) -> ok.
make_producers(InputGens, Configuration, Topology, ProducerType) ->
    make_producers(InputGens, Configuration, Topology, ProducerType, fun log_mod:no_message_logger/0).

-spec make_producers([{msg_generator(), impl_tag(), integer()}], configuration(), 
		     topology(), producer_type(), message_logger_init_fun()) -> ok.
make_producers(InputGens, Configuration, Topology, ProducerType, MessageLoggerInitFun) ->
    NodesRates = conf_gen:get_nodes_rates(Topology),
    ProducerPids = 
	lists:map(
	  fun({InputGen, ImplTag, Rate}) ->
		  %% Old way of finding node
		  %% TODO: Maybe at some point I will use the real given rate
		  %% {Node, Tag, _Rate} = lists:keyfind(Tag, 2, NodesRates),
		  {Tag, Node} = ImplTag,
		  %% Log producer creation
		  case ProducerType of
		      constant ->
			  Pid = spawn_link(Node, producer, route_constant_rate_source, 
					   [ImplTag, generator_to_list(InputGen), Rate, 
					    Configuration, MessageLoggerInitFun]),
			  io:format("Spawning constant rate producer for impl tag: ~p" 
				    "with pid: ~p in node: ~p~n", 
				    [ImplTag, Pid, Node]),
			  Pid;
		      timestamp_based ->
			  Pid = spawn_link(Node, producer, route_timestamp_rate_source, 
					   [ImplTag, InputGen, Rate, 
					    Configuration, MessageLoggerInitFun]),
			  io:format("Spawning timestamp based rate producer for" 
				    "impl tag: ~p with pid: ~p in node: ~p~n", 
				    [ImplTag, Pid, Node]),
			  Pid;
		      steady_timestamp ->
			  Pid = spawn_link(Node, producer, route_steady_timestamp_rate_source, 
					   [ImplTag, InputGen, Rate, 
					    Configuration, MessageLoggerInitFun]),
			  io:format("Spawning steady timestamp rate producer for" 
				    "impl tag: ~p with pid: ~p in node: ~p~n", 
				    [ImplTag, Pid, Node]),
			  Pid
		  end
	  end, InputGens),

    %% Synchronize the producers by starting them all together
    lists:foreach(
      fun(ProducerPid) ->
	      ProducerPid ! start
      end, ProducerPids).

%%
%% This producer sends a stream of messages with a rate that depends
%% on the message timestamps (like the one below). However, since
%% it seems like producers like that could drift far from each
%% other (based on some simple experiments that we did), this producer
%% timestamps the messages on its own before sending them.
%%
%% WARNING: Because these producers timestamp the messages on their own
%% the result of the computation will be different each time, so 
%% it shouldn't be expected to return the same results every time.
%%
%% TODO: Find a better name
-spec route_steady_timestamp_rate_source(impl_tag(), msg_generator(), integer(), configuration()) -> ok.
route_steady_timestamp_rate_source(ImplTag, MsgGen, Rate, Configuration) ->
    route_steady_timestamp_rate_source(ImplTag, MsgGen, Rate, Configuration, fun log_mod:no_message_logger/0).

-spec route_steady_timestamp_rate_source(impl_tag(), msg_generator(), integer(), 
				  configuration(), message_logger_init_fun()) -> ok.
route_steady_timestamp_rate_source(ImplTag, MsgGen, Rate, Configuration, MessageLoggerInitFun) ->
    log_mod:init_debug_log(),
    log_mod:debug_log("Ts: ~s -- Producer ~p of tag: ~p, started in ~p~n", 
		      [util:local_timestamp(),self(), ImplTag, node()]),
    %% Find where to route the message in the configuration tree
    {Tag, Node} = ImplTag,
    [{SendTo, undef}|_] = router:find_responsible_subtree_pids(Configuration, {{Tag, undef}, Node, 0}),
    log_mod:debug_log("Ts: ~s -- Producer ~p routes to: ~p~n", 
		      [util:local_timestamp(), self(), SendTo]),
    %% Every producer should synchronize, so that they 
    %% produce messages in the same order
    receive
	start ->
	    log_mod:debug_log("Ts: ~s -- Producer ~p received a start message~n", 
			      [util:local_timestamp(), self()])
    end,
    steady_timestamp_rate_source(MsgGen, Rate, SendTo, MessageLoggerInitFun).

-spec steady_timestamp_rate_source(msg_generator(), Rate::integer(), 
				   mailbox(), message_logger_init_fun()) -> ok.
steady_timestamp_rate_source(MsgGen, Rate, SendTo, MsgLoggerInitFun) ->
    LoggerFun = MsgLoggerInitFun(),
    steady_timestamp_rate_source(MsgGen, Rate, 0, SendTo, LoggerFun).

-spec steady_timestamp_rate_source(msg_generator(), Rate::integer(), PrevTs::integer(), 
				   mailbox(), message_logger_log_fun()) -> ok.
steady_timestamp_rate_source(MsgGen, Rate, PrevTs, SendTo, MsgLoggerLogFun) ->
    %% First compute how much time the process has to wait
    %% to send the next message.
    case messages_to_send(MsgGen, Rate, PrevTs) of
	done ->
	    log_mod:debug_log("Ts: ~s -- Producer ~p finished sending its messages~n", 
			      [util:local_timestamp(), self()]),
	    ok;
	{NewMsgGen, MsgsToSend, NewTs} ->
	    %% io:format("Prev ts: ~p~nNewTs: ~p~nMessages: ~p~n", [PrevTs, NewTs, length(MsgsToSend)]),
	    [timestamp_send_message_or_heartbeat(Msg, SendTo, MsgLoggerLogFun) || Msg <- MsgsToSend],
	    steady_timestamp_rate_source(NewMsgGen, Rate, NewTs, SendTo, MsgLoggerLogFun)
    end.

%%
%% This producer sends a stream of messages with a rate that depends
%% on the given rate multiplier and the message timestamps.
%% It is given a message generator, which is a function that returns
%% the next message, and the next generator. This can be implemented
%% as a file reader, or a list, or anything.
%% 
%% NOTE: The producer is assumed to return instantly. So it should
%%       be non-blocking.
%% WARNING: This producer doesn't really provide hard real time 
%%          guarantees, and messages could be delayed more or less
%%          than what their timestamps say. 
%% TODO: Unify the route_constant and route_timestamp, as they do the same thing
-spec route_timestamp_rate_source(impl_tag(), msg_generator(), integer(), configuration()) -> ok.
route_timestamp_rate_source(ImplTag, MsgGen, Rate, Configuration) ->
    route_timestamp_rate_source(ImplTag, MsgGen, Rate, Configuration, fun log_mod:no_message_logger/0).

-spec route_timestamp_rate_source(impl_tag(), msg_generator(), integer(), 
				  configuration(), message_logger_init_fun()) -> ok.
route_timestamp_rate_source(ImplTag, MsgGen, Rate, Configuration, MessageLoggerInitFun) ->
    log_mod:init_debug_log(),
    log_mod:debug_log("Ts: ~s -- Producer ~p of tag: ~p, started in ~p~n", 
		      [util:local_timestamp(),self(), ImplTag, node()]),
    %% Find where to route the message in the configuration tree
    {Tag, Node} = ImplTag,
    [{SendTo, undef}|_] = router:find_responsible_subtree_pids(Configuration, {{Tag, undef}, Node, 0}),
    log_mod:debug_log("Ts: ~s -- Producer ~p routes to: ~p~n", 
		      [util:local_timestamp(), self(), SendTo]),
    %% Every producer should synchronize, so that they 
    %% produce messages in the same order
    receive
	start ->
	    log_mod:debug_log("Ts: ~s -- Producer ~p received a start message~n", 
			      [util:local_timestamp(), self()])
    end,
    timestamp_rate_source(MsgGen, Rate, SendTo, MessageLoggerInitFun).

-spec timestamp_rate_source(msg_generator(), Rate::integer(), 
			    mailbox(), message_logger_init_fun()) -> ok.
timestamp_rate_source(MsgGen, Rate, SendTo, MsgLoggerInitFun) ->
    LoggerFun = MsgLoggerInitFun(),
    timestamp_rate_source(MsgGen, Rate, 0, SendTo, LoggerFun).

-spec timestamp_rate_source(msg_generator(), Rate::integer(), PrevTs::integer(), 
			    mailbox(), message_logger_log_fun()) -> ok.
timestamp_rate_source(MsgGen, Rate, PrevTs, SendTo, MsgLoggerLogFun) ->
    %% First compute how much time the process has to wait
    %% to send the next message.
    case messages_to_send(MsgGen, Rate, PrevTs) of
	done ->
	    log_mod:debug_log("Ts: ~s -- Producer ~p finished sending its messages~n", 
			      [util:local_timestamp(), self()]),
	    ok;
	{NewMsgGen, MsgsToSend, NewTs} ->
	    %% io:format("Prev ts: ~p~nNewTs: ~p~nMessages: ~p~n", [PrevTs, NewTs, length(MsgsToSend)]),
	    [send_message_or_heartbeat(Msg, SendTo, MsgLoggerLogFun) || Msg <- MsgsToSend],
	    timestamp_rate_source(NewMsgGen, Rate, NewTs, SendTo, MsgLoggerLogFun)
    end.

%% This function gathers the messages to send in the next batch, 
%% and waits the correct amount of time before collecting them.
%% It returns a new message generator, the new PrevTs, and the
%% messages to be sent.
-spec messages_to_send(msg_generator(), Rate::integer(), PrevTs::integer())
		      -> 'done' | {msg_generator(), [gen_message_or_heartbeat()], NewPrevTs::integer()}.
messages_to_send(MsgGen, Rate, PrevTs) ->
    messages_to_send(MsgGen, Rate, PrevTs, 0.0, []).

-spec messages_to_send(msg_generator(), Rate::integer(), PrevTs::integer(), 
		       Wait::float(), [gen_message_or_heartbeat()])
		      -> 'done' | {msg_generator(), [gen_message_or_heartbeat()], NewPrevTs::integer()}.
messages_to_send(MsgGen, Rate, PrevTs, Wait, Acc) ->
    %% We have to compute the time to wait before sending the next message,
    %% but since we can only wait for more than 10ms, we have to make sure
    %% that we send more messages in one batch if the rate * diff is too small.
    
    %% So this function gathers enough messages so that we have to wait at least 10ms
    %% before sending them. 
    %%
    %% WARNING: The problem with that is that if there are a lot of messages
    %% that are to be sent without any waiting, and then there is a message
    %% to be sent after a long wait, all the first messages will be delayed
    %% until the timestamp of the last message.
    case MsgGen() of
	done ->
	    case Acc of
		[] ->
		    done;
		[_|_] ->
		    {fun() -> done end, lists:reverse(Acc), PrevTs}
	    end;
	{Msg, NewMsgGen} ->
	    %% io:format("Checking message: ~p~n", [Msg]),
	    Ts =
		case Msg of
		    {heartbeat, {_Tag, Ts0}} ->
			Ts0;
		    {{_Tag, V}, _N, Ts0} ->
			Ts0
		end,
	    NewWait = Wait + ((Ts - PrevTs) / Rate),
	    %% Is the new total wait above 10ms
	    case NewWait > 10 of
		true ->
		    %% Wait
		    %% io:format("~p will wait for ~p ms~n", [self(), round(NewWait)]),
		    timer:sleep(round(NewWait)),
		    %% Return the messages to send
		    {NewMsgGen, lists:reverse([Msg|Acc]), Ts};
		false ->
		    messages_to_send(NewMsgGen, Rate, Ts, NewWait, [Msg|Acc])
	    end
    end.
	    
	    

%%
%% This producer, sends a sequence of messages one by one
%% every [Period] microseconds no matter what their timestamps are. 
%%
%% Because is Erlang it is not very easy to sleep for a very small
%% amount of time (specifically anything sleep less than 10 ms doesn't
%% really work), if the Period is set to be anything less than 10000
%% the producer sends messages in batches of 10000/Period and after every
%% batch it waits for 10 milliseconds.
%%
%% NOTE: The producer doesn't really send a message every Period microsecons,
%% but it rather sends a message, then waits for Period, and then repeat. So
%% the real period is Period + Time of sending one message.
%%
-spec constant_rate_source([gen_message_or_heartbeat()], integer(), mailbox()) -> ok.
constant_rate_source(Messages, Period, SendTo) ->
    constant_rate_source(Messages, Period, SendTo, fun log_mod:no_message_logger/0).

-spec constant_rate_source([gen_message_or_heartbeat()], integer(), mailbox(), 
			   message_logger_init_fun()) -> ok.
constant_rate_source(Messages, Period, SendTo, MsgLoggerInitFun) ->
    %% Initialize the logger, to get the message logger fun
    LoggerFun = MsgLoggerInitFun(),
    case Period =< 10000 of
	true ->
	    constant_rate_source_fast(Messages, 10000 div Period, SendTo, LoggerFun);
	false ->
	    constant_rate_source_slow(Messages, Period div 1000, SendTo, LoggerFun)
    end.

%% This sends a batch of BatchSize and then waits 10 ms
-spec constant_rate_source_fast([gen_message_or_heartbeat()], 1..10000, mailbox(), 
				message_logger_log_fun()) -> ok.
constant_rate_source_fast([], _BatchSize, _SendTo, _MsgLoggerLogFun) ->
    ok;
constant_rate_source_fast(Messages, BatchSize, SendTo, MsgLoggerLogFun) ->
    {MsgsToSend, Rest} = util:take_at_most(BatchSize, Messages),
    [send_message_or_heartbeat(Msg, SendTo, MsgLoggerLogFun) || Msg <- MsgsToSend],
    timer:sleep(10),
    constant_rate_source_fast(Rest, BatchSize, SendTo, MsgLoggerLogFun).

%% This only works with periods larger than 10 ms
-spec constant_rate_source_slow([gen_message_or_heartbeat()], integer(), mailbox(),
				message_logger_log_fun()) -> ok.
constant_rate_source_slow([], _Period, _SendTo,  _MsgLoggerLogFun) ->
    ok;
constant_rate_source_slow([Msg|Rest], Period, SendTo, MsgLoggerLogFun) ->
    send_message_or_heartbeat(Msg, SendTo, MsgLoggerLogFun),
    timer:sleep(Period),
    constant_rate_source_slow(Rest, Period, SendTo, MsgLoggerLogFun).

-spec route_constant_rate_source(impl_tag(), [gen_message_or_heartbeat()], integer(), configuration()) -> ok.
route_constant_rate_source(ImplTag, Messages, Period, Configuration) ->
    route_constant_rate_source(ImplTag, Messages, Period, Configuration, fun log_mod:no_message_logger/0).

-spec route_constant_rate_source(impl_tag(), [gen_message_or_heartbeat()], integer(), 
				 configuration(), message_logger_init_fun()) -> ok.
route_constant_rate_source(ImplTag, Messages, Period, Configuration, MessageLoggerInitFun) ->
    %% Find where to route the message in the configuration tree
    {Tag, Node} = ImplTag,
    [{SendTo, undef}|_] = router:find_responsible_subtree_pids(Configuration, {{Tag, undef}, Node, 0}),
    receive
	start ->
	    ok
    end,
    constant_rate_source(Messages, Period, SendTo, MessageLoggerInitFun).



%%
%% This is the simplest producer, that takes as input a list of 
%% messages (and heartbeats) to send, and just sends them all to
%% its receiver.
%%
-spec dumper([gen_message_or_heartbeat()], mailbox()) -> ok.
dumper([], _SendTo) ->
    ok;
dumper([Msg|Rest], SendTo) ->
    send_message_or_heartbeat(Msg, SendTo, fun util:always_ok/1),
    dumper(Rest, SendTo).


%% ASSUMPTION: This assumes that the input stream that it gets is sorted
%% 
%% NOTE: The Until number is exclusive
%% 
%% It also returns the stream in a correct order so it puts the
%% heartbeats in the exact position where they should be (inserting 
%% them later on would/should also be correct)
%% MAYBE: I could sort the stream before calling the internal interleave
%%
%% WARNING: It assumes that there is only one tag in the input stream
%% 
%% NOTE: It can either be given the starting time or not
-spec interleave_heartbeats([gen_message_or_heartbeat()], {impl_tag(), integer()}, timestamp()) 
			   -> [gen_message_or_heartbeat()].
interleave_heartbeats(Stream, {ImplTag, Period}, Until) ->
    interleave_heartbeats(Stream, {ImplTag, Period}, Period, Until).

-spec interleave_heartbeats([gen_message_or_heartbeat()], {impl_tag(), integer()}, timestamp(), timestamp()) 
			   -> [gen_message_or_heartbeat()].
interleave_heartbeats(Stream, {ImplTag, Period}, From, Until) ->
    interleave_heartbeats(Stream, ImplTag, From, Period, [], Until).

-spec interleave_heartbeats([gen_message_or_heartbeat()], impl_tag(), 
			    timestamp(), integer(), [gen_message_or_heartbeat()], timestamp()) 
			   -> [gen_message_or_heartbeat()].
interleave_heartbeats([], Tag, NextHeartbeat, Period, StreamAcc, Until) ->
    %% Generate the final heartbeats to send
    {FinalHeartbeatsToSend, _} = 
	maybe_generate_heartbeats(Until, NextHeartbeat, Tag, Period),
    %% Output the messages until now and the final heartbeats
    lists:reverse(StreamAcc) ++ FinalHeartbeatsToSend;
interleave_heartbeats([{{Tag, _V}, Node, Ts} = Msg|Rest], {Tag, Node} = ImplTag, 
		      NextHeartbeat, Period, StreamAcc, Until) ->
    {HeartbeatsToSend, NewNextHeartbeat} = 
	maybe_generate_heartbeats(Ts, NextHeartbeat, ImplTag, Period),
    NewStreamAcc = 
	[Msg|lists:reverse(HeartbeatsToSend)] ++ StreamAcc,
    interleave_heartbeats(Rest, ImplTag, NewNextHeartbeat, Period, NewStreamAcc, Until).

-spec maybe_generate_heartbeats(timestamp(), timestamp(), impl_tag(), integer()) 
			       -> {[gen_heartbeat()], timestamp()}.
maybe_generate_heartbeats(Ts, NextHeartbeat, ImplTag, Period) ->
    case Ts > NextHeartbeat of
	true ->
	    generate_heartbeats(ImplTag, NextHeartbeat, Ts, Period);
	false ->
	    {[], NextHeartbeat}
    end.

%% This function generates the latest heartbeat in a period
%% -spec generate_last_heartbeat(tag(), integer(), integer(), integer()) 
%% 			-> {gen_heartbeat(), integer()}.
%% generate_last_heartbeat(HTag, From, To, Period) ->
%%     Times = (To - From) div Period,
%%     HTs = From + Times * Period,
%%     Heartbeat = {heartbeat, {HTag, HTs}},
%%     {Heartbeat, HTs + Period}.

%% This generates all heartbeats that can fit in a period
-spec generate_heartbeats(impl_tag(), timestamp(), timestamp(), integer()) 
			 -> {[gen_heartbeat()], timestamp()}.
generate_heartbeats(HTag, From, To, Period) ->
    Times = (To - From) div Period,
    HTs = [From + Time * Period || Time <- lists:seq(0, Times)],
    %% A heartbeat timestamp that is same with the message
    %% could be generated but we only want the smaller ones
    SmallerThanTo = lists:takewhile(fun(HT) -> HT < To end, HTs),
    Heartbeats = 
	[{heartbeat, {HTag, HT}} || HT <- SmallerThanTo],
    {Heartbeats, lists:last(SmallerThanTo) + Period}.


%%
%% This function interleaves heartbeats with a specific frequency in the message stream.
%% Our new streams have one tag each, but this function can add heartbeats for streams 
%% with many tags.
%%
%% WARNING: The append generators might make it a bit inefficient.
-spec interleave_heartbeats_generator(msg_generator(), {impl_tag(), integer()}, timestamp()) 
				     -> msg_generator().
interleave_heartbeats_generator(StreamGen, {ImplTag, Period}, Until) ->
    interleave_heartbeats_generator(StreamGen, {ImplTag, Period}, Period, Until).

-spec interleave_heartbeats_generator(msg_generator(), {impl_tag(), integer()}, timestamp(), timestamp()) 
				     -> msg_generator().
interleave_heartbeats_generator(StreamGen, {ImplTag, Period}, From, Until) ->
    AccGen = fun() -> done end,
    interleave_heartbeats_generator(StreamGen, ImplTag, From, Period, AccGen, Until).


-spec interleave_heartbeats_generator(msg_generator(), impl_tag(), timestamp(), 
			    integer(), msg_generator(), timestamp()) 
			   -> msg_generator().
interleave_heartbeats_generator(MsgGen, {Tag, Node} = ImplTag, NextHeartbeat, Period, AccMsgGen, Until) ->
    case MsgGen() of
	done ->
	    %% Generate the final heartbeats to send
	    {FinalHeartbeatsToSend, _} = 
		maybe_generate_heartbeats(Until, NextHeartbeat, ImplTag, Period),
	    %% Output the messages until now and the final heartbeats
	    append_gens(AccMsgGen, list_generator(FinalHeartbeatsToSend));
	{{{Tag, _V}, Node, Ts} = Msg, RestMsgGen} ->
	    %% io:format("Msg: ~p~n", [{Tag, Ts, Payload}]),
	    %% Generate the new heartbeats
	    {HeartbeatsToSend, NewNextHeartbeat} = 
		maybe_generate_heartbeats(Ts, NextHeartbeat, ImplTag, Period),
	    io:format("To Send: ~p~n", [HeartbeatsToSend]),
	    NewAccMsgGen = 
	        append_gens(AccMsgGen, list_generator(HeartbeatsToSend ++ [Msg])),
	    interleave_heartbeats_generator(RestMsgGen, ImplTag, NewNextHeartbeat, Period, NewAccMsgGen, Until)
    end.
	    
	
-spec append_gens(msg_generator(), msg_generator()) -> msg_generator().
append_gens(MsgGen1, MsgGen2) ->
    case MsgGen1() of
	done ->
	    MsgGen2;
	{Msg, NewMsgGen1} ->
	    fun() ->
		    {Msg, append_gens(NewMsgGen1, MsgGen2)}
	    end
    end.



    
-spec send_message_or_heartbeat(gen_message_or_heartbeat(), mailbox(),
			        message_logger_log_fun()) -> gen_imessage_or_iheartbeat().
send_message_or_heartbeat({heartbeat, Hearbeat}, SendTo, MessageLoggerInitFun) ->
    SendTo ! {iheartbeat, Hearbeat};
send_message_or_heartbeat(Msg, SendTo, MessageLoggerInitFun) ->
    ok = MessageLoggerInitFun(Msg),
    SendTo ! {imsg, Msg}.

%% This function ignores the message timestamp and sends it with its own timestamp
-spec timestamp_send_message_or_heartbeat(gen_message_or_heartbeat(), mailbox(),
					  message_logger_log_fun()) -> gen_imessage_or_iheartbeat().
timestamp_send_message_or_heartbeat({heartbeat, {ImplTag, _}}, SendTo, MessageLoggerInitFun) ->
    Ts = erlang:system_time(),
    SendTo ! {iheartbeat, {ImplTag, Ts}};
timestamp_send_message_or_heartbeat({{Tag, Value}, Node, _Ts}, SendTo, MessageLoggerInitFun) ->
    Ts = erlang:system_time(),
    Msg = {{Tag, Value}, Node, Ts},
    ok = MessageLoggerInitFun(Msg),
    SendTo ! {imsg, Msg}.

%%
%% Message Generator
%% Those can either be file implementations, or lists that 
%% return the next element each time.
%%
-spec list_generator([gen_message_or_heartbeat()]) -> msg_generator().
list_generator(List) ->
    fun() ->
	    case List of
		[] ->
		    done;
		[Msg|Rest] ->
		    {Msg, list_generator(Rest)}
	    end
    end.

-spec file_generator(file:filename(), input_file_parser()) -> msg_generator().
file_generator(Filename, LineParser) ->
    {ok, IoDevice} = file:open(Filename, [read]),
    file_generator0(IoDevice, LineParser).

-spec file_generator0(file:io_device(), input_file_parser()) -> msg_generator().
file_generator0(IoDevice, LineParser) ->
    fun() ->
	    case file:read_line(IoDevice) of
		{ok, Line} ->
		    Msg = LineParser(Line),
		    {Msg, file_generator0(IoDevice, LineParser)};
		eof ->
		    done
		end
    end.

-spec file_generator_with_heartbeats(file:filename(), input_file_parser(),
				     {impl_tag(), integer()}, timestamp(), timestamp()) -> msg_generator().
file_generator_with_heartbeats(Filename, LineParser, {ImplTag, Period}, From, Until) ->
    FileGen = file_generator(Filename, LineParser),
    interleave_heartbeats_generator(FileGen, {ImplTag, Period}, From, Until).
    

-spec generator_to_list(msg_generator()) -> [gen_message_or_heartbeat()].
generator_to_list(Gen) ->
    generator_to_list(Gen, []).

generator_to_list(Gen, Acc) ->
    case Gen() of
	done ->
	    lists:reverse(Acc);
	{Msg, NewGen} ->
	    generator_to_list(NewGen, [Msg|Acc])
    end.
