-module(producer).

-export([make_producers/3,
	 make_producers/4,
	 make_producers/5,
         make_producers/6,
         init_producer/5,
         init_producer/6,
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
-include("config.hrl").

-ifndef(SLEEP_GRANULARITY_MILLIS).
-define(SLEEP_GRANULARITY_MILLIS, 2).
-endif.

-ifndef(GLOBAL_START_TIME_DELAY_MS).
-define(GLOBAL_START_TIME_DELAY_MS, 1000).
-endif.

%%%
%%% This module contains code that will be usually used by producer nodes
%%%
-spec make_producers(gen_producer_init(), configuration(), topology()) -> ok.
make_producers(InputGens, Configuration, _Topology) ->
    make_producers0(InputGens, Configuration, []).

-spec make_producers(gen_producer_init(), configuration(),
		     topology(), producer_type() | producer_options()) -> ok.
make_producers(InputGens, Configuration, _Topology, ProducerType) when is_atom(ProducerType) ->
    make_producers0(InputGens, Configuration, [{producer_type, ProducerType}]);

%% This is the new API to call this function. The rest are left here for backward compatibility.
make_producers(InputGens, Configuration, _Topology, ProducerOpts) when is_list(ProducerOpts) ->
    make_producers0(InputGens, Configuration, ProducerOpts).


-spec make_producers(gen_producer_init(), configuration(),
		     topology(), producer_type(),
                     {'log_tags', [tag()]} | message_logger_init_fun()) -> ok.
make_producers(InputGens, Configuration, _Topology, ProducerType, {log_tags, Tags}) ->
    ProducerOptions = [{producer_type, ProducerType},
                       {log_tags, Tags}],
    make_producers0(InputGens, Configuration, ProducerOptions);
make_producers(InputGens, Configuration, _Topology, ProducerType, MessageLoggerInitFun) ->
    ProducerOptions = [{producer_type, ProducerType},
                       {message_logger_init_fun, MessageLoggerInitFun}],
    make_producers0(InputGens, Configuration, ProducerOptions).

%% TODO: Instead of sending the beginning of time, we should pass it as an argument
-spec make_producers(gen_producer_init(), configuration(),
		     topology(), producer_type(), message_logger_init_fun(), integer()) -> ok.
make_producers(InputGens, Configuration, _Topology, ProducerType, MessageLoggerInitFun, BeginningOfTime) ->
    ProducerOptions = [{producer_type, ProducerType},
                       {message_logger_init_fun, MessageLoggerInitFun},
                       {producers_begin_time, BeginningOfTime}],
    make_producers0(InputGens, Configuration, ProducerOptions).

-spec make_producers0(gen_producer_init(), configuration(), producer_options()) -> ok.
make_producers0(InputGens, Configuration, ProducerOptions) ->
    io:format("Options: ~p~n", [ProducerOptions]),
    %% Get options
    {producer_type, ProducerType} =
        get_option(producer_type, ProducerOptions),
    {message_logger_init_fun, MessageLoggerInitFun} =
        get_option(message_logger_init_fun, ProducerOptions),
    {producers_begin_time, BeginningOfTime} =
        get_option(producers_begin_time, ProducerOptions),

    io:format("Options: ~p~n", [{ProducerType, MessageLoggerInitFun, BeginningOfTime}]),
    ProducerPids =
	lists:map(
	  fun({MsgGenInit, ImplTag, Rate}) ->
		  {_Tag, Node} = ImplTag,
		  %% Log producer creation
                  Pid = spawn_link(Node, producer, init_producer,
                                   [ProducerType, ImplTag, MsgGenInit, Rate,
                                    Configuration, MessageLoggerInitFun]),
                  io:format("Spawning '~p' producer for"
                            "impl tag: ~p with pid: ~p in node: ~p~n",
                            [ProducerType, ImplTag, Pid, Node]),
                  Pid
	  end, InputGens),

    %% Get a global start timestamp to give to all producers.
    %%
    %% Note: We delay the timestamp by GLOBAL_START_TIME_DELAY_MS so that
    %% there is no initial spike of events.
    GlobalStartTime = ?GET_SYSTEM_TIME(millisecond),

    %% Log the time that producers where done spawning.
    log_producers_spawn_finish_time(),

    %% Synchronize the producers by starting them all together
    lists:foreach(
      fun(ProducerPid) ->
	      ProducerPid ! {start, BeginningOfTime, GlobalStartTime + ?GLOBAL_START_TIME_DELAY_MS}
      end, ProducerPids),

    %% Sleep to return from this function as close as possible to the
    %% producer start time (if the producers all sync to this global
    %% time).
    case ProducerType =:= steady_sync_timestamp
        orelse ProducerType =:= steady_retimestamp of
        true ->
            SleepingTime = GlobalStartTime + ?GLOBAL_START_TIME_DELAY_MS - ?GET_SYSTEM_TIME(millisecond),
            timer:sleep(SleepingTime);
        _ ->
            ok
    end.

-spec log_producers_spawn_finish_time() -> ok.
log_producers_spawn_finish_time() ->
    Filename =
        io_lib:format("~s/producers_time.log",
		      [?LOG_DIR]),
    CurrentTimestamp = ?GET_SYSTEM_TIME(),
    ProducerStartTime = CurrentTimestamp +
        erlang:convert_time_unit(?GLOBAL_START_TIME_DELAY_MS, millisecond, native),
    Data = io_lib:format("Ts: ~s -- Pid: ~p@~p -- Producers are going to start at time: ~p~n",
                         [util:local_timestamp(), self(), node(), ProducerStartTime]),
    ok = file:write_file(Filename, Data).


-spec get_option(atom(), producer_options()) -> producer_option().
get_option(message_logger_init_fun, ProducerOptions) ->
    case get_option0(log_tags, ProducerOptions) of
        {log_tags, []} ->
            get_option0(message_logger_init_fun, ProducerOptions);
        {log_tags, Tags} ->
            %% It shouldn't be the case that both log tags and
            %% message logger init fun have been given.
            false = lists:keyfind(message_logger_init_fun, 1, ProducerOptions),
            {message_logger_init_fun,
             fun() ->
                     log_mod:initialize_message_logger_state("producer", sets:from_list(Tags))
             end}
    end;
get_option(Option, ProducerOptions) ->
    get_option0(Option, ProducerOptions).

-spec get_option0(atom(), producer_options()) -> producer_option().
get_option0(Option, ProducerOptions) ->
    case lists:keyfind(Option, 1, ProducerOptions) of
        false ->
            {Option, default_option(Option)};
        Result ->
            Result
    end.

-spec default_option(atom()) -> any().
default_option(producer_type) ->
    constant;
default_option(log_tags) ->
    [];
default_option(producers_begin_time) ->
    0;
default_option(message_logger_init_fun) ->
    fun log_mod:no_message_logger/0.

%%
%% Common initialization for all producers
%%
-spec init_producer(producer_type(), impl_tag(), msg_generator_init(), integer(), configuration()) -> ok.
init_producer(ProducerType, ImplTag, MsgGenInit, Rate, Configuration) ->
    init_producer(ProducerType, ImplTag, MsgGenInit, Rate, Configuration, fun log_mod:no_message_logger/0).
-spec init_producer(producer_type(), impl_tag(), msg_generator_init(), integer(),
                    configuration(), message_logger_init_fun()) -> ok.
init_producer(ProducerType, ImplTag, MsgGenInit, Rate, Configuration, MsgLoggerInitFun) ->
    log_mod:init_debug_log(),
    log_mod:debug_log("Ts: ~s -- Producer ~p of tag: ~p, started in ~p~n",
		      [util:local_timestamp(),self(), ImplTag, node()]),
    %% Initialize the generator
    MsgGen = init_generator(MsgGenInit),
    %% Find where to route the message in the configuration tree
    {Tag, Node} = ImplTag,
    SendTo = router:find_responsible_subtree_root(Configuration, {{Tag, undef}, Node, 0}),
    log_mod:debug_log("Ts: ~s -- Producer ~p routes to: ~p~n",
		      [util:local_timestamp(), self(), SendTo]),
    %% Initialize the latency logger
    LoggerFun = MsgLoggerInitFun(),
    sync_producer(ProducerType, MsgGen, Rate, SendTo, LoggerFun).

-spec sync_producer(producer_type(), msg_generator(), integer(), mailbox(), message_logger_log_fun()) -> ok.
sync_producer(ProducerType, MsgGen, Rate, SendTo, LoggerFun) ->
    %% Every producer should synchronize, so that they produce
    %% messages in the same order. FirstGeneratorTimestamp contains
    %% the starting timestamp of this experiment (the first message
    %% timestamp).
    receive
	{start, FirstGeneratorTimestamp, GlobalStartTime} ->
            LocalStartTime = ?GET_SYSTEM_TIME(millisecond),
	    log_mod:debug_log("Ts: ~s -- Producer ~p received a start message ~p"
                              " with global start timestamp: ~p. Local start timestamp is: ~p~n",
			      [util:local_timestamp(), self(), FirstGeneratorTimestamp,
                               GlobalStartTime, LocalStartTime])
    end,
    case ProducerType of
        constant ->
            constant_rate_source(MsgGen, Rate, SendTo, LoggerFun);
        timestamp_based ->
            io:format("WARNING: Producer type: ~p is old and should not be used anymore~n",
                      [ProducerType]),
            timestamp_rate_source(MsgGen, Rate, FirstGeneratorTimestamp, SendTo, LoggerFun);
        steady_retimestamp_old ->
            io:format("WARNING: Producer type: ~p is old and should not be used anymore~n",
                      [ProducerType]),
            steady_retimestamp_rate_source_old(MsgGen, Rate, FirstGeneratorTimestamp, SendTo, LoggerFun);
        steady_timestamp ->
            steady_timestamp_rate_source(MsgGen, Rate, LocalStartTime, SendTo, LoggerFun);
        %% This producer sends a stream of messages with a rate that depends
        %% on the message timestamps (like the one below). This producer tries
        %% to solve lagging issues by keeping track of a starting time
        %%
        %% In contrast to the simple timestamp producer, this one uses a given
        %% timestamp as the starting point.
        steady_sync_timestamp ->
            steady_timestamp_rate_source(MsgGen, Rate, GlobalStartTime, SendTo, LoggerFun);
        steady_retimestamp ->
            steady_retimestamp_rate_source(MsgGen, Rate, GlobalStartTime, SendTo, LoggerFun)
    end.


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
-spec steady_retimestamp_rate_source(msg_generator(), Rate::integer(), StartMonoTime::integer(),
                                     mailbox(), message_logger_log_fun()) -> ok.
steady_retimestamp_rate_source(MsgGen, Rate, StartMonoTime, SendTo, MsgLoggerLogFun) ->
    SendFun = fun timestamp_send_message_or_heartbeat/3,
    steady_timestamp_rate_source_base(MsgGen, Rate, StartMonoTime, SendTo, MsgLoggerLogFun, SendFun).

%% This is the old design
-spec steady_retimestamp_rate_source_old(msg_generator(), Rate::integer(), PrevTs::integer(),
                                         mailbox(), message_logger_log_fun()) -> ok.
steady_retimestamp_rate_source_old(MsgGen, Rate, PrevTs, SendTo, MsgLoggerLogFun) ->
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
	    steady_retimestamp_rate_source_old(NewMsgGen, Rate, NewTs, SendTo, MsgLoggerLogFun)
    end.


%%
%% This producer sends a stream of messages with a rate that depends
%% on the message timestamps (like the one below). This producer tries
%% to solve lagging issues by keeping track of a starting time
%%
%% WARNING: This producer assumes that all producers start their
%% timers at the same time!! Any differences there will lead to a
%% constant skew between different producers.
%%
-spec steady_timestamp_rate_source(msg_generator(), Rate::integer(),
                                        StartMonoTime::integer(),
                                        mailbox(), message_logger_log_fun()) -> ok.
steady_timestamp_rate_source(MsgGen, Rate, StartMonoTime, SendTo, MsgLoggerLogFun) ->
    SendFun = fun send_message_or_heartbeat/3,
    steady_timestamp_rate_source_base(MsgGen, Rate, StartMonoTime, SendTo, MsgLoggerLogFun, SendFun).


%%
%% This is the base producer function. It is pretty accurate and can
%% also retimestamp events given the correct send function.
%%
-spec steady_timestamp_rate_source_base(msg_generator(), Rate::integer(),
                                        StartMonoTime::integer(),
                                        mailbox(), message_logger_log_fun(),
                                        producer_send_fun()) -> ok.
steady_timestamp_rate_source_base(MsgGen, Rate, StartMonoTime, SendTo, MsgLoggerLogFun, SendFun) ->
    %% First get the current elapsed monotonic time from the start of
    %% the producer.
    BatchStartMonoTime = ?GET_SYSTEM_TIME(millisecond) - StartMonoTime,
    %% Then find the least amount of time the producer must sleep
    %% until (normalized based on the rate). The producer can then
    %% safely release all messages that have a timestamp until then.
    SleepAtLeastUntil = (BatchStartMonoTime + ?SLEEP_GRANULARITY_MILLIS) * Rate,
    %% Then compute all the messages that have to be sent, and send them
    case send_messages_until(MsgGen, SleepAtLeastUntil, SendTo, MsgLoggerLogFun, SendFun) of
	done ->
	    log_mod:debug_log("Ts: ~s -- Producer ~p finished sending its messages~n",
			      [util:local_timestamp(), self()]),
	    ok;
	{NewMsgGen, NewSleepUntil} ->
            %% Now that all messages are released, get the current
            %% time, and wait for what is left.
            CurrentMonoTime = ?GET_SYSTEM_TIME(millisecond) - StartMonoTime,
            %% Sleep until the next event (or not at all if we delayed by sending these events)
            SleepTime = NewSleepUntil / Rate - CurrentMonoTime,
            case SleepTime > 0 of
                true ->
                    timer:sleep(round(SleepTime)),
                    log_mod:debug_log("Ts: ~s -- Producer ~p went to sleep for ~p ms and slept for ~p ms ~n",
                                      [util:local_timestamp(), self(), round(SleepTime),
                                       ?GET_SYSTEM_TIME(millisecond) - (CurrentMonoTime + StartMonoTime)]);
                false ->
                    log_mod:debug_log("Ts: ~s -- Warning! Producer ~p is lagging behind by ~p ms ~n",
                                      [util:local_timestamp(), self(), -SleepTime])
            end,
	    steady_timestamp_rate_source_base(NewMsgGen, Rate, StartMonoTime, SendTo, MsgLoggerLogFun, SendFun)
    end.

%% This function sends all messages until a specific timestamp
-spec send_messages_until(msg_generator(), Until::integer(), mailbox(),
                          message_logger_log_fun(), producer_send_fun())
                         -> 'done' | {msg_generator(), NewUntil::integer()}.
send_messages_until(MsgGen, Until, SendTo, MsgLoggerLogFun, SendFun) ->
    case MsgGen() of
	done ->
            done;
        {Msg, NewMsgGen} ->
	    %% io:format("Checking message: ~p~n", [Msg]),
            %% {sink, node()} ! {check_message, Msg},
	    Ts =
		case Msg of
		    {heartbeat, {_Tag, Ts0}} ->
			Ts0;
		    {{_Tag, _V}, _N, Ts0} ->
			Ts0
		end,
	    case Ts =< Until of
		true ->
                    SendFun(Msg, SendTo, MsgLoggerLogFun),
                    send_messages_until(NewMsgGen, Until, SendTo, MsgLoggerLogFun, SendFun);
		false ->
		    %% If the message is after our sleep time, then
		    %% just keep its timestamp to wait until it.
		    {MsgGen, Ts0}
            end
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

%% This function gathers the messages to send in the next batch, and
%% waits the correct amount of time before collecting them. It
%% returns a new message generator, the new PrevTs, and the messages
%% to be sent.
-spec messages_to_send(msg_generator(), Rate::integer(), PrevTs::integer())
		      -> 'done' | {msg_generator(), [gen_message_or_heartbeat()], NewPrevTs::integer()}.
messages_to_send(MsgGen, Rate, PrevTs) ->
    messages_to_send(MsgGen, Rate, PrevTs, 0.0, []).

-spec messages_to_send(msg_generator(), Rate::integer(), PrevTs::integer(),
		       Wait::float(), [gen_message_or_heartbeat()])
		      -> 'done' | {msg_generator(), [gen_message_or_heartbeat()], NewPrevTs::integer()}.
messages_to_send(MsgGen, Rate, PrevTs, Wait, Acc) ->
    %% We have to compute the time to wait before sending the next
    %% message, but since we can only wait for more than 10ms, we have
    %% to make sure that we send more messages in one batch if the
    %% rate * diff is too small.

    %% So this function gathers enough messages so that we have to
    %% wait at least 10ms before sending them.
    %%
    %% WARNING: The problem with that is that if there are a lot of
    %% messages that are to be sent without any waiting, and then
    %% there is a message to be sent after a long wait, all the first
    %% messages will be delayed until the timestamp of the last
    %% message.
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
            %% {sink, node()} ! {check_message, Msg},
	    Ts =
		case Msg of
		    {heartbeat, {_Tag, Ts0}} ->
			Ts0;
		    {{_Tag, _V}, _N, Ts0} ->
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
-spec constant_rate_source(msg_generator(), integer(), mailbox(),
			   message_logger_log_fun()) -> ok.
constant_rate_source(MsgGen, Period, SendTo, LoggerFun) ->
    %% Get list of messages
    Messages = generator_to_list(MsgGen),
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
    case Period of
	0 -> 
	    Stream;
	_ ->
	    interleave_heartbeats(Stream, ImplTag, From, Period, [], Until)
    end.


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
    case Period of
	0 ->
	    StreamGen;
	_ ->
	    interleave_heartbeats_generator(StreamGen, ImplTag, From, Period, Until)
    end.


-spec interleave_heartbeats_generator(msg_generator(), impl_tag(), timestamp(), 
			    integer(), timestamp()) 
			   -> msg_generator().
interleave_heartbeats_generator(MsgGen, {Tag, Node} = ImplTag, NextHeartbeat, Period, Until) ->
    case MsgGen() of
	done ->
	    %% Generate the final heartbeats to send
	    {FinalHeartbeatsToSend, _} = 
		maybe_generate_heartbeats(Until, NextHeartbeat, ImplTag, Period),
	    %% Output the messages until now and the final heartbeats
	    list_generator(FinalHeartbeatsToSend);
	{{{Tag, _V}, Node, Ts} = Msg, RestMsgGen} ->
	    %% io:format("Msg: ~p~n", [{Tag, Ts, Payload}]),
	    %% Generate the new heartbeats
	    {HeartbeatsToSend, NewNextHeartbeat} = 
		maybe_generate_heartbeats(Ts, NextHeartbeat, ImplTag, Period),
	    %% io:format("To Send: ~p~n", [HeartbeatsToSend]),
            append_gens(list_generator(HeartbeatsToSend ++ [Msg]), 
                        fun() ->
                                interleave_heartbeats_generator(RestMsgGen, ImplTag, 
                                                                NewNextHeartbeat, Period, Until)
                        end)
    end.

-spec append_gens(msg_generator(), fun(() -> msg_generator())) -> msg_generator().
append_gens(MsgGen1, MsgGen2) ->
    case MsgGen1() of
	done ->
	    MsgGen2();
	{Msg, NewMsgGen1} ->
	    fun() ->
		    {Msg, append_gens(NewMsgGen1, MsgGen2)}
	    end
    end.


-type producer_send_fun() :: fun((gen_message_or_heartbeat(), mailbox(), message_logger_log_fun())
                                 -> gen_imessage_or_iheartbeat()).

-spec send_message_or_heartbeat(gen_message_or_heartbeat(), mailbox(),
			        message_logger_log_fun()) -> gen_imessage_or_iheartbeat().
send_message_or_heartbeat({heartbeat, Heartbeat}, SendTo, _MessageLoggerInitFun) ->
    SendTo ! {iheartbeat, Heartbeat};
send_message_or_heartbeat(Msg, SendTo, MessageLoggerInitFun) ->
    ok = MessageLoggerInitFun(Msg),
    SendTo ! {imsg, Msg}.

%% This function ignores the message timestamp and sends it with its own timestamp
-spec timestamp_send_message_or_heartbeat(gen_message_or_heartbeat(), mailbox(),
					  message_logger_log_fun()) -> gen_imessage_or_iheartbeat().
timestamp_send_message_or_heartbeat({heartbeat, {ImplTag, _}}, SendTo, MessageLoggerInitFun) ->
    Ts = ?GET_SYSTEM_TIME(),
    send_message_or_heartbeat({heartbeat, {ImplTag, Ts}}, SendTo, MessageLoggerInitFun);
timestamp_send_message_or_heartbeat({{Tag, Value}, Node, _Ts}, SendTo, MessageLoggerInitFun) ->
    Ts = ?GET_SYSTEM_TIME(),
    Msg = {{Tag, Value}, Node, Ts},
    send_message_or_heartbeat(Msg, SendTo, MessageLoggerInitFun).

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

%% Initializes the message generator
-spec init_generator(msg_generator_init()) -> msg_generator().
init_generator({MsgGenF, MsgGenA}) ->
    apply(MsgGenF, MsgGenA).
