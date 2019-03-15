-module(producer).

-export([constant_rate_source/3,
	 dumper/2,
	 interleave_heartbeats/3
	]).

-include("type_definitions.hrl").

%%%
%%% This module contains code that will be usually used by producer nodes
%%% 

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
    case Period =< 10000 of
	true ->
	    constant_rate_source_fast(Messages, 10000 div Period, SendTo);
	false ->
	    constant_rate_source_slow(Messages, Period div 1000, SendTo)
    end.

%% This sends a batch of BatchSize and then waits 10 ms
-spec constant_rate_source_fast([gen_message_or_heartbeat()], 1..10000, mailbox()) -> ok.
constant_rate_source_fast([], _BatchSize, _SendTo) ->
    ok;
constant_rate_source_fast(Messages, BatchSize, SendTo) ->
    {MsgsToSend, Rest} = util:take_at_most(BatchSize, Messages),
    [send_message_or_heartbeat(Msg, SendTo) || Msg <- MsgsToSend],
    timer:sleep(10),
    constant_rate_source_fast(Rest, BatchSize, SendTo).

%% This only works with periods larger than 10 ms
-spec constant_rate_source_slow([gen_message_or_heartbeat()], integer(), mailbox()) -> ok.
constant_rate_source_slow([], _Period, _SendTo) ->
    ok;
constant_rate_source_slow([Msg|Rest], Period, SendTo) ->
    send_message_or_heartbeat(Msg, SendTo),
    timer:sleep(Period),
    constant_rate_source_slow(Rest, Period, SendTo).

%%
%% This is the simplest producer, that takes as input a list of 
%% messages (and heartbeats) to send, and just sends them all to
%% its receiver.
%%
-spec dumper([gen_message_or_heartbeat()], mailbox()) -> ok.
dumper([], _SendTo) ->
    ok;
dumper([Msg|Rest], SendTo) ->
    send_message_or_heartbeat(Msg, SendTo),
    dumper(Rest, SendTo).


%% This assumes that the input stream that it gets is sorted
%% It also returns the stream in a correct order so it puts the
%% heartbeats in the exact position where they should be (inserting 
%% them later on would also be correct, but we could implement
%% this scrambling of message order in another function)
%% MAYBE: I could sort the stream before calling the internal interleave
interleave_heartbeats(Stream, Tags, Until) ->
    interleave_heartbeats(Stream, Tags, Tags, [], Until).

interleave_heartbeats([], NextHeartbeats, Periods, StreamAcc, Until) ->
    {FinalHeartbeatsToSend, _} = 
	maps:fold(
	  fun(HTag, HTs, Acc) ->
		  update_next_heartbeats(Until, HTag, HTs, Acc, Periods)
	  end, {[], NextHeartbeats}, NextHeartbeats),
    %% Output the messages until now
    lists:reverse(StreamAcc) ++ 
	%% Spit out the last heartbeats
	[{heartbeat, {Tag, Ts}} || {Tag, Ts} <- maps:to_list(NextHeartbeats)] ++
	%% and all the rest heartbeats until the end
	FinalHeartbeatsToSend;
interleave_heartbeats([{Tag, Ts, Payload}|Rest], NextHeartbeats, Periods, StreamAcc, Until) ->
    {HeartbeatsToSend, NewNextHeartbeats} = 
	maps:fold(
	  fun(HTag, HTs, Acc) ->
		  update_next_heartbeats(Ts, HTag, HTs, Acc, Periods)
	  end, {[], NextHeartbeats}, NextHeartbeats),
    NewStreamAcc = 
	HeartbeatsToSend ++ [{Tag, Ts, Payload}|StreamAcc],
    interleave_heartbeats(Rest, NewNextHeartbeats, Periods, NewStreamAcc, Until).
	
update_next_heartbeats(CurrTs, HTag, HTs, {ToSend, NewMap}, Periods) when CurrTs > HTs ->
    %% If the current timestamp is larger than the next heartbeat timestamp
    %% we have to generate a heartbeat and update the next heartbeat timestamp
    {Heartbeat, NextHTs} = 
	generate_heartbeat(HTag, HTs, CurrTs, maps:get(HTag, Periods)),
    {[Heartbeat|ToSend], maps:update(HTag, NextHTs, NewMap)};
update_next_heartbeats(CurrTs, _HTag, HTs, {ToSend, NewMap}, _Periods) when CurrTs =< HTs ->
    {ToSend, NewMap}.

%% There is no need to generate all heartbeats in a from to period, as only the 
%% last one will do the job
%% WARNING: That is not true in general, I have to implement a heartbeat
%%          mechanism that interleaves heartbeats in order to have a minimum rate
%%          of the stream
generate_heartbeat(HTag, From, To, Period) ->
    Times = (To - From) div Period,
    HTs = From + Times * Period,
    Heartbeat = {heartbeat, {HTag, HTs}},
    {Heartbeat, HTs + Period}.
    
-spec send_message_or_heartbeat(gen_message_or_heartbeat(), mailbox()) -> gen_imessage_or_iheartbeat().
send_message_or_heartbeat({heartbeat, Hearbeat}, SendTo) ->
    SendTo ! {iheartbeat, Hearbeat};
send_message_or_heartbeat(Msg, SendTo) ->
    SendTo ! {imsg, Msg}.
