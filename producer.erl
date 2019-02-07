-module(producer).

-export([interleave_heartbeats/3]).

%%%
%%% This module contains code that will be usually used by producer nodes
%%% 


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
	[{Tag, Ts, Payload}|HeartbeatsToSend] ++ StreamAcc,
    interleave_heartbeats(Rest, NewNextHeartbeats, Periods, NewStreamAcc, Until).
	
update_next_heartbeats(CurrTs, HTag, HTs, {ToSend, NewMap}, Periods) when CurrTs > HTs ->
    %% If the current timestamp is larger than the next heartbeat timestamp
    %% we have to generate a heartbeat and update the next heartbeat timestamp
    {Heartbeat, NextHTs} = 
	generate_heartbeat(HTag, HTs, CurrTs, maps:get(HTag, Periods)),
    {[Heartbeat|ToSend], maps:update(HTag, NextHTs, NewMap)};
update_next_heartbeats(CurrTs, HTag, HTs, {ToSend, NewMap}, Periods) when CurrTs =< HTs ->
    {ToSend, NewMap}.

%% There is no need to generate all heartbeats in a from to period, as only the 
%% last one will do the job
generate_heartbeat(HTag, From, To, Period) ->
    Times = (To - From) div Period,
    HTs = From + Times * Period,
    Heartbeat = {heartbeat, {HTag, HTs}},
    {Heartbeat, HTs + Period}.
    
