-module(taxiexample).

-export([main/0,
	 sequential/0,
	 distributed/0,
	 sequential_1/0,
	 distributed_1/0,
	 source/2]).

-include("type_definitions.hrl").

main() ->
    distributed().

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

distributed_1() ->

    %% Configuration Tree
    Funs = {fun update_1/3, fun split_1/2, fun merge_1/2},
    %% We give the parallel nodes crash functions because they should never be called
    %% for leaf nodes
    FunsP = {fun update_id_1/3, fun util:crash/2, fun util:crash/2},
    Ids = init_state_1(),
    Node1 = {Ids, fun isId1/1, FunsP, []},
    Node2 = {Ids, fun isId2/1, FunsP, []},
    Node0  = {Ids, fun true_pred/1, Funs, [Node1, Node2]},
    PidTree = configuration:create(Node0, dependencies_1(), self()),
    {{_NP0, MP0}, 
     [{{_NP1, MP1}, []}, 
      {{_NP2, MP2}, []}]} = PidTree,

    %% Set up where will the input arrive
    create_producers(fun sliding_period_input/0, [MP1, MP2, MP0]),

    sink().

sequential_1() ->

    %% Configuration Tree
    Funs = {fun update_1/3, fun split_1/2, fun merge_1/2},
    Ids = init_state_1(),
    Node  = {Ids, fun true_pred/1, Funs, []},
    PidTree = configuration:create(Node, dependencies_1(), self()),
    {{_HeadNodePid, HeadMailboxPid}, _} = PidTree,

    %% Set up where will the input arrive
    create_producers(fun sliding_period_input/0, [HeadMailboxPid, HeadMailboxPid, HeadMailboxPid]),

    sink().

%% This is what our compiler would come up with
distributed() ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    FunsP = {fun update_id/3, fun split/2, fun merge/2},
    Ids = init_state(),
    Node1 = {Ids, fun isId1/1, FunsP, []},
    Node21 = {Ids, fun isId2/1, FunsP, []},
    Node22 = {Ids, fun isId2/1, FunsP, []},
    Node2 = {Ids, fun isId2/1, FunsP, [Node21, Node22]},    
    Node0  = {Ids, fun true_pred/1, Funs, [Node1, Node2]},
    PidTree = configuration:create(Node0, dependencies(), self()),
    {{_NP0, MP0}, 
     [{{_NP1, MP1}, []}, 
      {{_NP2, MP2}, [_, _]}]} = PidTree,

    %% Set up where will the input arrive
    create_producers(fun hour_markets_input/0, [MP1, MP2, MP0]),

    sink().

sequential() ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    Ids = init_state(),
    Node  = {Ids, fun true_pred/1, Funs, []},
    PidTree = configuration:create(Node, dependencies(), self()),
    {{_HeadNodePid, HeadMailboxPid}, _} = PidTree,

    %% Set up where will the input arrive
    create_producers(fun hour_markets_input/0, [HeadMailboxPid, HeadMailboxPid, HeadMailboxPid]),

    sink().

create_producers(MarkerFun, [Pid1, Pid2, Pid3]) ->
    Input1 = id1_input_with_heartbeats(),
    Producer1 = spawn_link(?MODULE, source, [Input1, Pid1]),

    Input2 = id2_input_with_heartbeats(),
    Producer2 = spawn_link(?MODULE, source, [Input2, Pid2]),

    Input3 = MarkerFun(),
    Producer3 = spawn_link(?MODULE, source, [Input3, Pid3]).

%%
%% The specification of the computation
%%

%% This computation outputs the sum of tips for each driver with a sliding window 
%% of length 1 hour that moves every 20 minutes.

%% The implementation here keeps periods of the greatest common divisor of
%% both the window length and slide. Here this is the slide itself so the
%% implementation is simplified.

update_id_1({Tag, Ts, Value}, {TipSums, WindowTips}, SendTo) ->
    %% This is here for debugging purposes
    SendTo ! {"Time", Ts, Tag, Value},
    Tip = maps:get(Tag, TipSums),
    {maps:update(Tag, Tip + Value, TipSums), WindowTips}.

update_1({window, Ts, marker}, {TipSums, WindowTips0}, SendTo) ->
    %% This keeps the past sliding periods tips for each
    %% driver in the map
    WindowTips1 =
	maps:fold(
	  fun(Id, Tips, WTMap) ->
		  maps:put({Id, Ts}, Tips, WTMap)
	  end, WindowTips0, TipSums),
    %% Print the window sums with the last 3 sliding periods.
    %% Also delete the oldest period from the WindowTipMap
    WindowTips2 = output_tip_sums(WindowTips1, maps:keys(TipSums), Ts, SendTo),
    ResetTipSums = maps:map(fun(_,_) -> 0 end, TipSums),
    {ResetTipSums, WindowTips2};
update_1(Msg, State, SendTo) ->
    update_id_1(Msg, State, SendTo).



output_tip_sums(WindowTips, DriverIds, Ts, SendTo) ->
    [RandomId|_] = DriverIds,
    case maps:is_key({RandomId, Ts - 40}, WindowTips) of
	true ->
	    %% Sum the 3 last sliding periods
	    Sums = [ {Id, lists:sum(
		       [maps:get({Id, T}, WindowTips) || T <- [Ts-40, Ts-20, Ts]])}
		     || Id <- DriverIds],
	    %% Ouput the sum
	    SendTo ! {"Tips per rider", Sums, "Period:", Ts - 60, Ts},
	    %% Remove the oldest sliding period for each driver
	    lists:foldl(
	      fun(Id, WTMap) ->
		      maps:remove({Id, Ts-40}, WTMap)
	      end, WindowTips, DriverIds);
	false ->
	    WindowTips
    end.

%% TODO: At the moment we also split the WindowTips, which could just be
%%       kept normally in the parent node's state. Ideally we would like to
%%       allow for both split and merge to split 3-ways so that some state can be left behind.
%%
%% Alternative Solution:
%%       Another way we could deal with this would be to first split on windows
%%       and ids, and then on ids, thus keeping the WindowTips in the "windows"
%%       node. However in that case, we would need to allow for some optimization
%%       that can keep many virtual nodes in the same physical node, as the parent
%%       ones, don't really do any processing most of the time. So in essence,
%%       a parent can almost always be in the same node with one of its children.
split_1({Pred1, Pred2}, {TipSums, WindowTips}) ->
    {{maps:filter(fun(K,_) -> Pred1({K, dummy, dummy}) end, TipSums),
      maps:filter(fun({K, _T},_) -> Pred1({K, dummy, dummy}) end, WindowTips)},
     {maps:filter(fun(K,_) -> Pred2({K, dummy, dummy}) end, TipSums),
      maps:filter(fun({K, _T},_) -> Pred2({K, dummy, dummy}) end, WindowTips)}}.

merge_1({TipsMap1, WindowTips1}, {TipsMap2, WindowTips2}) ->
    {merge_with(
       fun(_K, V1, V2) ->
	       V1 + V2
       end, TipsMap1, TipsMap2), 
     merge_with(
       fun(_K, V1, V2) ->
	       %% There shouldn't be a common key between those
	       erlang:halt(1)
       end, WindowTips1, WindowTips2)}.


dependencies_1() ->
    #{{id,1} => [window],
      {id,2} => [window],
      window => [{id,1}, {id,2}, window]
     }.

init_state_1() ->
    {maps:from_list([{{id,1}, 0}, {{id,2}, 0}]), #{}}.


%% This computation outputs the sum of tips for each driver every hour.


%% This is the update that the parallel nodes will run
%% It is the same as the other ones, but the parallel
%% nodes are supposed to have maps for less ids
update_id({Tag, Ts, Value}, TipSums, SendTo) ->
    %% This is here for debugging purposes
    SendTo ! {"Time", Ts, Tag, Value, self()},
    Tip = maps:get(Tag, TipSums),
    maps:update(Tag, Tip + Value, TipSums).

%% This is the sequential update of the total 
update({hour, Ts, marker}, TipSums, SendTo) ->
    AllSums = maps:to_list(TipSums),
    SendTo ! {"Tips per rider", AllSums},
    maps:map(fun(_,_) -> 0 end, TipSums);
update(Msg, TipSums, SendTo) ->
    update_id(Msg, TipSums, SendTo).


merge(TipsMap1, TipsMap2) ->
    merge_with(
      fun(_K, V1, V2) ->
	      V1 + V2
      end, TipsMap1, TipsMap2).

split({Pred1, Pred2}, TipSums) ->
    {maps:filter(fun(K,_) -> Pred1({K, dummy, dummy}) end, TipSums),
     maps:filter(fun(K,_) -> Pred2({K, dummy, dummy}) end, TipSums)}.

dependencies() ->
    #{{id,1} => [hour],
      {id,2} => [hour],
      hour => [{id,1}, {id,2}, hour]
     }.

init_state() ->
    maps:from_list([{{id,1}, 0}, {{id,2}, 0}]).

%% The predicates
isId1({{id,1}, _, _}) -> true;
isId1(_) -> false.

isId2({{id,2}, _, _}) -> true;
isId2(_) -> false.    

true_pred(_) -> true.

%% This function accepts a merging function that takes a 
%% key and the two associated values and then merges them.
%% It merges two maps, and in case they both have a key, 
%% it merges the two values based on the merge function.
merge_with(Fun, Map1, Map2) ->
    maps:fold(
      fun(K2, V2, Map) ->
	      maps:update_with(
		K2,
		fun(V1) ->
			Fun(K2, V1, V2)
		end, V2, Map)
      end, Map1, Map2).


%% Source and Sink

source([], _SendTo) ->
    ok;
source([Msg|Rest], SendTo) ->
    case Msg of
	{heartbeat, Hearbeat} ->
	    SendTo ! {iheartbeat, Hearbeat};
	_ ->
	    SendTo ! {imsg, Msg}
    end,
    source(Rest, SendTo).

sink() ->
    receive
	Msg ->
	    io:format("~p~n", [Msg]),
	    sink()
    end.


%% Some input examples

hour_markets_input() ->
    Input = [{hour, T * 60, marker} || T <- lists:seq(1, 10)],
    producer:interleave_heartbeats(Input, #{hour => 60}, 500).

sliding_period_input() ->
    Input = [{window, T * 20, marker} || T <- lists:seq(1, 30)],
    producer:interleave_heartbeats(Input, #{window => 60}, 500).

id1_input_with_heartbeats() ->
    producer:interleave_heartbeats(id1_input(), #{{id,1} => 10}, 500).

id2_input_with_heartbeats() ->
    producer:interleave_heartbeats(id2_input(), #{{id,2} => 10}, 500).

id1_input() ->
    Inputs = 
	[{1, 15},
	 {10, 20},
	 {14, 28},
	 {25, 10},
	 {45, 21},
	 {75, 15},
	 {100, 23},
	 {121, 10},
	 {150, 34},
	 {174, 12},
	 {210, 21},
	 {234, 15},
	 {250, 12}],
    [{{id,1}, Ts, Tip} || {Ts, Tip} <- Inputs].

id2_input() ->
    Inputs = 
	[{5, 25},
	 {32, 26},
	 {41, 10},
	 {53, 24},
	 {59, 30},
	 {71, 15},
	 {84, 29},
	 {103, 21},
	 {125, 18},
	 {156, 12},
	 {189, 21},
	 {195, 15},
	 {210, 18},
	 {231, 12},
	 {245, 21},
	 {268, 15},
	 {290, 12}],
    [{{id,2}, Ts, Tip} || {Ts, Tip} <- Inputs].    

