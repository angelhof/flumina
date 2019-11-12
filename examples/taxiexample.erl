-module(taxiexample).

-export([main/0,
	 sequential/0,
	 distributed/0,
	 sequential_1/0,
	 distributed_1/0,
	 sequential_2/0,
	 distributed_2/0,
	 sequential_conf/1,
	 distributed_conf/1,
	 sequential_conf_1/1,
	 distributed_conf_1/1,
	 sequential_conf_2/1,
	 distributed_conf_2/1,
	 id1_positions_with_heartbeats/1,
	 id2_positions_with_heartbeats/1,
	 hour_positions_input/1,
	 hour_markets_input/1,
	 sliding_period_input/1,
	 id1_input_with_heartbeats/1,
	 id2_input_with_heartbeats/1,
	 id3_input_with_heartbeats/1
	]).

-include_lib("eunit/include/eunit.hrl").

-include("type_definitions.hrl").

main() ->
    distributed().

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

distributed_2() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, distributed_conf_2, [SinkName]),
    util:sink().

distributed_conf_2(SinkPid) ->
    %% Architecture
    Rates = [{node(), hour, 10},
	     {node(), {id,1}, 1000},
	     {node(), {id,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update_2/3, fun split_2/2, fun merge_2/2},
    FunsP = {fun update_id_2/3, fun split_2/2, fun merge_2/2},
    Ids = init_state_2(),
    {Ids1, Ids2} = split_2({fun isTagId1/1, fun isTagId2/1}, Ids),
    Node1 = {Ids1, node(), {fun isTagId1/1, fun isId1/1}, FunsP, []},
    Node2 = {Ids2, node(), {fun isTagId2/1, fun isId2/1}, FunsP, []},
    Node0  = {Ids, node(), {fun isTagHour/1, fun isHour/1}, Funs, [Node1, Node2]},
    ConfTree = configuration:create(Node0, dependencies_2(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    Input1 = {fun taxiexample:id1_positions_with_heartbeats/1, [node()]},
    Input2 = {fun taxiexample:id2_positions_with_heartbeats/1, [node()]},
    Input3 = {fun taxiexample:hour_positions_input/1, [node()]},
    InputStreams = [{Input1, {{id,1}, node()}, 10},
		    {Input2, {{id,2}, node()}, 10},
		    {Input3, {hour, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    %% io:format("Input3: ~p~n", [Input3]),

    SinkPid ! finished.

sequential_2() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sequential_conf_2, [SinkName]),
    util:sink().

sequential_conf_2(SinkPid) ->
    %% Architecture
    Rates = [{node(), hour, 10},
	     {node(), {id,1}, 1000},
	     {node(), {id,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [hour, {id,1}, {id,2}],
    StateTypesMap =
	#{'state2' => {sets:from_list(Tags), fun update_2/3}},
    SplitsMerges = [],
    Dependencies = dependencies_2(),
    InitState = {'state2', init_state_2()},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_sequential}]),

    %% Set up where will the input arrive
    Input1 = {fun taxiexample:id1_positions_with_heartbeats/1, [node()]},
    Input2 = {fun taxiexample:id2_positions_with_heartbeats/1, [node()]},
    Input3 = {fun taxiexample:hour_positions_input/1, [node()]},
    InputStreams = [{Input1, {{id,1}, node()}, 10},
		    {Input2, {{id,2}, node()}, 10},
		    {Input3, {hour, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    SinkPid ! finished.

distributed_1() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, distributed_conf_1, [SinkName]),
    util:sink().

distributed_conf_1(SinkPid) ->
    %% Architecture
    Rates = [{node(), window, 10},
	     {node(), {id,1}, 1000},
	     {node(), {id,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update_1/3, fun split_1/2, fun merge_1/2},
    %% We give the parallel nodes crash functions because they should never be called
    %% for leaf nodes
    FunsP = {fun update_id_1/3, fun util:crash/2, fun util:crash/2},
    Ids = init_state_1(),
    Node1 = {Ids, node(), {fun isTagId1/1, fun isId1/1}, FunsP, []},
    Node2 = {Ids, node(), {fun isTagId2/1, fun isId2/1}, FunsP, []},
    Node0  = {Ids, node(), {fun isTagWindow/1, fun isWindow/1}, Funs, [Node1, Node2]},
    ConfTree = configuration:create(Node0, dependencies_1(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    create_producers(fun taxiexample:sliding_period_input/1, window, node(), ConfTree, Topology),

    SinkPid ! finished.

sequential_1() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sequential_conf_1, [SinkName]),
    util:sink().

sequential_conf_1(SinkPid) ->
    %% Architecture
    Rates = [{node(), window, 10},
	     {node(), {id,1}, 1000},
	     {node(), {id,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update_1/3, fun split_1/2, fun merge_1/2},
    Ids = init_state_1(),
    Node  = {Ids, node(), {fun true_pred/1, fun true_pred/1}, Funs, []},
    ConfTree = configuration:create(Node, dependencies_1(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    create_producers(fun taxiexample:sliding_period_input/1, window, node(), ConfTree, Topology),

    SinkPid ! finished.

distributed() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, distributed_conf, [SinkName]),
    util:sink().

distributed_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), hour, 10},
	     {node(), {id,1}, 1000},
	     {node(), {id,2}, 1000},
	     {node(), {id,3}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    FunsP = {fun update_id/3, fun split/2, fun merge/2},
    Ids = init_state(),
    Node1 = {Ids, node(), {fun isTagId1/1, fun isId1/1}, FunsP, []},
    Node22 = {Ids, node(), {fun isTagId2/1, fun isId2/1}, FunsP, []},
    Node23 = {Ids, node(), {fun isTagId3/1, fun isId3/1}, FunsP, []},
    Node2 = {Ids, node(), {fun isTagId23/1, fun isId23/1}, FunsP, [Node22, Node23]},
    Node0  = {Ids, node(), {fun isTagHour/1, fun isHour/1}, Funs, [Node1, Node2]},
    ConfTree = configuration:create(Node0, dependencies(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    create_producers(fun taxiexample:hour_markets_input/1, hour, node(), ConfTree, Topology),

    Input3 = {fun taxiexample:id3_input_with_heartbeats/1, [node()]},
    InputStreams = [{Input3, {{id,3}, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    SinkPid ! finished.

sequential() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sequential_conf, [SinkName]),
    util:sink().

sequential_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), hour, 10},
	     {node(), {id,1}, 1000},
	     {node(), {id,2}, 1000},
	     {node(), {id,3}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    Ids = init_state(),
    Node  = {Ids, node(), {fun true_pred/1, fun true_pred/1}, Funs, []},
    ConfTree = configuration:create(Node, dependencies(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    create_producers(fun taxiexample:hour_markets_input/1, hour, node(), ConfTree, Topology),

    Input3 = {fun taxiexample:id3_input_with_heartbeats/1, [node()]},
    InputStreams = [{Input3, {{id,3}, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    SinkPid ! finished.

create_producers(MarkerFun, MarkerTag, Node, ConfTree, Topology) ->
    Input1 = {fun taxiexample:id1_input_with_heartbeats/1, [node()]},
    Input2 = {fun taxiexample:id2_input_with_heartbeats/1, [node()]},
    Input3 = {MarkerFun, [Node]},
    InputStreams = [{Input1, {{id,1}, Node}, 10},
		    {Input2, {{id,2}, Node}, 10},
		    {Input3, {MarkerTag, Node}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology).

%%
%% The specification of the computation
%%


%% This computation the total distance that each driver has moved every hour.
%% It finds the distance between each two consecutive points of each taxi driver
%% and then adds them all for each hour

update_id_2({Tag, Position}, DriverPosDists, SendTo) ->
    %% SendTo ! {"Time", Ts, Tag, Position, self()},
    {PrevPos, PrevDist} = maps:get(Tag, DriverPosDists),
    Dist = dist(PrevPos, Position),
    maps:update(Tag, {Position, Dist + PrevDist}, DriverPosDists).


update_2({hour, Ts}, DriverPosDists, SendTo) ->
    {Ids, Values} = lists:unzip(maps:to_list(DriverPosDists)),
    {_PrevPositions, Distances} = lists:unzip(Values),
    RoundedDistances = [round(D) || D <- Distances],
    SendTo ! {"Distances per rider", maps:from_list(lists:zip(Ids, RoundedDistances)), "Minutes: ", Ts},
    maps:map(fun(_,{PrevPos, Dist}) -> {PrevPos, 0} end, DriverPosDists);
update_2(Msg, DriverPosDists, SendTo) ->
    update_id_2(Msg, DriverPosDists, SendTo).

split_2({Pred1, Pred2}, DriverPosDists) ->
    {maps:filter(fun(K,_) -> Pred1(K) end, DriverPosDists),
     maps:filter(fun(K,_) -> Pred2(K) end, DriverPosDists)}.

merge_2(DriverPosDists1, DriverPosDists2) ->
    util:merge_with(
      fun(K, _V1, _V2) ->
	      %% This should never be called
	      %% Actually it could be called because preds might not be disjoint
	      util:err("Key: ~p shouldn't exist in both maps~n", [K]),
	      erlang:halt()
      end, DriverPosDists1, DriverPosDists2).

dependencies_2() ->
    #{{id,1} => [{id,1}, hour],
      {id,2} => [{id,2}, hour],
      hour => [{id,1}, {id,2}, hour]
     }.

init_state_2() ->
    maps:from_list([{{id,1}, {undef,0}},
		    {{id,2}, {undef,0}}]).

dist({X1, Y1}, {X2, Y2}) ->
    X12 = (X2 - X1) * (X2 - X1),
    Y12 = (Y2 - Y1) * (Y2 - Y1),
    math:sqrt(X12 + Y12);
dist(undef, {X2, Y2}) ->
    %% This is only here for the first update
    0.







%% This computation outputs the sum of tips for each driver with a sliding window
%% of length 1 hour that moves every 20 minutes.

%% The implementation here keeps periods of the greatest common divisor of
%% both the window length and slide. Here this is the slide itself so the
%% implementation is simplified.

update_id_1({Tag, Value}, {TipSums, WindowTips}, SendTo) ->
    %% This is here for debugging purposes
    %% SendTo ! {"Time", Ts, Tag, Value},
    Tip = maps:get(Tag, TipSums),
    {maps:update(Tag, Tip + Value, TipSums), WindowTips}.


update_1({window, Ts}, {TipSums, WindowTips0}, SendTo) ->
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
	    SendTo ! {"Tips per rider", maps:from_list(Sums), "Period:", Ts - 60, Ts},
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
    {{maps:filter(fun(K,_) -> Pred1(K) end, TipSums),
      maps:filter(fun({K, _T},_) -> Pred1(K) end, WindowTips)},
     {maps:filter(fun(K,_) -> Pred2(K) end, TipSums),
      maps:filter(fun({K, _T},_) -> Pred2(K) end, WindowTips)}}.

merge_1({TipsMap1, WindowTips1}, {TipsMap2, WindowTips2}) ->
    {util:merge_with(
       fun(_K, V1, V2) ->
	       V1 + V2
       end, TipsMap1, TipsMap2),
     util:merge_with(
       fun(_K, V1, V2) ->
	       %% There shouldn't be a common key between those
	       %% Actually there might be a common key between them
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
update_id({Tag, Value}, TipSums, SendTo) ->
    %% This is here for debugging purposes
    %% SendTo ! {"Time", Ts, Tag, Value, self()},
    Tip = maps:get(Tag, TipSums),
    maps:update(Tag, Tip + Value, TipSums).

%% This is the sequential update of the total
update({hour, Ts}, TipSums, SendTo) ->
    SendTo ! {"Tips per rider", TipSums},
    maps:map(fun(_,_) -> 0 end, TipSums);
update(Msg, TipSums, SendTo) ->
    update_id(Msg, TipSums, SendTo).


merge(TipsMap1, TipsMap2) ->
    util:merge_with(
      fun(_K, V1, V2) ->
	      V1 + V2
      end, TipsMap1, TipsMap2).

split({Pred1, Pred2}, TipSums) ->
    {maps:filter(fun(K,_) -> Pred1(K) end, TipSums),
     maps:filter(fun(K,_) -> Pred2(K) end, TipSums)}.

dependencies() ->
    #{{id,1} => [hour],
      {id,2} => [hour],
      {id,3} => [hour],
      hour => [{id,1}, {id,2}, {id, 3}, hour]
     }.

init_state() ->
    maps:from_list([{{id,1}, 0}, {{id,2}, 0}, {{id,3}, 0}]).

%% The predicates
isId1({{{id,1}, _}, _, _}) -> true;
isId1(_) -> false.

isId2({{{id,2}, _}, _, _}) -> true;
isId2(_) -> false.

isId3({{{id,3}, _}, _, _}) -> true;
isId3(_) -> false.

isId23(Msg) -> isId2(Msg) orelse isId3(Msg).

isHour({{hour, _}, _, _}) -> true;
isHour(_) -> false.

isWindow({{window, _}, _, _}) -> true;
isWindow(_) -> false.

isTagId1({id,1}) -> true;
isTagId1(_) -> false.

isTagId2({id,2}) -> true;
isTagId2(_) -> false.

isTagId3({id,3}) -> true;
isTagId3(_) -> false.

isTagId23(Tag) ->
    isTagId2(Tag) orelse isTagId3(Tag).

isTagHour(hour) -> true;
isTagHour(_) -> false.

isTagWindow(window) -> true;
isTagWindow(_) -> false.

true_pred(_) -> true.




%% Some input examples

id1_positions_with_heartbeats(Node) ->
    Msgs = producer:interleave_heartbeats(taxi_1_position_inputs(Node), {{{id,1}, Node}, 5}, 2050),
    producer:list_generator(Msgs).

id2_positions_with_heartbeats(Node) ->
    Msgs = producer:interleave_heartbeats(taxi_2_position_inputs(Node), {{{id,2}, Node}, 5}, 2050),
    producer:list_generator(Msgs).

taxi_1_position_inputs(Node) ->
    Positions = [{X, 0} || X <- lists:seq(1,1000)] ++ [{X + 1000, X} || X <- lists:seq(1,1000)],
    TsPositions = lists:zip(Positions, lists:seq(1, length(Positions))),
    [{{{id,1}, Pos}, Node, Ts} || {Pos, Ts} <- TsPositions].

taxi_2_position_inputs(Node) ->
    Positions = [{0, X} || X <- lists:seq(1,1000)] ++ [{X, 1000} || X <- lists:seq(1,1000)],
    TsPositions = lists:zip(Positions, lists:seq(1, length(Positions))),
    [{{{id,2}, Pos}, Node, Ts} || {Pos, Ts} <- TsPositions].

hour_positions_input(Node) ->
    Input = [{{hour, T * 60}, Node, T * 60} || T <- lists:seq(1, 35)],
    Msgs = producer:interleave_heartbeats(Input, {{hour, Node}, 60}, 2100),
    producer:list_generator(Msgs).


hour_markets_input(Node) ->
    Input = [{{hour, T * 60}, Node, T * 60} || T <- lists:seq(1, 10)],
    Msgs = producer:interleave_heartbeats(Input, {{hour, Node}, 60}, 650),
    producer:list_generator(Msgs).

sliding_period_input(Node) ->
    Input = [{{window, T * 20}, Node, T * 20} || T <- lists:seq(1, 30)],
    Msgs = producer:interleave_heartbeats(Input, {{window, Node}, 60}, 650),
    producer:list_generator(Msgs).

id1_input_with_heartbeats(Node) ->
    Msgs = producer:interleave_heartbeats(id1_input(Node), {{{id,1}, Node}, 10}, 650),
    producer:list_generator(Msgs).

id2_input_with_heartbeats(Node) ->
    Msgs = producer:interleave_heartbeats(id2_input(Node), {{{id,2}, Node}, 10}, 650),
    producer:list_generator(Msgs).

id3_input_with_heartbeats(Node) ->
    Msgs = producer:interleave_heartbeats(id3_input(Node), {{{id,3}, Node}, 10}, 650),
    producer:list_generator(Msgs).





id1_input(Node) ->
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
    [{{{id,1}, Tip}, Node, Ts} || {Ts, Tip} <- Inputs].

id2_input(Node) ->
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
    [{{{id,2}, Tip}, Node, Ts} || {Ts, Tip} <- Inputs].

id3_input(Node) ->
    Inputs =
	[{11, 15},
	 {21, 15},
	 {41, 25},
	 {51, 25},
	 {60, 30},
	 {69, 15},
	 {78, 23},
	 {94, 12},
	 {112, 32},
	 {125, 18},
	 {149, 12},
	 {174, 21},
	 {196, 15},
	 {209, 19},
	 {228, 15},
	 {239, 13},
	 {272, 24},
	 {285, 28}],
    [{{{id,3}, Tip}, Node, Ts} || {Ts, Tip} <- Inputs].


%% -------- TESTS -------- %%

output_2() ->
    Outputs =
	[#{{id,1} => 58, {id,2} => 58}] ++
	lists:duplicate(15, #{{id,1} => 60, {id,2} => 60}) ++
	[#{{id,1} => 68, {id,2} => 60}] ++
	lists:duplicate(16, #{{id,1} => 85, {id,2} => 60}) ++
	[#{{id,1} => 30, {id,2} => 21}],
    ZipOutputs = lists:zip(Outputs, lists:seq(60, 2040, 60)),
    [{"Distances per rider",Map,"Minutes: ",Ts} || {Map, Ts} <- ZipOutputs].

distributed_2_test_() ->
    Rounds = lists:seq(1,10),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf_2}, output_2()))
      end} || _ <- Rounds].

sequential_2_test_() ->
    Rounds = lists:seq(1,10),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, sequential_conf_2}, output_2()))
      end} || _ <- Rounds].

output_1() ->
    Outputs =
	[{#{{id,1} => 94, {id,2} => 115},0,60},
	 {#{{id,1} => 46, {id,2} => 105},20,80},
	 {#{{id,1} => 36, {id,2} => 108},40,100},
	 {#{{id,1} => 38, {id,2} => 65},60,120},
	 {#{{id,1} => 33, {id,2} => 68},80,140},
	 {#{{id,1} => 67, {id,2} => 51},100,160},
	 {#{{id,1} => 56, {id,2} => 30},120,180},
	 {#{{id,1} => 46, {id,2} => 48},140,200},
	 {#{{id,1} => 33, {id,2} => 54},160,220},
	 {#{{id,1} => 36, {id,2} => 66},180,240},
	 {#{{id,1} => 48, {id,2} => 51},200,260},
	 {#{{id,1} => 27, {id,2} => 48},220,280},
	 {#{{id,1} => 12, {id,2} => 48},240,300},
	 {#{{id,1} => 0 , {id,2} => 27},260,320},
	 {#{{id,1} => 0 , {id,2} => 12},280,340},
	 {#{{id,1} => 0, {id,2} => 0},300,360},
	 {#{{id,1} => 0, {id,2} => 0},320,380},
	 {#{{id,1} => 0, {id,2} => 0},340,400},
	 {#{{id,1} => 0, {id,2} => 0},360,420},
	 {#{{id,1} => 0, {id,2} => 0},380,440},
	 {#{{id,1} => 0, {id,2} => 0},400,460},
	 {#{{id,1} => 0, {id,2} => 0},420,480},
	 {#{{id,1} => 0, {id,2} => 0},440,500},
	 {#{{id,1} => 0, {id,2} => 0},460,520},
	 {#{{id,1} => 0, {id,2} => 0},480,540},
	 {#{{id,1} => 0, {id,2} => 0},500,560},
	 {#{{id,1} => 0, {id,2} => 0},520,580},
	 {#{{id,1} => 0, {id,2} => 0},540,600}],
    [{"Tips per rider",Map,"Period:",St,End} || {Map, St, End} <- Outputs].

distributed_1_test_() ->
    Rounds = lists:seq(1,100),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf_1}, output_1()))
      end} || _ <- Rounds].

sequential_1_test_() ->
    Rounds = lists:seq(1,100),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, sequential_conf_1}, output_1()))
      end} || _ <- Rounds].


output() ->
    Outputs =
	[#{{id,1} => 94,{id,2} => 115, {id,3} => 80},
	 #{{id,1} => 38,{id,2} => 65, {id,3} => 112},
	 #{{id,1} => 56,{id,2} => 30, {id,3} => 51},
	 #{{id,1} => 36,{id,2} => 66, {id,3} => 62},
	 #{{id,1} => 12,{id,2} => 48, {id,3} => 52},
	 #{{id,1} => 0, {id,2} => 0, {id,3} => 0},
	 #{{id,1} => 0, {id,2} => 0, {id,3} => 0},
	 #{{id,1} => 0, {id,2} => 0, {id,3} => 0},
	 #{{id,1} => 0, {id,2} => 0, {id,3} => 0},
	 #{{id,1} => 0, {id,2} => 0, {id,3} => 0}],
    [{"Tips per rider", Map} || Map <- Outputs].

distributed_test_() ->
    Rounds = lists:seq(1,100),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf}, output()))
      end} || _ <- Rounds].

sequential_test_() ->
    Rounds = lists:seq(1,100),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, sequential_conf}, output()))
      end} || _ <- Rounds].
