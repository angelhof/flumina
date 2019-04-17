-module(increment_decrement_example).

-export([
	]).

-include_lib("eunit/include/eunit.hrl").
-include("type_definitions.hrl").

%% =====================================
%% ===== Computation Specification =====
%% =====================================

%% State types, events, and dependencies
-type state() :: {integer(), integer()}. % Total today and max today

-type event_i() :: message({'i',integer()},'none').
-type event_d() :: message({'d',integer()},'none').
-type event_sync() :: message('#', 'none').
-type events() :: event_i() | event_d() | event_sync().

-type event_tags() :: {'i',integer()} | {'d',integer()} | '#'.

%% TODO: DELETE LATER WHEN CODE IS REFACTORED
-spec parameterized_dependencies([integer()],[integer()]) -> #{event_tags() := [event_tags()]}.
parameterized_dependencies(I_IDs,D_IDs) ->
    ITags = [{'i',Id} || Id <- I_IDs],
    DTags = [{'d',Id} || Id <- D_IDs],
    SharpDeps = ['#'] ++ ITags ++ DTags,
    IDeps = ['#'] ++ DTags,
    DDeps = ['#'] ++ ITags,
    Deps = [{'#', SharpDeps}]
            ++ [{{'i', Id}, IDeps} || Id <- I_IDs]
            ++ [{{'d', Id}, DDeps} || Id <- D_IDs],
    maps:from_list(Deps).

% dependencies() ->
%     #{'i' => ['d','#'],
%       'd' => ['i','#'],
%       '#' => ['i','d','#']
%      }.

%% Sequential Specification
-spec init_state() -> state().
init_state() -> {0,0}.

-spec update(events(), state(), pid()) -> state().
update({'#', _Timestamp, 'none'}, {_Total, Max}, SinkPid) ->
    SinkPid ! {Max},
    {0,0};
update({{Tag,_ImpID}, _Timestamp, 'none'}, {Total, Max}, _SinkPid) ->
    case Tag of
        'i' ->
            NewTotal = Total + 1;
        'd' ->
            NewTotal = Total - 1
    end,
    NewMax = max(NewTotal,Max),
    {NewTotal,NewMax}.

%% Parallelization Primitives
-spec fork(split_preds(), state()) -> {state(), state()}.
fork(_, {Total, Max}) ->
    {{Total, Max}, {0, 0}}

-spec join(state(), state()) -> state().
join({X1, Y1}, {X2, _Y2}) -> % Y2 not used
    X = X1 + X2,
    {X, max(Y1,X)}.

%% ============================
%% ===== Experiment Setup =====
%% ============================

-spec distributed_setup(atom(), [atom()], [atom()], float(), float(), float(), float(), float(), float(), float(), integer(), optimizer_type()) -> ok.
distributed_setup(SharpNodeName, INodeNames, DNodeNames, SharpRate, IRate, BRate, SharpHBRate, IHBRate, BHBRate, RateMultiplier, UpdateCost, Optimizer) ->
    %% Print arguments to IO
    io:format("Setting up edge cluster with args:~n~p~n", [[SharpNodeName, INodeNames, DNodeNames, SharpRate, IRate, BRate, SharpHBRate, IHBRate, BHBRate, RateMultiplier, UpdateCost, Optimizer]]),

    %% Nodes and Implementation Tags
    true = register('sink', self()),
    SinkName = {sink, node()},
    NumINodes = length(INodeNames),
    NumDNodes = length(DNodeNames),
    ITags = [{i,Id} || Id <- lists:seq(1,NumINodes)],
    DTags = [{d,Id} || Id <- lists:seq(1,NumDNodes)],
    Tags = ['#'] ++ ITags ++ DTags,

    %% Architecture with Rates
    IRateTopo = lists:zip3(INodeNames,ITags,lists:duplicate(NumINodes,IRate),
    DRateTopo = lists:zip3(DNodeNames,DTags,lists:duplicate(NumDNodes,DRate),
    SharpRateTopo = [{SharpNodeName, '#', SharpRate}],
    RateTopo = IRatesTopo ++ DRateTopo ++ SharpRateTopo,
    Topology = conf_gen:make_topology(RateTopo, SinkName),

    %% Computation Specification
    StateTypesMap = 
	#{'state' => {sets:from_list(Tags), fun update/3}
        },
    SplitsMerges = [{{'state', 'state', 'state'}, {fun fork/2, fun join/2}}],
    Dependencies = parametrized_dependencies(
            lists:seq(1,NumINodes),lists:seq(1,NumDNodes)),
    InitState = {'state', init_state()},
    Specification = 
	conf_gen:make_specification(
            StateTypesMap, SplitsMerges, Dependencies, InitState),

    %% Logging and configuration tree
    LogTriple = log_mod:make_num_log_triple(),    
    ConfTree = conf_gen:generate(Specification, Topology, 
				 [{optimizer,Optimizer}, 
				  {checkpoint, fun conf_gen:always_checkpoint/2},
				  {log_triple, LogTriple}]),
    
    %% Input Streams
    {Is, Ds} = parametrized_input_distr_example(NumberAs, RatioAB, HeartbeatBRatio),
    %% InputStreams = [{A1input, {a,1}, 30}, {A2input, {a,2}, 30}, {BsInput, b, 30}],
    AInputStreams = [{AIn, ATag, RateMultiplier} || {AIn, ATag} <- lists:zip(As, ATags)],
    BInputStream = {Bs, b, RateMultiplier},
    InputStreams = [BInputStream|AInputStreams],
    
    %% Log the input times of b messages
    _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun = 
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, steady_timestamp, LoggerInitFun),
    
    SinkPid ! finished,
    ok,
    
    
    ExecPid = spawn_link(?MODULE, distributed_experiment_conf, 
			 [SinkName, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio, Optimizer]),
    LoggerInitFun = 
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

%% Input to the system

% generate_distributed_input(ITags, DTags, IRate, DRate, SharpRate, )
% 
% parametrized_input_distr_example(NumberAs, RatioAB, HeartbeatBRatio) ->
%     LengthAStream = 1000000,
%     As = [make_as(Id, LengthAStream, 1) || Id <- lists:seq(1, NumberAs)],
% 
%     LengthBStream = LengthAStream div RatioAB,
%     %% Bs = [{b, RatioAB + (RatioAB * BT), empty} 
%     %% 	  || BT <- lists:seq(0,LengthBStream)]
%     %% 	++ [{heartbeat, {b,LengthAStream + 1}}],
%     Bs = lists:flatten(
% 	   [[{heartbeat, {b, (T * RatioAB div HeartbeatBRatio) + (RatioAB * BT)}} 
% 	    || T <- lists:seq(0, HeartbeatBRatio - 1)] 
% 	   ++ [{b, RatioAB + (RatioAB * BT), RatioAB + (RatioAB * BT)}]
% 	   || BT <- lists:seq(0,LengthBStream)])
% 	++ [{heartbeat, {b,LengthAStream + 1}}],
%     {As, Bs}.




% distributed() ->
%     true = register('sink', self()),
%     SinkName = {sink, node()},
%     _ExecPid = spawn_link(?MODULE, distributed_conf, [SinkName]),
%     util:sink().
% 
% distributed_conf(SinkPid) ->
%     %% Architecture
%     Rates = [{node(), minute, 10},
% 	     {node(), {a,1}, 1000},
% 	     {node(), {a,2}, 1000},
% 	     {node(), b, 1000}],
%     Topology =
% 	conf_gen:make_topology(Rates, SinkPid),
% 
%     %% Computation
%     Tags = [minute, {a,1}, {a,2}, b],
%     StateTypesMap = 
% 	#{'state0' => {sets:from_list(Tags), fun update0/3},
% 	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update_a/3},
% 	  'state_b' => {sets:from_list([b]), fun update_b/3}},
%     SplitsMerges = [{{'state0', 'state_a', 'state_b'}, {fun split1/2, fun merge1/2}},
% 		    {{'state_a', 'state_a', 'state_a'}, {fun split_a/2, fun merge_a/2}}],
%     Dependencies = dependencies(),
%     InitState = {'state0', init_state()},
%     Specification = 
% 	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),
% 
%     ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_greedy}]),
% 
%     %% Set up where will the input arrive
%     create_producers(fun minute_markers_input/0, minute, ConfTree, Topology),
% 
%     SinkPid ! finished.
% 





% update_a({{a, Id}, _Ts, Thermo}, ThermoMap, _SendTo) ->
%     maps:update_with({a, Id}, 
% 		     fun(PrevMax) ->
% 			     max(Thermo, PrevMax)
% 		     end, ThermoMap).





% 
% 
% %% Run a distributed experiment for this example
% distributed() ->
%     true = register('sink', self()),
%     SinkName = {sink, node()},
%     _ExecPid = spawn_link(?MODULE, distributed_conf, [SinkName]),
%     util:sink().
% 
% %% Run a distributed experiment given a Sink node
% distributed_conf(SinkPid) ->
%     %% Architecture
%     Rates = [{node(), minute, 10},
% 	     {node(), {a,1}, 1000},
% 	     {node(), {a,2}, 1000},
% 	     {node(), b, 1000}],
%     Topology =
% 	conf_gen:make_topology(Rates, SinkPid),
% 
%     %% Computation
%     Tags = [minute, {a,1}, {a,2}, b],
%     StateTypesMap = 
% 	#{'state0' => {sets:from_list(Tags), fun update0/3},
% 	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update_a/3},
% 	  'state_b' => {sets:from_list([b]), fun update_b/3}},
%     SplitsMerges = [{{'state0', 'state_a', 'state_b'}, {fun split1/2, fun merge1/2}},
% 		    {{'state_a', 'state_a', 'state_a'}, {fun split_a/2, fun merge_a/2}}],
%     Dependencies = dependencies(),
%     InitState = {'state0', init_state()},
%     Specification = 
% 	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),
% 
%     ConfTree = conf_gen:generate(Specification, Topology, optimizer_greedy),
% 
%     %% Set up where will the input arrive
%     create_producers(fun minute_markers_input/0, minute, ConfTree, Topology),
% 
%     SinkPid ! finished.
% 
% sequential() ->
%     true = register('sink', self()),
%     SinkName = {sink, node()},
%     _ExecPid = spawn_link(?MODULE, sequential_conf, [SinkName]),
%     util:sink().
% 
% sequential_conf(SinkPid) ->
%     %% Architecture
%     Rates = [{node(), minute, 10},
% 	     {node(), {a,1}, 1000},
% 	     {node(), {a,2}, 1000},
% 	     {node(), b, 1000}],
%     Topology =
% 	conf_gen:make_topology(Rates, SinkPid),
% 
%     %% Computation
%     Tags = [minute, {a,1}, {a,2}, b],
%     StateTypesMap = 
% 	#{'state0' => {sets:from_list(Tags), fun update0/3}},
%     SplitsMerges = [],
%     Dependencies = dependencies(),
%     InitState = {'state0', init_state()},
%     Specification = 
% 	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),
% 
%     ConfTree = conf_gen:generate(Specification, Topology, optimizer_sequential),
% 
%     %% Set up where will the input arrive
%     create_producers(fun minute_markers_input/0, minute, ConfTree, Topology),
% 
%     SinkPid ! finished.
% 
% create_producers(MarkerFun, MarkerTag, ConfTree, Topology) ->
%     Input1 = a1_input_with_heartbeats(),
%     Input2 = a2_input_with_heartbeats(),
%     Input3 = b_input_with_heartbeats(),
%     Input4 = MarkerFun(),
% 
%     InputStreams = [{Input1, {a,1}, 100}, {Input2, {a,2}, 100}, {Input3, b, 100}, {Input4, MarkerTag, 100}],
%     producer:make_producers(InputStreams, ConfTree, Topology).
% 
% 
% 
% 
% 
