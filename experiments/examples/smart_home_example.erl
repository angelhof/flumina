-module(smart_home_example).

-export([main/0,
	 sequential/0,
	 distributed/0,
	 sequential_conf/1,
	 distributed_conf/1,
	 minute_markers_input/1,
	 input_with_heartbeats/3,
	 a1_input/1,
	 a2_input/1,
	 b_input/1
	]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flumina/include/type_definitions.hrl").

main() ->
    sequential().

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

distributed() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, distributed_conf, [SinkName]),
    util:sink().

distributed_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), minute, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000},
	     {node(), b, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [minute, {a,1}, {a,2}, b],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update0/3},
	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update_a/3},
	  'state_b' => {sets:from_list([b]), fun update_b/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_b'}, {fun split1/2, fun merge1/2}},
		    {{'state_a', 'state_a', 'state_a'}, {fun split_a/2, fun merge_a/2}}],
    Dependencies = dependencies(),
    InitState = {'state0', init_state()},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_greedy}]),

    %% Set up where will the input arrive
    create_producers(fun smart_home_example:minute_markers_input/1, minute, ConfTree, Topology),

    SinkPid ! finished.

sequential() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sequential_conf, [SinkName]),
    util:sink().

sequential_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), minute, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000},
	     {node(), b, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [minute, {a,1}, {a,2}, b],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update0/3}},
    SplitsMerges = [],
    Dependencies = dependencies(),
    InitState = {'state0', init_state()},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_sequential}]),

    %% Set up where will the input arrive
    create_producers(fun smart_home_example:minute_markers_input/1, minute, ConfTree, Topology),

    SinkPid ! finished.

create_producers(MarkerFun, MarkerTag, ConfTree, Topology) ->
    Input1 = {fun smart_home_example:input_with_heartbeats/3, [node(), {a,1}, fun a1_input/1]},
    Input2 = {fun smart_home_example:input_with_heartbeats/3, [node(), {a,2}, fun a2_input/1]},
    Input3 = {fun smart_home_example:input_with_heartbeats/3, [node(), b, fun b_input/1]},
    Input4 = {MarkerFun, [node()]},

    InputStreams = [{Input1, {{a,1},node()}, 100},
		    {Input2, {{a,2},node()}, 100},
		    {Input3, {b,node()}, 100},
		    {Input4, {MarkerTag,node()}, 100}],
    producer:make_producers(InputStreams, ConfTree, Topology).

%%
%% The specification of the computation
%%

%% TODO: Describe the computation in words

-type thermo_id() :: {'a', integer()}.
-type pressure_tag() :: 'b'.
-type sync_tag() :: 'minute'.

-type pressure() :: integer().
-type pressure_spike() :: pressure(). %% That is a difference between two consecutive pressures
-type temperature() :: integer().

-type a_message() :: message(thermo_id(), integer()).
-type b_message() :: message(pressure_tag(), integer()).
-type sync_message() :: message(sync_tag(), 'marker').

-type thermo_map() :: #{thermo_id() := temperature()}.

%%
%% Universal state
%%

%% The state keeps the latest pressure reading,
%%                 the largest pressure spike,
%%                 the largest temperature for each thermometer
-type state0() :: {pressure(), pressure_spike(), thermo_map()}.
-type tags0() :: thermo_id() | pressure_tag() | sync_tag().
-type messages0() :: a_message()
		   | b_message()
		   | sync_message().


-spec update0(messages0(), state0(), pid()) -> state0().
update0({minute, Ts}, {LastPres, MaxSpike, ThermoMap}, SendTo) ->
    MinMaxThermo = lists:min(maps:values(ThermoMap)),
    case MaxSpike >= 100 andalso MinMaxThermo >= 50 of
	true ->
	    SendTo ! {"Alert!", Ts, "Pressure Spike:", MaxSpike, "MinMaxThermo:", MinMaxThermo};
	false ->
	    ok
    end,
    NewThermoMap = maps:map(fun(_,_MaxT) -> 0 end, ThermoMap),
    {LastPres, 0, NewThermoMap};
update0({b, Pressure}, {LastPres, MaxSpike, ThermoMap}, _SendTo) ->
    NewMaxSpike = max(Pressure - LastPres, MaxSpike),
    {Pressure, NewMaxSpike, ThermoMap};
update0({{a, Id}, Thermo}, {LastPres, MaxSpike, ThermoMap}, _SendTo) ->
    NewThermoMap =
	maps:update_with({a, Id},
			 fun(PrevMax) ->
				 max(Thermo, PrevMax)
			 end, ThermoMap),
    {LastPres, MaxSpike, NewThermoMap}.

%%
%% Pressure state and its update
%%

-type state_b() :: {pressure(), pressure_spike()}.
-type tags_b() :: pressure_tag().
-type messages_b() :: b_message().

%% To think about: Ideally we would want to not have to rewrite the same update
-spec update_b(messages_b(), state_b(), pid()) -> state_b().
update_b({b, Pressure}, {LastPres, MaxSpike}, _SendTo) ->
    NewMaxSpike = max(Pressure - LastPres, MaxSpike),
    {Pressure, NewMaxSpike}.

%%
%% Thermo state and its update
%%

-type state_a() :: thermo_map().
-type tags_a() :: thermo_id().
-type messages_a() :: a_message().

%% To think about: Ideally we would want to not have to rewrite the same update
-spec update_a(messages_a(), state_a(), pid()) -> state_a().
update_a({{a, Id}, Thermo}, ThermoMap, _SendTo) ->
    maps:update_with({a, Id},
		     fun(PrevMax) ->
			     max(Thermo, PrevMax)
		     end, ThermoMap).

%%
%% Split from universal state to pressure and thermo state
%%

%% This split contains two simple predicates because it is only
%% allowed to keep the whole thermo map
-spec split1(split_preds(), state0()) -> {state_a(), state_b()}.
split1({_Pred1, _Pred2}, {LastPres, MaxSpike, ThermoMap}) ->
    {ThermoMap, {LastPres, MaxSpike}}.

-spec merge1(state_a(), state_b()) -> state0().
merge1(ThermoMap, {LastPres, MaxSpike}) ->
    {LastPres, MaxSpike, ThermoMap}.

%%
%% Split from thermo state to thermo state
%%

%% This split contains two simple predicates because it is only
%% allowed to keep the whole thermo map
-spec split_a(split_preds(), state_a()) -> {state_a(), state_a()}.
split_a({Pred1, Pred2}, ThermoMap) ->
    {maps:filter(fun(K,_) -> Pred1(K) end, ThermoMap),
     maps:filter(fun(K,_) -> Pred2(K) end, ThermoMap)}.

-spec merge_a(state_a(), state_a()) -> state_a().
merge_a(ThermoMap1, ThermoMap2) ->
    util:merge_with(
      fun(_K, V1, V2) ->
	      %% There shouldn't be a common key between those
	      erlang:halt(1)
      end, ThermoMap1, ThermoMap2).

-spec dependencies() -> #{tags0() := [tags0()]}.
dependencies() ->
    #{{a,1} => [minute],
      {a,2} => [minute],
      b => [minute, b],
      minute => [{a,1}, {a,2}, b]
     }.

-spec init_state() -> state0().
init_state() ->
    {0, 0, #{{a,1} => 0, {a,2} => 0}}.

%% The predicates
%% isA1({{a,1}, _, _}) -> true;
%% isA1(_) -> false.

%% isA2({{a,2}, _, _}) -> true;
%% isA2(_) -> false.

%% isB({b, _, _}) -> true;
%% isB(_) -> false.

%% isA(Msg) -> isA1(Msg) orelse isA2(Msg).

%% true_pred(_) -> true.



%% Some input examples

minute_markers_input(Node) ->
    Input = [{{minute, T * 60}, Node, T * 60} || T <- lists:seq(1, 10)],
    Msgs = producer:interleave_heartbeats(Input, {{minute, Node}, 60}, 650),
    producer:list_generator(Msgs).

input_with_heartbeats(Node, Tag, Fun) ->
    Msgs = producer:interleave_heartbeats(Fun(Node), {{Tag,Node}, 10}, 650),
    producer:list_generator(Msgs).

%% a1_input_with_heartbeats(Node) ->
%%     Msgs = producer:interleave_heartbeats(a1_input(Node), {{{a,1},Node}, 10}, 650),
%%     producer:list_generator(Msgs).

%% a2_input_with_heartbeats(Node) ->
%%     Msgs = producer:interleave_heartbeats(a2_input(Node), {{{a,2}, Node}, 10}, 650),
%%     producer:list_generator(Msgs).

%% b_input_with_heartbeats(Node) ->
%%     Msgs = producer:interleave_heartbeats(b_input(Node), {{b,Node}, 10}, 650),
%%     producer:list_generator(Msgs).

a1_input(Node) ->
    Inputs =
	[{20, 30},
	 {40, 20},
	 {60, 47},

	 {80, 54},
	 {100, 40},
	 {120, 33},

	 {140, 20},
	 {160, 30},
	 {180, 60},

	 {200, 70},
	 {220, 49},
	 {240, 20},

	 {260, 31},
	 {280, 12},
	 {300, 21}],
    [{{{a,1}, Tip}, Node, Ts} || {Ts, Tip} <- Inputs].

a2_input(Node) ->
    Inputs =
	[{20, 25},
	 {40, 26},
	 {60, 10},

	 {80, 40},
	 {100, 56},
	 {120, 20},

	 {140, 29},
	 {160, 21},
	 {180, 18},

	 {200, 80},
	 {220, 45},
	 {240, 60},

	 {260, 18},
	 {280, 12},
	 {300, 21}],
    [{{{a,2}, Tip}, Node, Ts} || {Ts, Tip} <- Inputs].

b_input(Node) ->
    Inputs =
	[{20, 10},
	 {40, 120},
	 {60, 50},

	 {80, 40},
	 {100, 56},
	 {120, 20},

	 {140, 29},
	 {160, 21},
	 {180, 18},

	 {200, 160},
	 {220, 280},
	 {240, 60},

	 {260, 18},
	 {280, 12},
	 {300, 21}],
    [{{b, Tip}, Node, Ts} || {Ts, Tip} <- Inputs].


%% -------- TESTS -------- %%

output() ->
    [{"Alert!", 240, "Pressure Spike:", 142, "MinMaxThermo:", 70}].

distributed_test_() ->
    Rounds = lists:seq(1,100),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mf({?MODULE, distributed_conf}, output()))
      end} || _ <- Rounds].

sequential_test_() ->
    Rounds = lists:seq(1,100),
    [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mf({?MODULE, sequential_conf}, output()))
      end} || _ <- Rounds].
