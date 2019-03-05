-module(smart_home_example).

-export([main/0,
	 sequential/0,
	 distributed/0,
	 sequential_conf/1,
	 distributed_conf/1,
	 source/2]).

-include_lib("eunit/include/eunit.hrl").
-include("type_definitions.hrl").

main() ->
    sequential().

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

distributed() ->
    ExecPid = spawn_link(?MODULE, distributed_conf, [self()]),
    util:sink().

distributed_conf(SinkPid) ->
    %% The functions of each node
    Funs = {fun update0/3, fun split1/2, fun merge1/2},
    FunsA = {fun update_a/3, fun split_a/2, fun merge_a/2},
    FunsB = {fun update_b/3, fun util:crash/2, fun util:crash/2},

    %% The initial states of each node
    Init = init_state(),
    {InitA, InitB} = split1({fun true_pred/1, fun true_pred/1}, Init),
    {InitA1, InitA2} = split_a({fun isA1/1, fun isA2/1}, InitA),

    %% Initializing the nodes
    NodeA1 = {InitA1, fun isA1/1, FunsA, []},
    NodeA2 = {InitA2, fun isA2/1, FunsA, []},
    NodeA = {InitA, fun isA/1, FunsA, [NodeA1, NodeA2]},
    NodeB = {InitB, fun isB/1, FunsB, []},
    Node  = {Init, fun true_pred/1, Funs, [NodeA, NodeB]},
    PidTree = configuration:create(Node, dependencies(), SinkPid),
    {{_NP0, MP0}, 
     [{{_NPA, MPA}, 
       [{{_NPA1, MPA1}, []}, 
	{{_NPA2, MPA2}, []}]}, 
      {{_NPB, MPB}, []}]} = PidTree,

    %% Set up where will the input arrive
    create_producers(fun minute_markers_input/0, [MPA1, MPA2, MPB, MP0]),

    SinkPid ! finished.

sequential() ->
    ExecPid = spawn_link(?MODULE, sequential_conf, [self()]),
    util:sink().

sequential_conf(SinkPid) ->
    %% Configuration Tree
    Funs = {fun update0/3, fun util:crash/2, fun util:crash/2},
    Ids = init_state(),
    Node  = {Ids, fun true_pred/1, Funs, []},
    PidTree = configuration:create(Node, dependencies(), SinkPid),
    {{_HeadNodePid, HeadMPid}, _} = PidTree,

    %% Set up where will the input arrive
    create_producers(fun minute_markers_input/0, [HeadMPid, HeadMPid, HeadMPid, HeadMPid]),

    SinkPid ! finished.

create_producers(MarkerFun, [Pid1, Pid2, Pid3, Pid4]) ->
    Input1 = a1_input_with_heartbeats(),
    Producer1 = spawn_link(?MODULE, source, [Input1, Pid1]),

    Input2 = a2_input_with_heartbeats(),
    Producer2 = spawn_link(?MODULE, source, [Input2, Pid2]),

    Input3 = b_input_with_heartbeats(),
    Producer3 = spawn_link(?MODULE, source, [Input3, Pid3]),

    Input4 = MarkerFun(),
    Producer4 = spawn_link(?MODULE, source, [Input4, Pid4]).

%%
%% The specification of the computation
%%


%% This computation the total distance that each driver has moved every hour.
%% It finds the distance between each two consecutive points of each taxi driver
%% and then adds them all for each hour

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
update0({minute, Ts, marker}, {LastPres, MaxSpike, ThermoMap}, SendTo) ->
    MinMaxThermo = lists:min(maps:values(ThermoMap)),
    case MaxSpike >= 100 andalso MinMaxThermo >= 50 of
	true ->
	    SendTo ! {"Alert!", Ts, "Pressure Spike:", MaxSpike, "MinMaxThermo:", MinMaxThermo};
	false ->
	    ok
    end,
    NewThermoMap = maps:map(fun(_,_MaxT) -> 0 end, ThermoMap),
    {LastPres, 0, NewThermoMap};
update0({b, _Ts, Pressure}, {LastPres, MaxSpike, ThermoMap}, _SendTo) ->
    NewMaxSpike = max(Pressure - LastPres, MaxSpike),
    {Pressure, NewMaxSpike, ThermoMap};
update0({{a, Id}, _Ts, Thermo}, {LastPres, MaxSpike, ThermoMap}, _SendTo) ->
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
update_b({b, _Ts, Pressure}, {LastPres, MaxSpike}, _SendTo) ->
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
update_a({{a, Id}, _Ts, Thermo}, ThermoMap, _SendTo) ->
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
    {maps:filter(fun(K,_) -> Pred1({K,u,u}) end, ThermoMap),
     maps:filter(fun(K,_) -> Pred2({K,u,u}) end, ThermoMap)}.

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
isA1({{a,1}, _, _}) -> true;
isA1(_) -> false.

isA2({{a,2}, _, _}) -> true;
isA2(_) -> false.    

isB({b, _, _}) -> true;
isB(_) -> false.

isA(Msg) -> isA1(Msg) orelse isA2(Msg).

true_pred(_) -> true.


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



%% Some input examples

minute_markers_input() ->
    Input = [{minute, T * 60, marker} || T <- lists:seq(1, 10)],
    producer:interleave_heartbeats(Input, #{minute => 60}, 650).

a1_input_with_heartbeats() ->
    producer:interleave_heartbeats(a1_input(), #{{a,1} => 10}, 650).

a2_input_with_heartbeats() ->
    producer:interleave_heartbeats(a2_input(), #{{a,2} => 10}, 650).

b_input_with_heartbeats() ->
    producer:interleave_heartbeats(b_input(), #{b => 10}, 650).

a1_input() ->
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
    [{{a,1}, Ts, Tip} || {Ts, Tip} <- Inputs].

a2_input() ->
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
    [{{a,2}, Ts, Tip} || {Ts, Tip} <- Inputs].

b_input() ->
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
    [{b, Ts, Tip} || {Ts, Tip} <- Inputs].


%% -------- TESTS -------- %%

output() ->
    [{"Alert!", 240, "Pressure Spike:", 142, "MinMaxThermo:", 70}].

distributed_test_() ->
    Rounds = lists:seq(1,100),
    [?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf}, output()))
     || _ <- Rounds].

sequential_test_() ->
    Rounds = lists:seq(1,100),
    [?_assertEqual(ok, testing:test_mfa({?MODULE, sequential_conf}, output()))
     || _ <- Rounds].

