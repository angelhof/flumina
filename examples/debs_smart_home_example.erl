-module(debs_smart_home_example).

-export([main/0,
	 sequential/0,
	 distributed/0,
	 sequential_conf/1,
	 distributed_conf/1
	]).

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
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, distributed_conf, [SinkName]),
    util:sink().

distributed_conf(SinkPid) ->
    %% Architecture
    %% Rates = [{node(), minute, 10},
    %% 	     {node(), {a,1}, 1000},
    %% 	     {node(), {a,2}, 1000},
    %% 	     {node(), b, 1000}],
    %% Topology =
    %% 	conf_gen:make_topology(Rates, SinkPid),

    %% %% Computation
    %% Tags = [minute, {a,1}, {a,2}, b],
    %% StateTypesMap = 
    %% 	#{'state0' => {sets:from_list(Tags), fun update0/3},
    %% 	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update_a/3},
    %% 	  'state_b' => {sets:from_list([b]), fun update_b/3}},
    %% SplitsMerges = [{{'state0', 'state_a', 'state_b'}, {fun split1/2, fun merge1/2}},
    %% 		    {{'state_a', 'state_a', 'state_a'}, {fun split_a/2, fun merge_a/2}}],
    %% Dependencies = dependencies(),
    %% InitState = {'state0', init_state()},
    %% Specification = 
    %% 	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    %% ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_greedy}]),

    %% %% Set up where will the input arrive
    %% create_producers(fun minute_markers_input/0, minute, ConfTree, Topology),

    SinkPid ! finished.

sequential() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sequential_conf, [SinkName]),
    %% The initial and final values could also be gotten in an 
    %% automatic way.
    HouseGen = make_house_generator(0, 100, 1377986401000, 1377986405000),
    io:format("Messages:~n~p~n", [producer:generator_to_list(HouseGen)]),
    util:sink().

sequential_conf(SinkPid) ->
    %% Architecture
    %% Rates = [{node(), minute, 10},
    %% 	     {node(), {a,1}, 1000},
    %% 	     {node(), {a,2}, 1000},
    %% 	     {node(), b, 1000}],
    %% Topology =
    %% 	conf_gen:make_topology(Rates, SinkPid),

    %% %% Computation
    %% Tags = [minute, {a,1}, {a,2}, b],
    %% StateTypesMap = 
    %% 	#{'state0' => {sets:from_list(Tags), fun update0/3}},
    %% SplitsMerges = [],
    %% Dependencies = dependencies(),
    %% InitState = {'state0', init_state()},
    %% Specification = 
    %% 	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    %% ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_sequential}]),

    %% %% Set up where will the input arrive
    %% create_producers(fun minute_markers_input/0, minute, ConfTree, Topology),


    SinkPid ! finished.


-type house_tag() :: {'house', integer()}.
-type payload() :: {integer(), float(), integer(), integer(), integer()}.
-type event() :: message(house_tag(), payload()).


%% Makes a generator for that house, and adds heartbeats
-spec make_house_generator(integer(), integer(), integer(), integer()) -> msg_generator().
make_house_generator(HouseId, Period, From, Until) ->
    Filename = io_lib:format("sample_debs_house_~w", [HouseId]),
    %% producer:file_generator(Filename, fun parse_house_csv_line/1).
    producer:file_generator_with_heartbeats(Filename, fun parse_house_csv_line/1, 
    					    {{house,HouseId}, Period}, From, Until).
    
%% NOTE: This adjusts the timestamps to be ms instead of seconds
-spec parse_house_csv_line(string()) -> event().
parse_house_csv_line(Line) ->
    TrimmedLine = string:trim(Line),
    [SId, STs, SValue, SProp, SPlug, SHousehold, SHouse] = 
	string:split(TrimmedLine, ",", all),
    Id = list_to_integer(SId),
    Ts = 1000 * list_to_integer(STs),
    Value = util:list_to_number(SValue),
    Prop = list_to_integer(SProp),
    Plug = list_to_integer(SPlug),
    Household = list_to_integer(SHousehold),
    House = list_to_integer(SHouse),
    {{house, House}, Ts, {Id, Value, Prop, Plug, Household}}.
    
