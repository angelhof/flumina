-module(debs_2014_query1).

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

%% =====================================
%% ===== Computation Specification =====
%% =====================================

%% ========== Input Events ==========

% Whether value_id is a work measurement or load measurement
-type measurement_type() :: 'work' | 'load'.
% IDs: house implies household implies plug implies measurement. Each measurement ID is unique.
-type house_id() :: integer().
-type household_id() :: integer().
-type plug_id() :: integer().
-type measurement_id() :: integer().
% Timestamp and value of the actual measurement.
-type measurement_timestamp() :: integer().
-type measurement_value() :: float().
-type time_overall() :: integer(). % Time of the current slice of input msgs
-type measurement_tag() :: {house, house_id(), measurement_type()}.
-type measurement_payload() :: {household_id(), plug_id(), measurement_id(), measurement_timestamp(), measurement_value()}.
-type end_timeslice() :: 'end-timeslice'.

% Finally, the input event tags and input events:
-type event_tag() :: measurement_tag() | end_timeslice().
-type event() :: message(measurement_tag(), measurement_payload())
		| message(end_timeslice(), time_overall()).

%% ========== Tag Dependencies ==========

-define(HOUSE_ID_MAX,40).

% Dependencies for query 1:
% - (house, _, work) tags are dependent on nothing.
% - end_timeslice are dependent on themselves and all remaining.
% - (house, _, load) tags are dependent on themselves, but not on each other.
dependencies(NumHouseIDs) ->
	EndTimesliceDeps =
			[{end_timeslice, {house, Id, load}}
				|| Id <- lists:seq(0,NumHouseIDs-1)]
			++ [{{house, Id, load}, end_timeslice}
				|| Id <- lists:seq(0,NumHouseIDs-1)]
			++ [{end_timeslice, end_timeslice}],
	HouseSelfDeps =
			[{{house, Id, load}, {house, Id, load}}
				|| Id <- lists:seq(0,NumHouseIDs-1)],
	maps:from_list(EndTimesliceDeps ++ HouseSelfDeps).


%% ========== State Type ==========

-type totals() :: {integer(), integer()}. % Total, count
-type time_of_day() :: integer(). % Will be computed as a modulo of the time_overall
-type total_by_time_of_day() :: #{time_of_day() := totals()}.
% Summary for an individual plug, household, house, etc.: (1) total in the previous slice, and (2) total in each time of day
-type past_load_summary() :: {totals(), total_by_time_of_day()}.

%% The state will be several nested maps of load summaries.
-type plugid_load_summary() :: past_load_summary().
-type household_load_summary() :: {past_load_summary(), #{plug_id() := plugid_load_summary()}}.
-type house_load_summary() :: {past_load_summary(), #{household_id() := household_load_summary()}}.
-type global_load_summary() :: {past_load_summary(), #{house_id() := global_load_summary()}}.
%% Complete state includes load summaries overall, for each house, for each household, and for each plug. We also include the time overall and the time of day for each plug.
-type state() :: {time_overall(), time_of_day(), global_load_summary()}.

%% ========== Sequential Specification ==========

-define(INITIAL_TIME,0). % We hope the initial time will be overridden

-spec init_state() -> state(). %% TODO: Why does this fail?
init_state() ->
	{?INITIAL_TIME, ?INITIAL_TIME, {{{0,0},maps:new()}, maps:new()}}.

%% To update the state we need two helper functions, to update totals, and to update a complete load summary. We also need a function to reset the load summary.
-spec update_totals(totals(), integer()) -> totals().
update_totals({Sum, Count}, NewVal) ->
	{Sum + NewVal, Count + 1}.

-spec update_load_summary(past_load_summary(), integer(), time_of_day()) -> past_load_summary().
update_load_summary({Totals, TotalsByTimeOfDay}, NewVal, TimeOfDay) ->
	NewTotals = update_totals(Totals, NewVal),
	NewTotalsByTimeOfDay = maps:update_with(
		TimeOfDay,
		fun (T) ->
			update_totals(T, NewVal)
		end,
		update_totals({0,0}, NewVal),
		TotalsByTimeOfDay
	),
	{NewTotals, NewTotalsByTimeOfDay}.

%% I lied, we need more helper functions. One for each level of nesting.

-spec update_household_load_summary(household_load_summary(), integer(), time_of_day(), integer()) -> household_load_summary().
update_household_load_summary({Totals, TotalsByPlug}, NewVal, TimeOfDay, PlugID) ->
	NewTotals = update_load_summary(Totals, NewVal, TimeOfDay),
	NewTotalsByPlug = maps:update_with(
		PlugID,
		fun (T) ->
			update_load_summary(T, NewVal, TimeOfDay)
		end,
		update_load_summary({{0,0}, maps:new()}, NewVal, TimeOfDay),
		TotalsByPlug
	),
	{NewTotals, NewTotalsByPlug}.

-spec update_house_load_summary(house_load_summary(), integer(), time_of_day(), integer(), integer()) -> house_load_summary().
update_house_load_summary({Totals, TotalsByHousehold}, NewVal, TimeOfDay, HouseholdID, PlugID) ->
	NewTotals = update_load_summary(Totals, NewVal, TimeOfDay),
	NewTotalsByHousehold = maps:update_with(
		HouseholdID,
		fun (T) ->
			update_household_load_summary(T, NewVal, TimeOfDay, PlugID)
		end,
		update_household_load_summary({{{0,0},maps:new()}, maps:new()}, NewVal, TimeOfDay, PlugID),
		TotalsByHousehold
	),
	{NewTotals, NewTotalsByHousehold}.

-spec update_global_load_summary(global_load_summary(), integer(), time_of_day(), integer(), integer(), integer()) -> global_load_summary().
update_global_load_summary({Totals, TotalsByHouse}, NewVal, TimeOfDay, HouseID, HouseholdID, PlugID) ->
	NewTotals = update_load_summary(Totals, NewVal, TimeOfDay),
	NewTotalsByHouse = maps:update_with(
		HouseID,
		fun (T) ->
			update_house_load_summary(T, NewVal, TimeOfDay, HouseholdID, PlugID)
		end,
		update_house_load_summary({{{0,0},maps:new()}, maps:new()}, NewVal, TimeOfDay, HouseholdID, PlugID),
		TotalsByHouse
	),
	{NewTotals, NewTotalsByHouse}.

%% Resetting the totals (do NOT reset totals by time of day)

-spec reset_household_load_summary(household_load_summary()) -> household_load_summary().
reset_household_load_summary({_Totals, TotalsByPlug}) ->
	{{{0,0},maps:new()}, TotalsByPlug}.
-spec reset_house_load_summary(house_load_summary()) -> house_load_summary().
reset_house_load_summary({_Totals, TotalsByHousehold}) ->
	{{{0,0},maps:new()},maps:map(
		fun (_HouseholdID, HouseholdSummary) ->
			reset_household_load_summary(HouseholdSummary)
		end,
		TotalsByHousehold
	)}.
-spec reset_global_load_summary(global_load_summary()) -> global_load_summary().
reset_global_load_summary({_Totals, TotalsByHouse}) ->
	{{{0,0},maps:new()},maps:map(
		fun (_HouseID, HouseSummary) ->
			reset_house_load_summary(HouseSummary)
		end,
		TotalsByHouse
	)}.

%% Also we need to write the code which does the power prediction after each timeslice

%% TODO: Write this.
-spec output_predictions(global_load_summary(),time_of_day(),pid()) -> ok.
output_predictions(_GlobalTotals,_NewTimeOfDay,_SinkPID) ->
	ok.

%% Convert a time to a time of day

-define(NANOSECONDS_IN_A_DAY,   86400000000000).
-define(NANOSECONDS_IN_AN_HOUR,  3600000000000).

-spec get_time_of_day(time_overall()) -> time_of_day().
get_time_of_day(TimeOverall) ->
	NanosecondOfDay = util:mod(TimeOverall,?NANOSECONDS_IN_A_DAY),
	HourOfDay = util:intdiv(NanosecondOfDay,?NANOSECONDS_IN_AN_HOUR),
	HourOfDay.

%% Finally, this is the function to actually update the state.

-spec update(event(), state(), pid()) -> state().
update({{house, _HouseId, work}, _Payload}, State, _SinkPID) ->
	State;
update(
		{
			{house, HouseID, load},
			{HouseholdID, PlugID, _MeasID, _MeasTS, MeasVal}
		},
		{TimeOverall, TimeOfDay, GlobalSummary},
		_SinkPID
	) ->
	{TimeOverall, TimeOfDay, update_global_load_summary(
		GlobalSummary,
		MeasVal,
		TimeOfDay,
		HouseID,
		HouseholdID,
		PlugID
	)};
update({'end_timeslice', TimeValue}, {TimeOverall, TimeOfDay, GlobalTotals}, SinkPID) ->
	NewTimeOfDay = get_time_of_day(TimeValue),
	output_predictions(GlobalTotals,NewTimeOfDay,SinkPID),
	NewGlobalTotals = reset_global_load_summary(GlobalTotals),
	{TimeValue, NewTimeOfDay, NewGlobalTotals}.


%% ========== Parallelization Primitives ==========


% -spec init_state() -> state().
% init_state() -> {0,0}.
% 
% -spec update(events(), state(), pid()) -> state().
% update({'#', 'none'}, {_Total, Max}, SinkPid) ->
%     SinkPid ! {Max},
%     {0,0};
% update({{Tag,_ImpID}, 'none'}, {Total, Max}, _SinkPid) ->
%     case Tag of
%         'i' ->
%             NewTotal = Total + 1;
%         'd' ->
%             NewTotal = Total - 1
%     end,
%     NewMax = max(NewTotal,Max),
%     {NewTotal,NewMax}.
% 
% %% Parallelization Primitives
% -spec fork(split_preds(), state()) -> {state(), state()}.
% fork(_, {Total, Max}) ->
%     {{Total, Max}, {0, 0}}.
% 
% -spec join(state(), state()) -> state().
% join({X1, Y1}, {X2, _Y2}) -> % Y2 not used
%     X = X1 + X2,
%     {X, max(Y1,X)}.

%% ============================
%% ===== Experiment Setup =====
%% ============================

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
    util:sink().

sequential_conf(SinkPid) ->
    %% TODO: Make this parameterizable
    Tags = [
				end_timeslice,
				{house,0,work},
				{house,1,work},
				{house,0,load},
				{house,1,load}
			],

    %% Architecture
    Rates = [
				{node(), end_timeslice, 1},
				{node(), {house,0,work}, 1000},
				{node(), {house,1,work}, 1000},
				{node(), {house,0,load}, 1000},
				{node(), {house,1,load}, 1000}
			],
    Topology =
    	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    StateTypesMap = 
    	#{'state' => {sets:from_list(Tags), fun update/3}},
    SplitsMerges = [],
    Dependencies = dependencies(2),
    InitState = {'state', init_state()},
    Specification = 
    	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_sequential}]),

    %% Set up where will the input arrive
	BeginSimulationTime = 1377986401000,
	EndSimulationTime = 1377986405000,
    HouseGen = make_house_generator(0, node(), 100, BeginSimulationTime, EndSimulationTime),

	%% TODO: Konstantinos

    SinkPid ! finished.


-type impl_event() :: impl_message(measurement_tag(), measurement_payload()).

%% Makes a generator for that house, and adds heartbeats
-spec make_house_generator(integer(), node(), integer(), timestamp(), timestamp()) -> msg_generator().
make_house_generator(HouseId, NodeName, Period, From, Until) ->
    Filename = io_lib:format("sample_debs_house_~w", [HouseId]),
    %% producer:file_generator(Filename, fun parse_house_csv_line/1).
    producer:file_generator_with_heartbeats(Filename, fun parse_house_csv_line/1, 
    					    {{{house,HouseId}, NodeName}, Period}, From, Until).
    
%% NOTE: This adjusts the timestamps to be ms instead of seconds
-spec parse_house_csv_line(string()) -> impl_event(). %% TODO: Why does this fail?
parse_house_csv_line(Line) ->
    TrimmedLine = string:trim(Line),
    [SId, STs, SValue, SProp, SPlug, SHousehold, SHouse] = 
	string:split(TrimmedLine, ",", all),
    Id = list_to_integer(SId),
    Ts = 1000 * list_to_integer(STs),
    Value = util:list_to_number(SValue),
    Prop = 
		case SProp of % Measurement type
			"0" -> work; % See DEBS 2014 specification
			"1" -> load % See DEBS 2014 specification
		end,
    Plug = list_to_integer(SPlug),
    Household = list_to_integer(SHousehold),
    House = list_to_integer(SHouse),
    %% WARNING: This should return the producer node
    Node = node(),
    {{{house, House, Prop}, {Household, Plug, Id, Ts, Value}}, Node, Ts}.
    
