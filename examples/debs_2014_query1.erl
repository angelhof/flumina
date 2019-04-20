-module(debs_2014_query1).

-export([main/0,
	 sequential/0,
	 distributed/0,
	 sequential_conf/1,
	 distributed_conf/1,
	 
	 make_house_generator/6,
	 make_end_timeslice_stream/5
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
-type end_timeslice_message() :: message(end_timeslice(), time_overall()).
-type event() :: message(measurement_tag(), measurement_payload())
		| end_timeslice_message().

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

%% The state will be several maps of load summaries.
-type load_summary_map(KeyType) :: #{KeyType := past_load_summary()}.
-type house_load_summary() ::
	load_summary_map(house_id()).
-type household_load_summary() ::
	load_summary_map({house_id(),household_id()}).
-type plug_load_summary() ::
	load_summary_map({house_id(),household_id(),plug_id()}).
-type all_load_summaries() ::
	{
		past_load_summary(),
		house_load_summary(),
		household_load_summary(),
		plug_load_summary()
	}.
%% Complete state includes load summaries overall, for each house, for each household, and for each plug. We also include the time overall and the time of day for each plug.
-type state() :: {
					time_overall(),
					time_of_day(),
					all_load_summaries()
				}.

%% ========== Sequential Specification ==========

-define(INITIAL_TIME,0). % We hope the initial time will be overridden

-spec init_state(time_of_day()) -> state().
init_state(InitialTime) ->
	{InitialTime, InitialTime, {
		new_load_summary(),
		maps:new(),
		maps:new(),
		maps:new()
	}}.

%% To update the state we need some helper functions: to update totals, and to update a complete load summary. Also to update a map of load summaries.
%% For totals and load_summary values, we write an update function and a new function. Also, a reset function for load summaries (which resets the first coordinate only).
-spec update_totals(totals(), integer()) -> totals().
update_totals({Sum, Count}, NewVal) ->
	{Sum + NewVal, Count + 1}.

-spec new_totals() -> totals().
new_totals() ->
	{0, 0}.

-spec update_load_summary(past_load_summary(), integer(), time_of_day()) -> past_load_summary().
update_load_summary({Totals, TotalsByTimeOfDay}, NewVal, TimeOfDay) ->
	NewTotals = update_totals(Totals, NewVal),
	NewTotalsByTimeOfDay = maps:update_with(
		TimeOfDay,
		fun (T) ->
			update_totals(T, NewVal)
		end,
		update_totals(new_totals(), NewVal),
		TotalsByTimeOfDay
	),
	{NewTotals, NewTotalsByTimeOfDay}.

-spec new_load_summary() -> past_load_summary().
new_load_summary() ->
	{new_totals(), maps:new()}.

-spec reset_load_summary(past_load_summary()) -> past_load_summary().
reset_load_summary({Totals, TotalsByTimeOfDay}) ->
	{new_totals(), TotalsByTimeOfDay}.

-spec update_load_summary_map(load_summary_map(KeyType), KeyType, integer(), time_of_day()) -> load_summary_map(KeyType).
update_load_summary_map(LoadSummaryMap, Key, NewVal, TimeOfDay) ->
	maps:update_with(
		Key,
		fun (LoadSummary) ->
			update_load_summary(LoadSummary, NewVal, TimeOfDay)
		end,
		update_load_summary(new_load_summary(), NewVal, TimeOfDay),
		LoadSummaryMap
	).

-spec reset_load_summary_map(load_summary_map(KeyType)) -> load_summary_map(KeyType).
reset_load_summary_map(LoadSummaryMap) ->
	maps:map(
		fun (_Key, LoadSummary) ->
			reset_load_summary(LoadSummary)
		end,
		LoadSummaryMap
	).

%% Also we need to write the code which does the power prediction after each timeslice

%% TODO: Write this.
-spec output_predictions(all_load_summaries(),time_of_day(),pid()) -> ok.
output_predictions(_AllLoadSummaries,_NewTimeOfDay,_SinkPID) ->
	ok.

%% Convert a time to a time of day

-define(NANOSECONDS_IN_A_DAY,   86400000000000).
-define(NANOSECONDS_IN_AN_HOUR,  3600000000000).

-spec get_time_of_day(time_overall()) -> time_of_day().
get_time_of_day(TimeOverall) ->
	NanosecondOfDay = util:intmod(TimeOverall,?NANOSECONDS_IN_A_DAY),
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
		{TimeOverall, TimeOfDay, {
			LS_Global, % LS = Load Summary
			LS_ByHouse,
			LS_ByHousehold,
			LS_ByPlug
		}},
		_SinkPID
	) ->
	{TimeOverall, TimeOfDay, {
		update_load_summary(LS_Global, MeasVal, TimeOfDay),
		update_load_summary_map(
			LS_ByHouse,
			HouseID,
			MeasVal,
			TimeOfDay
		),
		update_load_summary_map(
			LS_ByHousehold,
			{HouseID, HouseholdID},
			MeasVal,
			TimeOfDay
		),
		update_load_summary_map(
			LS_ByPlug,
			{HouseID, HouseholdID, PlugID},
			MeasVal,
			TimeOfDay
		)
	}};
update(
		{'end_timeslice', TimeValue},
		{TimeOverall, TimeOfDay, {
			LS_Global,
			LS_ByHouse,
			LS_ByHousehold,
			LS_ByPlug
		}},
		_SinkPID
	) ->
	{
		TimeValue,
		get_time_of_day(TimeValue),
		{
			reset_load_summary(LS_Global),
			reset_load_summary_map(LS_ByHouse),
			reset_load_summary_map(LS_ByHousehold),
			reset_load_summary_map(LS_ByPlug)
		}
	}.

%% ========== Parallelization Primitives ==========

%% To fork the state: split the maps by key. Fork the global load summary by preserving the sum (fork (x,y) -> (x,y), (0,0)).


% -spec fork(split_preds(), state()) -> {state(), state()}.
% fork(SplitPreds, {TimeOverall, TimeOfDay, AllLoadSummaries}) ->
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

	%% Some Garbage
	BeginSimulationTime = 1377986401000,
	EndSimulationTime = 1377986405000,

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
    InitState = {'state', init_state(BeginSimulationTime)},
    Specification = 
    	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, optimizer_sequential}]),

    %% Prepare the producers input
    Houses = [{0, node()}, {1, node()}],
    ProducerInit = 
	make_producer_init(Houses, node(), 100, 5000, 1000, 
			   BeginSimulationTime, EndSimulationTime, 10),

    %% Log the input times of b messages
    producer:make_producers(ProducerInit, ConfTree, Topology, constant),
	%% TODO: Konstantinos

    SinkPid ! finished.


-type impl_event() :: impl_message(measurement_tag(), measurement_payload()).

-spec make_producer_init([{house_id(), node()}], node(), integer(), 
			 integer(), integer(), integer(), integer(), integer()) 
			-> [{msg_generator_init(), {measurement_tag(), node()}, integer()}].	 
make_producer_init(Houses, EndTimesliceNode, MeasurementHeartbeatPeriod, EndTimeslicePeriod, 
		   EndTimesliceHeartbeatPeriod, BeginTime, EndTime, RateMult) ->
    HousesProducerInit =
	lists:flatmap(
	  fun(House) ->
		  make_house_producer_init(House, MeasurementHeartbeatPeriod, 
					   BeginTime, EndTime, RateMult)
	  end, Houses),
    TimesliceStream = 
	{fun ?MODULE:make_end_timeslice_stream/5, 
	 [EndTimesliceNode, BeginTime, EndTime, 
	  EndTimeslicePeriod, EndTimesliceHeartbeatPeriod]},
    TimesliceInit = 
	{TimesliceStream, {end_timeslice, EndTimesliceNode}, RateMult},
    [TimesliceInit|HousesProducerInit].

-spec make_house_producer_init({house_id(), node()}, integer(), integer(), integer(), integer()) 
			      -> [{msg_generator_init(), {measurement_tag(), node()}, integer()}].	 
make_house_producer_init({HouseId, Node}, MeasurementHeartbeatPeriod, BeginTime, EndTime, RateMult) ->
    WorkGenInit = 
	{fun ?MODULE:make_house_generator/6, 
	 [HouseId, work, Node, 100, BeginTime, EndTime]},
    LoadGenInit = 
	{fun ?MODULE:make_house_generator/6, 
	 [HouseId, work, Node, 100, BeginTime, EndTime]},
    [{WorkGenInit, {{house, HouseId, work}, Node}, RateMult},
     {LoadGenInit, {{house, HouseId, load}, Node}, RateMult}].



-spec make_end_timeslice_stream(node(), integer(), integer(), integer(), integer()) -> msg_generator().
make_end_timeslice_stream(Node, From, To, Step, HeartbeatPeriod) ->
    Timeslices 
	= [{{end_timeslice, T}, Node, T} 
	   || T <- lists:seq(From, To, Step)]
	++ [{heartbeat, {{end_timeslice, Node}, To + 1}}],
    TimeslicesWithHeartbeats = 
	producer:interleave_heartbeats(Timeslices, {{end_timeslice, Node}, HeartbeatPeriod}, To + 2),
    producer:list_generator(TimeslicesWithHeartbeats).


%% Makes a generator for that house, and adds heartbeats
-spec make_house_generator(integer(), atom(), node(), integer(), timestamp(), timestamp()) -> msg_generator().
make_house_generator(HouseId, WorkLoad, NodeName, Period, From, Until) ->
    Filename = io_lib:format("sample_debs_house_~w_~s", [HouseId, atom_to_list(WorkLoad)]),	    
    %% producer:file_generator(Filename, fun parse_house_csv_line/1).
    producer:file_generator_with_heartbeats(Filename, fun parse_house_csv_line/1, 
    					    {{{house,HouseId, WorkLoad}, NodeName}, Period}, From, Until).
    
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
    
