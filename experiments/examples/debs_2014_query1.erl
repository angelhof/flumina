-module(debs_2014_query1).

-export([main/0,
         experiment_sequential/0,
         experiment_greedy/0,
         setup_experiment/6,
         run_experiment/7,

         experiment_distributed/0,
         run_experiment_distributed/2,

         make_house_generator/6,
         make_end_timeslice_stream/5
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flumina/include/type_definitions.hrl").

main() ->
    experiment_sequential().

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
-type measurement_tag() :: {{house, measurement_type()}, house_id()}.
-type measurement_payload() :: {household_id(), plug_id(), measurement_id(), measurement_timestamp(), measurement_value()}.
-type end_timeslice() :: 'end-timeslice'.

% Finally, the input event tags and input events:
% -type event_tag() :: measurement_tag() | end_timeslice().
-type measurement_message() :: message(measurement_tag(), measurement_payload()).
-type end_timeslice_message() :: message(end_timeslice(), time_overall()).
-type event() :: measurement_message() | end_timeslice_message().

%% ========== Tag Dependencies ==========

-define(HOUSE_ID_MAX,40).

% Dependencies for query 1:
% - (house, _, work) tags are dependent on nothing.
% - end_timeslice are dependent on themselves and all remaining.
% - (house, _, load) tags are dependent on themselves, but not on each other.
dependencies(NumHouseIDs) ->
    DepsList =
        [{end_timeslice,
          [end_timeslice] ++
              [{{house, load}, Id}
               || Id <- lists:seq(0,NumHouseIDs-1)]}]
        ++ [{{{house, load}, Id}, [end_timeslice, {{house, load}, Id}]}
            || Id <- lists:seq(0,NumHouseIDs-1)],
    maps:from_list(DepsList).

%% ========== State Type ==========

-type totals() :: {float(), integer()}. % Total, count
-type time_of_day() :: integer(). % Will be computed as a modulo of the time_overall
-type total_by_time_of_day() :: #{time_of_day() := totals()}.
% Summary for an individual plug, household, house, etc.: (1) total in
% the previous slice, and (2) total in each time of day
-type past_load_summary() :: {totals(), total_by_time_of_day()}.

%% The state will be several maps of load summaries.
-type load_summary_map(KeyType) :: #{KeyType := past_load_summary()}.
-type house_load_summary() ::
        load_summary_map(house_id()).
-type household_load_summary() ::
        load_summary_map({house_id(),household_id()}).
-type plug_load_summary() ::
        load_summary_map({house_id(),household_id(),plug_id()}).
-type all_load_summaries() :: {past_load_summary(),
							   house_load_summary(),
							   household_load_summary(),
                               plug_load_summary()
                              }.

%% The state will also include WEIGHTS for a linear model for load prediction.
% These include 4 House weights and 8 plug weights.
-type weights() :: [float()].
-type all_weights() :: {weights(), weights()}.

%% Complete state includes load summaries overall, for each house, for
%% each household, and for each plug. We also include the time overall
%% and the time of day for each plug.
-type state() :: {time_overall(),
				  time_of_day(),
				  all_load_summaries(),
                  all_weights()
                 }.

%% ========== Sequential Specification ==========

-define(INITIAL_TIME,0). % We hope the initial time will be overridden

-spec init_state(time_overall()) -> state().
init_state(InitialTime) ->
    {InitialTime,
     get_time_of_day(InitialTime),
     {new_load_summary(),
      maps:new(),
      maps:new(),
      maps:new()
     },
     {lists:duplicate(4, 1 / 4), % 4 equal weights to predict house load
      lists:duplicate(8, 1 / 8)  % 8 equal weights to predict plug load
     }}.

%% To update the state we need some helper functions: to update
%% totals, and to update a complete load summary. Also to update a map
%% of load summaries.  For totals and load_summary values, we write an
%% update function and a new function. Also, a reset function for load
%% summaries (which resets the first coordinate only).  The functions
%% to add totals and load summaries won't be used until later (for
%% joining state).
-spec update_totals(totals(), integer()) -> totals().
update_totals({Sum, Count}, NewVal) ->
    {Sum + NewVal, Count + 1}.

-spec new_totals() -> totals().
new_totals() ->
    {0.0, 0}.

-spec add_totals(totals(), totals()) -> totals().
add_totals({Sum1, Count1}, {Sum2, Count2}) ->
    {Sum1 + Sum2, Count1 + Count2}.

-spec get_average(totals()) -> float().
get_average({_Sum,0}) ->
    0.0; % Default
get_average({Sum,Count}) ->
    Sum / Count.

-spec update_load_summary(past_load_summary(), integer(), time_of_day()) -> past_load_summary().
update_load_summary({Totals, TotalsByTimeOfDay}, NewVal, TimeOfDay) ->
    NewTotals = update_totals(Totals, NewVal),
    NewTotalsByTimeOfDay =
        maps:update_with(
          TimeOfDay,
          fun (T) ->
                  update_totals(T, NewVal)
          end,
          update_totals(new_totals(), NewVal),
          TotalsByTimeOfDay),
    {NewTotals, NewTotalsByTimeOfDay}.

-spec new_load_summary() -> past_load_summary().
new_load_summary() ->
    {new_totals(), maps:new()}.

-spec reset_load_summary(past_load_summary()) -> past_load_summary().
reset_load_summary({_Totals, TotalsByTimeOfDay}) ->
    {new_totals(), TotalsByTimeOfDay}.

-spec add_load_summaries(past_load_summary(), past_load_summary()) -> past_load_summary().
add_load_summaries({Totals1, Map1}, {Totals2, Map2}) ->
    NewTotals = add_totals(Totals1, Totals2),
    Keys = maps:keys(maps:merge(Map1, Map2)),
    NewMap = maps:from_list(lists:map(
        fun (Key) ->
            Val = add_totals(maps:get(Key, Map1, new_totals()),
                             maps:get(Key, Map2, new_totals())),
            {Key, Val}
        end,
        Keys
    )),
    {NewTotals, NewMap}.

-spec get_ls_averages(past_load_summary(), time_of_day()) -> [float()].
get_ls_averages({Totals, TotalsByTimeOfDay},TimeOfDay) ->
    Avg1 = get_average(Totals),
    Avg2 = get_average(maps:get(TimeOfDay,TotalsByTimeOfDay,new_totals())),
    [Avg1, Avg2].

-spec update_load_summary_map(load_summary_map(KeyType), KeyType, integer(), time_of_day()) -> load_summary_map(KeyType).
update_load_summary_map(LoadSummaryMap, Key, NewVal, TimeOfDay) ->
    maps:update_with(
      Key,
      fun (LoadSummary) ->
              update_load_summary(LoadSummary, NewVal, TimeOfDay)
      end,
      update_load_summary(new_load_summary(), NewVal, TimeOfDay),
      LoadSummaryMap).

-spec reset_load_summary_map(load_summary_map(KeyType)) -> load_summary_map(KeyType).
reset_load_summary_map(LoadSummaryMap) ->
    maps:map(
      fun (_Key, LoadSummary) ->
              reset_load_summary(LoadSummary)
      end, LoadSummaryMap).

%% Also we need to write the code which does the power prediction after each timeslice

-spec predict_plug_load(all_load_summaries(), weights(), time_of_day(), {house_id(), household_id(), plug_id()}) -> float().
predict_plug_load(AllLoadSummaries, Weights, NewTimeOfDay, IDs) ->
    {LS_Global, LS_ByHouse, LS_ByHousehold, LS_ByPlug} = AllLoadSummaries,
    {HouseID, HouseholdID, PlugID} = IDs,
    LS_House = maps:get(HouseID, LS_ByHouse, new_load_summary()),
    LS_Household = maps:get({HouseID,HouseholdID},
                             LS_ByHousehold,
                             new_load_summary()),
    LS_Plug = maps:get({HouseID,HouseholdID,PlugID},
                        LS_ByPlug,
                        new_load_summary()),

    %% 8 averages total. Sum product with the weights for a prediction
    Avgs = get_ls_averages(LS_Global, NewTimeOfDay)
           ++ get_ls_averages(LS_House, NewTimeOfDay)
           ++ get_ls_averages(LS_Household, NewTimeOfDay)
           ++ get_ls_averages(LS_Plug, NewTimeOfDay),

    Prediction = lists:sum(lists:zipwith(fun (X, Y) -> X * Y end, Avgs, Weights)),
    Prediction.

-spec predict_house_load(all_load_summaries(), weights(), time_of_day(), house_id()) -> float().
predict_house_load(AllLoadSummaries, Weights, NewTimeOfDay, HouseID) ->
    {LS_Global, LS_ByHouse, _LS_ByHousehold, _LS_ByPlug} = AllLoadSummaries,
    LS_House = maps:get(HouseID,LS_ByHouse,new_load_summary()),

    %% 4 averages total. Sum product with the weights for a prediction
    Avgs = get_ls_averages(LS_Global, NewTimeOfDay)
           ++ get_ls_averages(LS_House, NewTimeOfDay),

    Prediction = lists:sum(lists:zipwith(fun (X, Y) -> X * Y end, Avgs, Weights)),
    Prediction.

-spec output_predictions(all_load_summaries(), all_weights(), time_of_day(), pid()) -> ok.
output_predictions(AllLoadSummaries, AllWeights, NewTimeOfDay, SinkPID) ->
    {_LS_Global, LS_ByHouse, _LS_ByHousehold, LS_ByPlug} = AllLoadSummaries,
    {HouseWeights, PlugWeights} = AllWeights,
    Houses = maps:keys(LS_ByHouse),
    HouseHouseholdPlugs = maps:keys(LS_ByPlug),

    lists:foreach(
        fun (House) ->
            SinkPID ! {'house prediction', House,
                       predict_house_load(AllLoadSummaries, HouseWeights,
                                          NewTimeOfDay, House)}
        end, Houses),
    %% Output the predicted load for each plug
    lists:foreach(
        fun ({House, Household, Plug}) ->
            SinkPID ! {'    plug prediction', Plug,
                       predict_plug_load(AllLoadSummaries, PlugWeights,
                                          NewTimeOfDay,
                                          {House, Household, Plug})}
        end, HouseHouseholdPlugs),

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
update({{{house, work}, _HouseId}, _Payload}, State, _SinkPid) ->
    State;
update({{{house, load}, HouseID}, Payload}, State, _SinkPid) ->
    {HouseholdID, PlugID, _MeasID, _MeasTS, MeasVal} = Payload,
    {TimeOverall, TimeOfDay, AllLoadSummaries, AllWeights} = State,
    {LS_Global, LS_ByHouse, LS_ByHousehold, LS_ByPlug} = AllLoadSummaries,

    %% Note: to debug you have to send the messages to the sink
    %% because the stdout of the workers is not shown on the shell
    % {sink, node()} ! {message_load, Payload},

    %% New State
    {TimeOverall, TimeOfDay,
     {update_load_summary(LS_Global, MeasVal, TimeOfDay),
      update_load_summary_map(LS_ByHouse, HouseID, MeasVal, TimeOfDay),
      update_load_summary_map(LS_ByHousehold, {HouseID, HouseholdID}, MeasVal, TimeOfDay),
      update_load_summary_map(LS_ByPlug, {HouseID, HouseholdID, PlugID}, MeasVal, TimeOfDay)
     }, AllWeights};
update({end_timeslice, TimeValue}, State, SinkPid) ->
    {_TimeOverall, _TimeOfDay, AllLoadSummaries, AllWeights} = State,
    {LS_Global, LS_ByHouse, LS_ByHousehold, LS_ByPlug} = AllLoadSummaries,

    %% New Timeslice
    NewTimeOfDay = get_time_of_day(TimeValue),

    %% Output: for latency
    SinkPid ! {output, {{end_timeslice, TimeValue}, NewTimeOfDay}},

    %% Output: the load prediction for each house and each plug
    %output_predictions(AllLoadSummaries, AllWeights, NewTimeOfDay, SinkPid),

    %% Output the state (comment out if desired)
    % SinkPid ! {TimeValue, get_time_of_day(TimeValue), LS_Global, LS_ByHouse, LS_ByHousehold},

    %% New State
    NewGlobal      = reset_load_summary(LS_Global),
    NewByHouse     = reset_load_summary_map(LS_ByHouse),
    NewByHousehold = reset_load_summary_map(LS_ByHousehold),
    NewByPlug      = reset_load_summary_map(LS_ByPlug),
    {TimeValue, NewTimeOfDay,
     {NewGlobal, NewByHouse, NewByHousehold, NewByPlug},
     AllWeights}.

%% ========== Parallelization Primitives ==========

%% To fork the state: split the maps by key. Fork the global load
%% summary by preserving the sum (fork (x,y) -> (x,y), (0,0)).

-spec fork(split_preds(), state()) -> {state(), state()}.
fork(SplitPreds, State) ->
    {TimeOverall, TimeOfDay, AllLoadSummaries, AllWeights} = State,
    {LS_Global, LS_ByHouse, LS_ByHousehold, LS_ByPlug} = AllLoadSummaries,
    {Pred1, _Pred2} = SplitPreds,

    %% Split the load summaries
    {Global1, Global2} = {LS_Global, new_load_summary()},
    {ByHouse1, ByHouse2} =
        util:split_map(
            LS_ByHouse,
            fun (HouseID) ->
                Pred1({{house, load}, HouseID})
            end),
    {ByHousehold1, ByHousehold2} =
        util:split_map(
            LS_ByHousehold,
            fun ({HouseID, _HouseholdID}) ->
                Pred1({{house, load}, HouseID})
            end),
    {ByPlug1, ByPlug2} =
        util:split_map(
            LS_ByPlug,
            fun ({HouseID, _HouseholdID, _PlugID}) ->
                Pred1({{house, load}, HouseID})
            end),

    All1 = {Global1, ByHouse1, ByHousehold1, ByPlug1},
    All2 = {Global2, ByHouse2, ByHousehold2, ByPlug2},

    %% Split the weights -- set the second to nothing (it's unused)
    Weights1 = AllWeights,
    Weights2 = {[], []},

    % {sink, node()} ! ['DEBUG:   Fork State', LS_Global, LS_ByHouse],
    % {sink, node()} ! ['DEBUG: Fork Result1', Global1, ByHouse1],
    % {sink, node()} ! ['DEBUG: Fork Result2', Global2, ByHouse2],
    {{TimeOverall, TimeOfDay, All1, Weights1},
     {TimeOverall, TimeOfDay, All2, Weights2}}.

-spec join(state(), state()) -> state().
join(State1, State2) ->
    {TimeOverall, TimeOfDay, All1, Weights1} = State1,
    {_TimeOverall, _TimeOfDay, All2, _Weights2} = State2,
    {Global1, ByHouse1, ByHousehold1, ByPlug1} = All1,
    {Global2, ByHouse2, ByHousehold2, ByPlug2} = All2,

    Global      = add_load_summaries(Global1, Global2),
    ByHouse     = maps:merge(ByHouse1, ByHouse2),
    ByHousehold = maps:merge(ByHousehold1, ByHousehold2),
    ByPlug      = maps:merge(ByPlug1, ByPlug2),

    %% Join the weights -- use the first and ignore the second
    Weights = Weights1,

    All = {Global, ByHouse, ByHousehold, ByPlug},
    % {sink, node()} ! ['DEBUG: Join State1', Global1, ByHouse1],
    % {sink, node()} ! ['DEBUG: Join State2', Global2, ByHouse2],
    % {sink, node()} ! ['DEBUG: Join Result', Global, ByHouse],
    {TimeOverall, TimeOfDay, All, Weights}.

%% ============================
%% ===== Experiment Setup =====
%% ============================

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

experiment_distributed() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, run_experiment_distributed, [SinkName, optimizer_greedy]),
    util:sink().

run_experiment_distributed(SinkPid, _Optimizer) ->
    %% Architecture
    %% Rates = [{node(), minute, 10},
    %%       {node(), {a,1}, 1000},
    %%       {node(), {a,2}, 1000},
    %%       {node(), b, 1000}],
    %% Topology =
    %%  conf_gen:make_topology(Rates, SinkPid),

    %% %% Computation
    %% Tags = [minute, {a,1}, {a,2}, b],
    %% StateTypesMap =
    %%  #{'state0' => {sets:from_list(Tags), fun update0/3},
    %%    'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update_a/3},
    %%    'state_b' => {sets:from_list([b]), fun update_b/3}},
    %% SplitsMerges = [{{'state0', 'state_a', 'state_b'}, {fun split1/2, fun merge1/2}},
    %%              {{'state_a', 'state_a', 'state_a'}, {fun split_a/2, fun merge_a/2}}],
    %% Dependencies = dependencies(),
    %% InitState = {'state0', init_state()},
    %% Specification =
    %%  conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    %% ConfTree = conf_gen:generate(Specification, Topology, [{optimizer, Optimizer}]),

    %% %% Set up where will the input arrive
    %% create_producers(fun minute_markers_input/0, minute, ConfTree, Topology),

    SinkPid ! finished.

experiment_sequential() ->
    NumHouses = 5,
    NodeNames = [node() || _ <- lists:seq(0, NumHouses)],
    setup_experiment(optimizer_sequential, NodeNames, 60, 10, 20, log_latency_throughput).

experiment_greedy() ->
    NumHouses = 5,
    NodeNames = [node() || _ <- lists:seq(0, NumHouses)],
    setup_experiment(optimizer_greedy, NodeNames, 60, 10, 20, log_latency_throughput).

setup_experiment(Optimizer, NodeNames, EndtimeslicePeriodSeconds,
                 EndtimesliceHeartbeatPeriodSeconds, RateMultiplier, DoLog) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, run_experiment,
                          [SinkName, Optimizer, NodeNames, EndtimeslicePeriodSeconds,
                           EndtimesliceHeartbeatPeriodSeconds, RateMultiplier, DoLog]),
    case DoLog of
        log_latency_throughput ->
            LoggerInitFun =
                fun() ->
                        log_mod:initialize_message_logger_state("sink", sets:from_list([output]))
                end,
            util:sink(LoggerInitFun);
        no_log ->
            util:sink()
    end.


run_experiment(SinkPid, Optimizer, NodeNames, EndtimeslicePeriodSeconds,
               EndtimesliceHeartbeatPeriodSeconds, RateMultiplier, DoLog) ->

    io:format("Args:~n~p~n", [[SinkPid, Optimizer, NodeNames, EndtimeslicePeriodSeconds,
                               EndtimesliceHeartbeatPeriodSeconds, RateMultiplier, DoLog]]),

    %% We assume that the first node name is the main node
    [MainNodeName|HouseNodeNames] = NodeNames,

    %% We assume that there is one node for each house and one for main
    NumHouses = length(HouseNodeNames),

    HouseTags = [{{house, load}, Id} || Id <- lists:seq(0, NumHouses - 1)],
    Tags = [end_timeslice|HouseTags],

    %% Some Garbage
    BeginSimulationTime = 1377986401000,
    %% EndSimulationTime   = 1377986427000,
    %% Arbitrarily long end simulation time
    %EndSimulationTime   = 1380000000000,
    EndSimulationTime   = 1380578399000,

    %% Architecture
    %% TODO: Make this parametrizable (the nodes)
    HouseRates = [{HouseNodeName, HTag, 1000}
                  || {HTag, HouseNodeName} <- lists:zip(HouseTags, HouseNodeNames)],
    Rates = [{MainNodeName, end_timeslice, 1}|HouseRates],
    Topology =
        conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    StateTypesMap =
        #{'state' => {sets:from_list(Tags), fun update/3}},
    SplitsMerges =
        [{{'state', 'state', 'state'}, {fun fork/2, fun join/2}}],
    Dependencies = dependencies(NumHouses),
    io:format("Dependencies: ~p~n", [Dependencies]),
    InitState = {'state', init_state(BeginSimulationTime)},
    Specification =
        conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),


    %% Maybe setup logging
    LogOptions =
        case DoLog of
            log_latency_throughput ->
                LogTriple = log_mod:make_num_log_triple(),
                [{log_triple, LogTriple}];
            no_log ->
                []
        end,
    ConfTree = conf_gen:generate(Specification, Topology,
                                 LogOptions ++ [{optimizer, Optimizer}]),

    %% Prepare the producers input
    Houses = [{Id, HouseNodeName}
              || {Id, HouseNodeName} <- lists:zip(lists:seq(0, NumHouses - 1), HouseNodeNames)],
    ProducerInit =
        make_producer_init(Houses, [load], MainNodeName,
                           10000,         % House heartbeats
                           EndtimeslicePeriodSeconds * 1000,              % End TImeslice period in ms
                           EndtimesliceHeartbeatPeriodSeconds * 1000,     % End timeslice heartbeat period in ms
                           BeginSimulationTime, EndSimulationTime,
                           RateMultiplier),                               % Rate multiplier (10 roughly means 10 * c * NumHouses messages per second

    LoggerInitFun =
        case DoLog of
            log_latency_throughput ->
                _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
                fun() ->
                        log_mod:initialize_message_logger_state("producer", sets:from_list([end_timeslice]))
                end;
            no_log ->
                fun log_mod:no_message_logger/0
        end,

    producer:make_producers(ProducerInit, ConfTree, Topology, steady_timestamp,
                            LoggerInitFun, BeginSimulationTime),

    SinkPid ! finished.


-spec make_producer_init([{house_id(), node()}], ['work' | 'load'], node(), integer(),
                         integer(), integer(), integer(), integer(), integer())
                        -> producer_init(measurement_tag()).
make_producer_init(Houses, WorkLoad, EndTimesliceNode, MeasurementHeartbeatPeriod, EndTimeslicePeriod,
                   EndTimesliceHeartbeatPeriod, BeginTime, EndTime, RateMult) ->
    HousesProducerInit =
        lists:flatmap(
          fun(House) ->
                  make_house_producer_init(House, WorkLoad, MeasurementHeartbeatPeriod,
                                           BeginTime, EndTime, RateMult)
          end, Houses),
    %% NOTE: Offset the timeslice messages to be strictly larger or
    %%       smaller than house messages.
    TimesliceStream =
        {fun ?MODULE:make_end_timeslice_stream/5,
         [EndTimesliceNode, BeginTime + 500, EndTime + 500,
          EndTimeslicePeriod, EndTimesliceHeartbeatPeriod]},
    TimesliceInit =
        {TimesliceStream, {end_timeslice, EndTimesliceNode}, RateMult},
    [TimesliceInit|HousesProducerInit].

-spec make_house_producer_init({house_id(), node()}, ['work' | 'load'], integer(),
                               integer(), integer(), integer())
                              -> producer_init(measurement_tag()).
make_house_producer_init({HouseId, Node}, WorkLoad, MeasurementHeartbeatPeriod, BeginTime, EndTime, RateMult) ->
    lists:map(
      fun(WorkOrLoad) ->
              GenInit =
                  {fun ?MODULE:make_house_generator/6,
                   [HouseId, WorkOrLoad, Node, MeasurementHeartbeatPeriod, BeginTime, EndTime]},
              {GenInit, {{{house, WorkOrLoad}, HouseId}, Node}, RateMult}
      end, WorkLoad).



-spec make_end_timeslice_stream(node(), integer(), integer(), integer(), integer()) -> msg_generator().
make_end_timeslice_stream(Node, From, To, Step, HeartbeatPeriod) ->
    Timeslices
        = [{{end_timeslice, T}, Node, T}
           || T <- lists:seq(From, To, Step)],
    %% ++ [{heartbeat, {{end_timeslice, Node}, To + 1}}],
    TimeslicesWithHeartbeats =
        producer:interleave_heartbeats(Timeslices, {{end_timeslice, Node}, HeartbeatPeriod}, From, To + 2),
    {sink, Node} ! {end_timeslice_length, length(TimeslicesWithHeartbeats)},
    producer:list_generator(TimeslicesWithHeartbeats).


%% Makes a generator for that house, and adds heartbeats
-spec make_house_generator(integer(), atom(), node(), integer(), timestamp(), timestamp()) -> msg_generator().
make_house_generator(HouseId, WorkLoad, NodeName, Period, From, Until) ->
    Filename = io_lib:format("data/debs_house_~w_~s", [HouseId, atom_to_list(WorkLoad)]),
    %% producer:file_generator(Filename, fun parse_house_csv_line/1).
    producer:file_generator_with_heartbeats(Filename, fun parse_house_csv_line/1,
                                            {{{{house, WorkLoad},HouseId}, NodeName}, Period}, From, Until).

%% NOTE: This adjusts the timestamps to be ms instead of seconds
-spec parse_house_csv_line(string()) -> impl_message(measurement_tag(), measurement_payload()).
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
    {{{{house, Prop}, House}, {Household, Plug, Id, Ts, Value}}, Node, Ts}.
