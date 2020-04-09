-module(abexample).

-export([distributed/0,
	 distributed_conf/1,
	 distributed_1/0,
	 distributed_conf_1/1,
	 real_distributed/1,
	 real_distributed_conf/2,
	 distributed_experiment/5,
	 distributed_experiment_conf/6,
	 seq_big/0,
	 seq_big_conf/1,
	 distr_big/0,
	 distr_big_conf/1,
	 greedy_big/0,
	 greedy_big_conf/2,
	 greedy_big_conf_test/2,
	 greedy_complex/0,
	 greedy_complex_conf/1,
	 greedy_local/0,
	 greedy_local_conf/1,
	 make_as/4,
	 make_bs_heartbeats/4
	]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("flumina/include/type_definitions.hrl").

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

seq_big() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, seq_big_conf, [SinkName]),
    util:sink().

seq_big_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [b, {a,1}, {a,2}],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3}},
    SplitsMerges = [],
    Dependencies = dependencies(),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer,optimizer_sequential}]),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(node(), node(), node()),
    InputStreams = [{A1, {{a,1},node()}, 10},
		    {A2, {{a,2},node()}, 10},
		    {Bs, {b, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    SinkPid ! finished.

distr_big() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, distr_big_conf, [SinkName]),
    util:sink().

distr_big_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, node(), {fun isTagA1/1, fun(Msg) -> isImplA1(Msg, node()) end}, Funs, []},
    NodeA2 = {0, node(), {fun isTagA2/1, fun(Msg) -> isImplA2(Msg, node()) end}, Funs, []},
    NodeB  = {0, node(), {fun true_pred/1, fun true_pred/1}, Funs, [NodeA1, NodeA2]},
    ConfTree = configuration:create(NodeB, dependencies(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(node(), node(), node()),
    InputStreams = [{A1, {{a,1}, node()}, 10},
		    {A2, {{a,2}, node()}, 10},
		    {Bs, {b, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    SinkPid ! finished.



greedy_big() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, greedy_big_conf, [SinkName, steady_sync_timestamp]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

greedy_big_conf(SinkPid, ProducerType) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [b, {a,1}, {a,2}],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3},
	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_a'}, {fun split/2, fun merge/2}}],
    Dependencies = dependencies(),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    LogTriple = log_mod:make_num_log_triple(),
    ConfTree = conf_gen:generate(Specification, Topology,
				 [{optimizer,optimizer_greedy}, {log_triple,LogTriple}]),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(node(), node(), node()),
    %% InputStreams = [{A1, {a,1}, 50}, {A2, {a,2}, 50}, {Bs, b, 500}],
    InputStreams = [{A1, {{a,1}, node()}, 100},
		    {A2, {{a,2}, node()}, 100},
		    {Bs, {b, node()}, 100}],

    log_stats_time_and_number_of_messages(1001000),

    %% Setup logging
    _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, ProducerType, LoggerInitFun),

    SinkPid ! finished.

greedy_big_conf_test(SinkPid, ProducerType) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [b, {a,1}, {a,2}],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3},
	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_a'}, {fun split/2, fun merge/2}}],
    Dependencies = dependencies(),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology,
				 [{optimizer,optimizer_greedy}]),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(node(), node(), node()),
    %% InputStreams = [{A1, {a,1}, 50}, {A2, {a,2}, 50}, {Bs, b, 500}],
    InputStreams = [{A1, {{a,1}, node()}, 100},
		    {A2, {{a,2}, node()}, 100},
		    {Bs, {b, node()}, 100}],

    %% Setup logging
    producer:make_producers(InputStreams, ConfTree, Topology, ProducerType),
    SinkPid ! finished.


greedy_complex() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, greedy_complex_conf, [SinkName]),
    util:sink().

greedy_complex_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000},
	     {node(), {a,3}, 1000},
	     {node(), {a,4}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [b, {a,1}, {a,2}, {a,3}, {a,4}],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3},
	  'state_a' => {sets:from_list([{a,1}, {a,2}, {a,3}, {a,4}]), fun update/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_a'}, {fun split/2, fun merge/2}},
		    {{'state_a', 'state_a', 'state_a'}, {fun split/2, fun merge/2}}],
    Dependencies = complex_dependencies(),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer,optimizer_greedy}]),

    %% Set up where will the input arrive
    {A1, A2, A3, A4, Bs} = complex_input_distr_example(node(), node(), node(), node(), node()),
    InputStreams = [{A1, {{a,1},node()}, 10},
		    {A2, {{a,2},node()}, 10},
		    {A3, {{a,3},node()}, 10},
		    {A4, {{a,4},node()}, 10},
		    {Bs, {b,node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    SinkPid ! finished.



greedy_local() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, greedy_local_conf, [SinkName]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

greedy_local_conf(SinkPid) ->
    %% io:format("Args:~n~p~n", [[SinkPid, NodeNames, RateMultiplier, RatioAB]]),
    NodeNames = [node() || _ <- lists:seq(1,3)],
    RateMultiplier = 20,
    RatioAB = 1000,
    HeartbeatBRatio = 100,

    %% We assume that there is one node for each a and one for b
    NumberAs = length(NodeNames) - 1,
    %% For the rates we only care about ratios
    ARate = 1000000,
    BRate = ARate div RatioAB,

    %% Tags
    ATags = [{a,Id} || Id <- lists:seq(1,NumberAs)],
    Tags = [b] ++ ATags,

    %% We assume that the first node name is the B node
    [BNodeName|ANodeNames] = NodeNames,

    %% Architecture
    ARatesTopo = [{ANN, AT, ARate} || {ANN, AT} <- lists:zip(ANodeNames, ATags)],
    BRateTopo = {BNodeName, b, BRate},
    Rates = [BRateTopo|ARatesTopo],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation

    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3},
	  'state_a' => {sets:from_list(ATags), fun update/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_a'}, {fun split/2, fun merge/2}},
		    {{'state_a', 'state_a', 'state_a'}, {fun split/2, fun merge/2}}],
    Dependencies = parametrized_dependencies(ATags),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    LogTriple = log_mod:make_num_log_triple(),
    ConfTree = conf_gen:generate(Specification, Topology,
				 [{optimizer,optimizer_greedy}, {log_triple,LogTriple}]),

    %% Set up where will the input arrive

    %% Input Streams
    {As, Bs, _NumberOfMessages} =
        parametrized_input_distr_example(NumberAs, NodeNames, RatioAB, HeartbeatBRatio),
    %% InputStreams = [{A1input, {a,1}, 30}, {A2input, {a,2}, 30}, {BsInput, b, 30}],
    AInputStreams = [{AIn, {ATag, ANode}, RateMultiplier}
		     || {AIn, ATag, ANode} <- lists:zip3(As, ATags, ANodeNames)],
    BInputStream = {Bs, {b, BNodeName}, RateMultiplier},
    InputStreams = [BInputStream|AInputStreams],

    %% Log the input times of b messages
    _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, steady_timestamp, LoggerInitFun),

    SinkPid ! finished,
    ok.

%% This is what our compiler would come up with
distributed() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, distributed_conf, [SinkName]),
    util:sink().

distributed_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, node(), {fun isTagA1/1, fun(Msg) -> isImplA1(Msg, node()) end}, Funs, []},
    NodeA2 = {0, node(), {fun isTagA2/1, fun(Msg) -> isImplA2(Msg, node()) end}, Funs, []},
    NodeB  = {0, node(), {fun true_pred/1, fun true_pred/1}, Funs, [NodeA1, NodeA2]},
    ConfTree = configuration:create(NodeB, dependencies(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    {A1, A2, Bs} = input_example2(node(), node(), node()),
    InputStreams = [{{fun() -> producer:list_generator(A1) end, []}, {{a,1}, node()}, 10},
		    {{fun() -> producer:list_generator(A2) end, []}, {{a,2}, node()}, 10},
		    {{fun() -> producer:list_generator(Bs) end, []}, {b, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    io:format("Tree: ~p~n", [ConfTree]),
    SinkPid ! finished,
    ok.

distributed_1() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, distributed_conf_1, [SinkName]),
    util:sink().

distributed_conf_1(SinkPid) ->
    %% Architecture
    Rates = [{node(), b, 10},
	     {node(), {a,1}, 1000},
	     {node(), {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    ImplTags = [{Tag, Node} || {Node, Tag, _R} <- Rates],

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, node(), {fun isTagA1/1, fun(Msg) -> isImplA1(Msg, node()) end}, Funs, []},
    NodeA2 = {0, node(), {fun isTagA2/1, fun(Msg) -> isImplA2(Msg, node()) end}, Funs, []},
    NodeB  = {0, node(), {fun true_pred/1, fun true_pred/1}, Funs, [NodeA1, NodeA2]},
    ConfTree = configuration:create(NodeB, dependencies(), SinkPid, ImplTags),

    %% Set up where will the input arrive
    {A1, A2, Bs} = input_example(node(), node(), node()),
    io:format("Inputs: ~p~n", [{A1, A2, Bs}]),
    InputStreams = [{{fun() -> producer:list_generator(A1) end, []}, {{a,1}, node()}, 10},
		    {{fun() -> producer:list_generator(A2) end, []}, {{a,2}, node()}, 10},
		    {{fun() -> producer:list_generator(Bs) end, []}, {b, node()}, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),

    %% io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
    SinkPid ! finished,
    ok.

%%% Has to be called with long node names
real_distributed(NodeNames) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, real_distributed_conf, [SinkName, NodeNames]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

real_distributed_conf(SinkPid, [A1NodeName, A2NodeName, BNodeName]) ->
    %% Architecture
    Rates = [{BNodeName, b, 10},
	     {A1NodeName, {a,1}, 1000},
	     {A2NodeName, {a,2}, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [b, {a,1}, {a,2}],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3},
	  'state_a' => {sets:from_list([{a,1}, {a,2}]), fun update/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_a'}, {fun split/2, fun merge/2}}],
    Dependencies = dependencies(),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    LogTriple = log_mod:make_num_log_triple(),
    ConfTree = conf_gen:generate(Specification, Topology,
				 [{optimizer, optimizer_greedy}, {log_triple, LogTriple}]),

    %% Set up where will the input arrive

    %% Big Inputs
    {A1, A2, Bs} = big_input_distr_example(A1NodeName, A2NodeName, BNodeName),
    %% InputStreams = [{A1input, {a,1}, 30}, {A2input, {a,2}, 30}, {BsInput, b, 30}],
    InputStreams = [{A1, {{a,1}, A1NodeName}, 100},
		    {A2, {{a,2}, A2NodeName}, 100},
		    {Bs, {b, BNodeName}, 100}],

    %% BsInput = bs_input_example(),
    %% {A1input, A2input} = as_input_example(),
    %% InputStreams = [{BsInput, b, 10}, {A1input, {a,1}, 10}, {A2input, {a,2}, 10}],

    %% Log the input times of b messages
    _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, steady_timestamp, LoggerInitFun),

    SinkPid ! finished,
    ok.


%% TODO: Maybe also parametrize the b heartbeat ratio
distributed_experiment(NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio, Optimizer) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, distributed_experiment_conf,
			 [SinkName, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio, Optimizer]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

distributed_experiment_conf(SinkPid, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio, Optimizer) ->
    io:format("Args:~n~p~n", [[SinkPid, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio, Optimizer]]),

    %% We assume that the first node name is the B node
    [BNodeName|ANodeNames] = NodeNames,

    %% We assume that there is one node for each a and one for b
    NumberAs = length(NodeNames) - 1,
    %% For the rates we only care about ratios
    ARate = 1000000,
    BRate = ARate div RatioAB,

    %% Tags
    ATags = [{a,Id} || Id <- lists:seq(1,NumberAs)],
    Tags = [b] ++ ATags,



    %% Architecture
    ARatesTopo = [{ANN, AT, ARate} || {ANN, AT} <- lists:zip(ANodeNames, ATags)],
    BRateTopo = {BNodeName, b, BRate},
    Rates = [BRateTopo|ARatesTopo],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation

    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update/3},
	  'state_a' => {sets:from_list(ATags), fun update/3}},
    SplitsMerges = [{{'state0', 'state_a', 'state_a'}, {fun split/2, fun merge/2}},
		    {{'state_a', 'state_a', 'state_a'}, {fun split/2, fun merge/2}}],
    Dependencies = parametrized_dependencies(ATags),
    InitState = {'state0', 0},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    %% LogTriple = log_mod:make_num_log_triple(),
    ConfTree = conf_gen:generate(Specification, Topology,
				 [{optimizer,Optimizer}
				  %% {checkpoint, fun conf_gen:always_checkpoint/2},
				  %% {log_triple, LogTriple}
                                 ]),

    %% Set up where will the input arrive

    %% Input Streams
    {As, BsMsgInit, NumberOfMessages} =
        parametrized_input_distr_example(NumberAs, NodeNames, RatioAB, HeartbeatBRatio),
    AInputStreams = [{AIn, {ATag, ANode}, RateMultiplier}
		     || {AIn, ATag, ANode} <- lists:zip3(As, ATags, ANodeNames)],
    BInputStream = {BsMsgInit, {b, BNodeName}, RateMultiplier},
    InputStreams = [BInputStream|AInputStreams],

    %% Log the current time and total number of events
    log_stats_time_and_number_of_messages(NumberOfMessages),

    %% Log the input times of b messages
    %% _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun =
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, steady_sync_timestamp, LoggerInitFun),

    SinkPid ! finished,
    ok.

log_stats_time_and_number_of_messages(NumberOfMessages) ->
    %% Setup the statistics filename
    StatsFilename = "logs/experiment_stats.log",
    %% Log the time before the producers have been spawned and the
    %% number of events that will be sent in total.
    BeforeProducersTimestamp = erlang:monotonic_time(),
    StatsData = io_lib:format("ab-experiment -- Time before spawning producers: ~p, Number of messages: ~p~n",
                             [BeforeProducersTimestamp, NumberOfMessages]),
    ok = file:write_file(StatsFilename, StatsData).


%% The specification of the computation
update({{a,_}, Value}, Sum, SendTo) ->
    %% This is here for debugging purposes
    %% io:format("log: ~p~n", [{self(), a, Value, Ts}]),
    %% SendTo ! {self(), a, Value, Ts},
    Sum + Value;
update({b, Ts}, Sum, SendTo) ->
    SendTo ! {sum, {{b, Ts}, Sum}},
    Sum.

merge(Sum1, Sum2) ->
    Sum1 + Sum2.

%% This split doesn't use the predicates
split(_, Sum) ->
    {Sum, 0}.

dependencies() ->
    #{{a,1} => [b],
      {a,2} => [b],
      b => [{a,1}, {a,2}, b]
     }.

complex_dependencies() ->
    #{{a,1} => [b],
      {a,2} => [b],
      {a,3} => [b],
      {a,4} => [b],
      b => [{a,1}, {a,2}, {a,3}, {a,4}, b]
     }.

parametrized_dependencies(ATags) ->
    ADeps = [{ATag, [b]} || ATag <- ATags],
    BDeps = {b, ATags},
    maps:from_list([BDeps|ADeps]).

%% THe implementation predicates
isImplA1({{{a,1}, _V}, Node, _Ts}, Node) ->
    true;
isImplA1(_, _) ->
    false.

isImplA2({{{a,2}, _V}, Node, _Ts}, Node) ->
    true;
isImplA2(_, _) ->
    false.


%% The predicates
isA1({{a,1}, _}) -> true;
isA1(_) -> false.

isA2({{a,2}, _}) -> true;
isA2(_) -> false.

isTagA1({a,1}) -> true;
isTagA1(_) -> false.

isTagA2({a,2}) -> true;
isTagA2(_) -> false.

%% isB({b, _, _}) -> true;
%% isB(_) -> false.

true_pred(_) -> true.


%% Some input examples
input_example(NodeA1, NodeA2, NodeB) ->
    AllInputs = [gen_a(V) || V <- lists:seq(1, 1000)] ++ [gen_a(V) || V <- lists:seq(1002, 2000)],
    A1 = [{Msg, NodeA1, Ts}  || {_, Ts} = Msg <- AllInputs, isA1(Msg)]
	++ [{heartbeat, {{{a,1},NodeA1},2005}}],
    A2 = [{Msg, NodeA2, Ts}  || {_, Ts} = Msg <- AllInputs, isA2(Msg)]
	++ [{heartbeat, {{{a,2},NodeA2},2005}}],
    B = [{{b, 1001}, NodeB, 1001}, {{b, 2001}, NodeB, 2001}, {heartbeat, {{b,NodeB},2005}}],
    {A1, A2, B}.


-spec make_as(integer(), node(), integer(), integer()) -> msg_generator().
make_as(Id, ANode, N, Step) ->
    As = [{{{a,Id}, T}, ANode, T} || T <- lists:seq(1, N, Step)]
	++ [{heartbeat, {{{a,Id}, ANode}, N + 1}}],
    producer:list_generator(As).

-spec make_bs_heartbeats(node(), integer(), integer(), integer()) -> msg_generator().
make_bs_heartbeats(BNodeName, LengthAStream, RatioAB, HeartbeatBRatio) ->
    LengthBStream = LengthAStream div RatioAB,
    Bs = lists:flatten(
	   [[{heartbeat, {{b, BNodeName}, (T * RatioAB div HeartbeatBRatio) + (RatioAB * BT)}}
	     || T <- lists:seq(0, HeartbeatBRatio - 2)]
	    ++ [{{b, RatioAB + (RatioAB * BT)}, BNodeName, RatioAB + (RatioAB * BT)}]
	    || BT <- lists:seq(0,LengthBStream - 1)])
	++ [{heartbeat, {{b, BNodeName}, LengthAStream + 1}}],
    producer:list_generator(Bs).

%% WARNING: The hearbeat ratio needs to be a divisor of RatioAB (Maybe not necessarily)
-spec parametrized_input_distr_example(integer(), [node()], integer(), integer())
				      -> {[msg_generator_init()], msg_generator_init(), integer()}.
parametrized_input_distr_example(NumberAs, [BNodeName|ANodeNames], RatioAB, HeartbeatBRatio) ->
    LengthAStream = 1000000,
    %% Return a triple that makes the results
    As = [{fun abexample:make_as/4, [Id, ANode, LengthAStream, 1]}
	  || {Id, ANode} <- lists:zip(lists:seq(1, NumberAs), ANodeNames)],

    Bs = {fun abexample:make_bs_heartbeats/4, [BNodeName, LengthAStream, RatioAB, HeartbeatBRatio]},

    %% Return the streams and the total number of messages
    {As, Bs, (LengthAStream * NumberAs) + (LengthAStream div RatioAB)}.



-spec big_input_distr_example(node(), node(), node())
			     -> {msg_generator_init(), msg_generator_init(), msg_generator_init()}.
big_input_distr_example(NodeA1, NodeA2, NodeB) ->
    LengthA = 1000000,
    A1 = {fun abexample:make_as/4, [1, NodeA1, LengthA, 2]},
    %% A1 = lists:flatten(
    %% 	   [[{{{a,1}, T + (1000 * BT)}, NodeA1, T + (1000 * BT)}
    %% 	     || T <- lists:seq(1, 999, 2)]
    %% 	    || BT <- lists:seq(0,1000)])
    %% 	++ [{heartbeat, {{{a,1},NodeA1},1000000}}],

    A2 = {fun abexample:make_as/4, [2, NodeA2, LengthA, 2]},
    %% A2 = lists:flatten(
    %% 	   [[{{{a,2}, T + (1000 * BT)}, NodeA2, T + (1000 * BT)}
    %% 	     || T <- lists:seq(2, 998, 2)]
    %% 	    || BT <- lists:seq(0,1000)])
    %% 	++ [{heartbeat, {{{a,2},NodeA2},1000000}}],

    Bs = {fun abexample:make_bs_heartbeats/4, [NodeB, LengthA, 1000, 1]},
    %% Bs = [{{b, 1000 + (1000 * BT)},NodeB, 1000 + (1000 * BT)}
    %% 	  || BT <- lists:seq(0,1000)]
    %% 	++ [{heartbeat, {{b,NodeB},1000000}}],
    {A1, A2, Bs}.

-spec complex_input_distr_example(node(), node(), node(), node(), node())
				 -> {msg_generator_init(), msg_generator_init(),
				     msg_generator_init(), msg_generator_init(), msg_generator_init()}.
complex_input_distr_example(NodeA1, NodeA2, NodeA3, NodeA4, NodeB) ->
    LengthA = 1000000,
    {A1, A2, Bs} = big_input_distr_example(NodeA1, NodeA2, NodeB),
    A3 = {fun abexample:make_as/4, [3, NodeA3, LengthA, 2]},
    %% A3 = lists:flatten(
    %% 	   [[{{{a,3}, T + (1000 * BT)}, NodeA3, T + (1000 * BT)}
    %% 	     || T <- lists:seq(1, 999, 2)]
    %% 	    || BT <- lists:seq(1,1000)])
    %% 	++ [{heartbeat, {{{a,3},NodeA3}, 10000000}}],
    A4 = {fun abexample:make_as/4, [4, NodeA4, LengthA, 2]},
    %% A4 = lists:flatten(
    %% 	   [[{{{a,4}, T + (1000 * BT)}, NodeA4, T + (1000 * BT)}
    %% 	     || T <- lists:seq(2, 998, 2)]
    %% 	    || BT <- lists:seq(1,1000)])
    %% 	++ [{heartbeat, {{{a,4},NodeA4}, 10000000}}],
    {A1, A2, A3, A4, Bs}.

%% bs_input_example() ->
%%     [{b, 1001, empty},
%%      {b, 2001, empty},
%%      {heartbeat, {b,2005}}].

%% as_input_example() ->
%%     AllAs =
%% 	[gen_a(V) || V <- lists:seq(1, 1000, 2)] ++
%% 	[gen_a(V) || V <- lists:seq(1002, 2000, 2)],
%%     {A1s, A2s} = lists:partition(fun({{a,Id},_,_}) -> Id =:= 1 end, AllAs),
%%     {A1s ++ [{heartbeat, {{a,1},2005}}],
%%      A2s ++ [{heartbeat, {{a,2},2005}}]}.

gen_a(V) ->
    Id = random:uniform(2),
    {{a, Id}, V}.

input_example2(NodeA1, NodeA2, NodeB) ->
    A1 = [{{{a,1}, 1}, NodeA1, 1},
	  {{{a,1}, 5}, NodeA1, 3},
	  {{{a,1}, 3}, NodeA1, 4},
	  {heartbeat, {{{a,1}, NodeA1}, 5}},
	  {{{a,1}, 7}, NodeA1, 7},
	  {heartbeat, {{{a,1}, NodeA1}, 10}},
	  {{{a,1}, 3}, NodeA1, 15},
	  {heartbeat, {{{a,1}, NodeA1}, 20}}],
    A2 = [{heartbeat, {{{a,2}, NodeA2}, 5}},
	  {{{a,2}, 6}, NodeA2, 6},
	  {{{a,2}, 5}, NodeA2, 8},
	  {{{a,2}, 6}, NodeA2, 10},
	  {heartbeat, {{{a,2}, NodeA2}, 10}},
	  {{{a,2}, 5}, NodeA2, 11},
	  {{{a,2}, 1}, NodeA2, 12},
	  {{{a,2}, 0}, NodeA2, 13},
	  {{{a,2}, 9}, NodeA2, 14},
	  {heartbeat, {{{a,2}, NodeA2}, 20}}],
    B = [{{b, 2}, NodeB, 2},
	 {{b, 5}, NodeB, 5},
	 {heartbeat, {{b, NodeB}, 7}},
	 {{b, 9}, NodeB, 9},
	 {heartbeat, {{b, NodeB}, 9}},
	 {{b, 16}, NodeB, 16},
	 {heartbeat, {{b, NodeB}, 20}}],
    {A1, A2, B}.

%% -------- TESTS -------- %%

input_example_output() ->
    [{sum,{{b,1001},500500}},
     {sum,{{b,2001},1999999}}].

input_example_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mf({?MODULE, distributed_conf_1}, input_example_output()))
      end} || _ <- Rounds]}.

input_example2_output() ->
    [{sum,{{b,2},1}},
     {sum,{{b,5},9}},
     {sum,{{b,9},27}},
     {sum,{{b,16},51}}].

input_example2_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example2 test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mf({?MODULE, distributed_conf}, input_example2_output()))
      end} || _ <- Rounds]}.

input_greedy_big_output() ->
    [{sum, {{b, B}, B * B div 2}} || B <- lists:seq(1000,1000000,1000)].

input_greedy_big_test_() ->
    ProducerTypes = [timestamp_based,
                     steady_timestamp,
                     steady_sync_timestamp
                    ],
    {"Input greedy big test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) -> {timeout, 120,
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, greedy_big_conf_test}, [ProducerType],
                                                 input_greedy_big_output())) }
      end} || ProducerType <- ProducerTypes]}.
