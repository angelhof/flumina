-module(abexample).

-export([distributed/0,
	 distributed_conf/1,
	 distributed_1/0,
	 distributed_conf_1/1,
	 real_distributed/1,
	 real_distributed_conf/2,
	 distributed_experiment/4,
	 distributed_experiment_conf/5,
	 seq_big/0,
	 seq_big_conf/1,
	 distr_big/0,
	 distr_big_conf/1,
	 greedy_big/0,
	 greedy_big_conf/1,
	 greedy_complex/0,
	 greedy_complex_conf/1,
	 greedy_local/0,
	 greedy_local_conf/1
	]).

-include_lib("eunit/include/eunit.hrl").

-include("type_definitions.hrl").

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
    
    ConfTree = conf_gen:generate(Specification, Topology, optimizer_sequential),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(),
    InputStreams = [{A1, {a,1}, 10}, {A2, {a,2}, 10}, {Bs, b, 10}],
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

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, node(), fun isA1/1, Funs, []},
    NodeA2 = {0, node(), fun isA2/1, Funs, []},
    NodeB  = {0, node(), fun true_pred/1, Funs, [NodeA1, NodeA2]},
    ConfTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(),
    InputStreams = [{A1, {a,1}, 10}, {A2, {a,2}, 10}, {Bs, b, 10}],
    producer:make_producers(InputStreams, ConfTree, Topology),   

    SinkPid ! finished.



greedy_big() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, greedy_big_conf, [SinkName]),
    LoggerInitFun = 
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

greedy_big_conf(SinkPid) ->
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
    ConfTree = conf_gen:generate(Specification, Topology, LogTriple, optimizer_greedy),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(),
    %% InputStreams = [{A1, {a,1}, 50}, {A2, {a,2}, 50}, {Bs, b, 500}],
    InputStreams = [{A1, {a,1}, 100}, {A2, {a,2}, 100}, {Bs, b, 100}],

    %% Setup logging
    _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun = 
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, timestamp_based, LoggerInitFun),

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
    
    ConfTree = conf_gen:generate(Specification, Topology, optimizer_greedy),

    %% Set up where will the input arrive
    {A1, A2, A3, A4, Bs} = complex_input_distr_example(),
    InputStreams = [{A1, {a,1}, 10}, {A2, {a,2}, 10}, {A3, {a,3}, 10}, {A4, {a,4}, 10}, {Bs, b, 10}],
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
    ConfTree = conf_gen:generate(Specification, Topology, LogTriple, optimizer_greedy),

    %% Set up where will the input arrive

    %% Input Streams
    {As, Bs} = parametrized_input_distr_example(NumberAs, RatioAB, HeartbeatBRatio),
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
    producer:make_producers(InputStreams, ConfTree, Topology, timestamp_based, LoggerInitFun),

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

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, node(), fun isA1/1, Funs, []},
    NodeA2 = {0, node(), fun isA2/1, Funs, []},
    NodeB  = {0, node(), fun true_pred/1, Funs, [NodeA1, NodeA2]},
    ConfTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    {A1, A2, Bs} = input_example2(),
    InputStreams = [{A1, {a,1}, 10}, {A2, {a,2}, 10}, {Bs, b, 10}],
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

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, node(), fun isA1/1, Funs, []},
    NodeA2 = {0, node(), fun isA2/1, Funs, []},
    NodeB  = {0, node(), fun true_pred/1, Funs, [NodeA1, NodeA2]},
    ConfTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    {A1, A2, Bs} = input_example(),
    InputStreams = [{A1, {a,1}, 10}, {A2, {a,2}, 10}, {Bs, b, 10}],
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
    ConfTree = conf_gen:generate(Specification, Topology, LogTriple, optimizer_greedy),

    %% Set up where will the input arrive

    %% Big Inputs
    {A1, A2, Bs} = big_input_distr_example(),
    %% InputStreams = [{A1input, {a,1}, 30}, {A2input, {a,2}, 30}, {BsInput, b, 30}],
    InputStreams = [{A1, {a,1}, 100}, {A2, {a,2}, 100}, {Bs, b, 100}],

    %% BsInput = bs_input_example(),
    %% {A1input, A2input} = as_input_example(),
    %% InputStreams = [{BsInput, b, 10}, {A1input, {a,1}, 10}, {A2input, {a,2}, 10}],

    %% Log the input times of b messages
    _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    LoggerInitFun = 
	fun() ->
	        log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	end,
    producer:make_producers(InputStreams, ConfTree, Topology, timestamp_based, LoggerInitFun),

    SinkPid ! finished,
    ok.


%% TODO: Maybe also parametrize the b heartbeat ratio
distributed_experiment(NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(?MODULE, distributed_experiment_conf, 
			 [SinkName, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio]),
    LoggerInitFun = 
	fun() ->
	        log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
	end,
    util:sink(LoggerInitFun).

distributed_experiment_conf(SinkPid, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio) ->
    io:format("Args:~n~p~n", [[SinkPid, NodeNames, RateMultiplier, RatioAB]]),

    %% We assume that the first node name is the B node
    [BNodeName|ANodeNames] = NodeNames,
    true = net_kernel:connect_node(hd(ANodeNames)),
    ok = net_kernel:setopts(hd(ANodeNames), [{delay_send, false}]),
    %% ok = net_kernel:setopts(hd(ANodeNames), [{sndbuf,87040},
    %% 					     {rcvbuf,87040}]),
    %% Options = [active ,
    %% 	       buffer ,
    %% 	       delay_send ,
    %% 	       deliver ,
    %% 	       dontroute ,
    %% 	       exit_on_close ,
    %% 	       header ,
    %% 	       high_msgq_watermark ,
    %% 	       high_watermark ,
    %% 	       keepalive ,
    %% 	       linger ,
    %% 	       low_msgq_watermark ,
    %% 	       low_watermark ,
    %% 	       mode ,
    %% 	       nodelay ,
    %% 	       packet ,
    %% 	       packet_size ,
    %% 	       pktoptions ,
    %% 	       priority ,
    %% 	       recbuf ,
    %% 	       reuseaddr ,
    %% 	       send_timeout ,
    %% 	       send_timeout_close ,
    %% 	       show_econnreset ,
    %% 	       sndbuf ,
    %% 	       tos ,
    %% 	       tclass ,
    %% 	       ttl ,
    %% 	       recvtos ,
    %% 	       recvtclass ,
    %% 	       recvttl ,
    %% 	       pktoptions ,
    %% 	       ipv6_v6only],
    %% io:format("Opts:~n~p~n", [net_kernel:getopts(hd(ANodeNames), Options)]),

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

    LogTriple = log_mod:make_num_log_triple(),    
    ConfTree = conf_gen:generate(Specification, Topology, LogTriple, optimizer_greedy),

    %% Set up where will the input arrive

    %% Input Streams
    {As, Bs} = parametrized_input_distr_example(NumberAs, RatioAB, HeartbeatBRatio),
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
    producer:make_producers(InputStreams, ConfTree, Topology, timestamp_based, LoggerInitFun),

    SinkPid ! finished,
    ok.

%% The specification of the computation
update({{a,_}, Ts, Value}, Sum, SendTo) ->
    %% This is here for debugging purposes
    %% io:format("log: ~p~n", [{self(), a, Value, Ts}]),
    %% SendTo ! {self(), a, Value, Ts},
    Sum + Value;
update({b, Ts, empty}, Sum, SendTo) ->
    SendTo ! {sum, {b, Ts, empty}, Sum},
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
    

%% The predicates
isA1({{a,1}, _, _}) -> true;
isA1(_) -> false.

isA2({{a,2}, _, _}) -> true;
isA2(_) -> false.

%% isB({b, _, _}) -> true;
%% isB(_) -> false.    

true_pred(_) -> true.


%% Some input examples
input_example() ->
    AllInputs = [gen_a(V) || V <- lists:seq(1, 1000)] ++ [gen_a(V) || V <- lists:seq(1002, 2000)],
    A1 = [Msg || Msg <- AllInputs, isA1(Msg)] ++ [{heartbeat, {{a,1},2005}}],
    A2 = [Msg || Msg <- AllInputs, isA2(Msg)] ++ [{heartbeat, {{a,2},2005}}],
    B = [{b, 1001, empty}, {b, 2001, empty}, {heartbeat, {b,2005}}],
    {A1, A2, B}.

make_as(Id, N, Step) ->
    [{{a,Id}, T, T} || T <- lists:seq(1, N, Step)]
	++ [{heartbeat, {{a,Id}, N + 1}}].

%% WARNING: The hearbeat ratio needs to be a divisor of RatioAB (Maybe not necessarily)
parametrized_input_distr_example(NumberAs, RatioAB, HeartbeatBRatio) ->
    LengthAStream = 1000000,
    As = [make_as(Id, LengthAStream, 1) || Id <- lists:seq(1, NumberAs)],

    LengthBStream = LengthAStream div RatioAB,
    %% Bs = [{b, RatioAB + (RatioAB * BT), empty} 
    %% 	  || BT <- lists:seq(0,LengthBStream)]
    %% 	++ [{heartbeat, {b,LengthAStream + 1}}],
    Bs = lists:flatten(
	   [[{heartbeat, {b, (T * RatioAB div HeartbeatBRatio) + (RatioAB * BT)}} 
	    || T <- lists:seq(0, HeartbeatBRatio - 1)] 
	   ++ [{b, RatioAB + (RatioAB * BT), empty}]
	   || BT <- lists:seq(0,LengthBStream)])
	++ [{heartbeat, {b,LengthAStream + 1}}],
    {As, Bs}.


big_input_distr_example() ->
    A1 = lists:flatten(
	   [[{{a,1}, T + (1000 * BT), T + (1000 * BT)} 
	     || T <- lists:seq(1, 999, 2)]
	    || BT <- lists:seq(0,1000)])
	++ [{heartbeat, {{a,1},1000000}}], 

    A2 = lists:flatten(
	   [[{{a,2}, T + (1000 * BT), T + (1000 * BT)} 
	     || T <- lists:seq(2, 998, 2)]
	    || BT <- lists:seq(0,1000)])
	++ [{heartbeat, {{a,2},1000000}}],

    Bs = [{b, 1000 + (1000 * BT), empty} 
	  || BT <- lists:seq(0,1000)]
	++ [{heartbeat, {b,1000000}}],
    {A1, A2, Bs}.

complex_input_distr_example() ->
    {A1, A2, Bs} = big_input_distr_example(),
    A3 = lists:flatten(
	   [[{{a,3}, T + (1000 * BT), T + (1000 * BT)} 
	     || T <- lists:seq(1, 999, 2)]
	    || BT <- lists:seq(1,1000)])
	++ [{heartbeat, {{a,3},10000000}}], 
    
    A4 = lists:flatten(
	   [[{{a,4}, T + (1000 * BT), T + (1000 * BT)} 
	     || T <- lists:seq(2, 998, 2)]
	    || BT <- lists:seq(1,1000)])
	++ [{heartbeat, {{a,4},10000000}}],
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
    {{a, Id}, V, V}.

input_example2() ->
    A1 = [{{a,1}, 1, 1},
	  {{a,1}, 3, 5},
	  {{a,1}, 4, 3},
	  {heartbeat, {{a,1}, 5}},
	  {{a,1}, 7, 7},
	  {heartbeat, {{a,1}, 10}},
	  {{a,1}, 15, 3},
	  {heartbeat, {{a,1}, 20}}],
    A2 = [{heartbeat, {{a,2}, 5}},
	  {{a,2}, 6, 6},
	  {{a,2}, 8, 5},	  
	  {{a,2}, 10, 6},	  
	  {heartbeat, {{a,2}, 10}},
	  {{a,2}, 11, 5},
	  {{a,2}, 12, 1},
	  {{a,2}, 13, 0},
	  {{a,2}, 14, 9},
	  {heartbeat, {{a,2}, 20}}],
    B = [{b, 2, empty},
	  {b, 5, empty},
	  {heartbeat, {b, 7}},
	  {b, 9, empty},
	  {heartbeat, {b, 9}},
	  {b, 16, empty},
	  {heartbeat, {b, 20}}],
    {A1, A2, B}.

%% -------- TESTS -------- %%

input_example_output() ->
    [{sum,{b,1001,empty},500500},
     {sum,{b,2001,empty},1999999}].

input_example_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf_1}, input_example_output()))
      end} || _ <- Rounds]}.

input_example2_output() ->
    [{sum,{b,2,empty},1},
     {sum,{b,5,empty},9},
     {sum,{b,9,empty},27},
     {sum,{b,16,empty},51}].

input_example2_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example2 test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf}, input_example2_output()))
      end} || _ <- Rounds]}.

