-module(abexample).

-export([distributed/0,
	 distributed_conf/1,
	 distributed_1/0,
	 distributed_conf_1/1,
	 real_distributed/0,
	 real_distributed_conf/1,
	 seq_big/0,
	 seq_big_conf/1,
	 distr_big/0,
	 distr_big_conf/1,
	 source/2]).

-include_lib("eunit/include/eunit.hrl").

-include("type_definitions.hrl").

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

seq_big() ->
    _ExecPid = spawn_link(?MODULE, seq_big_conf, [self()]),
    util:sink().

seq_big_conf(SinkPid) ->
    %% Configuration Tree
    Funs = {fun update/3, fun util:crash/2, fun util:crash/2},
    Node  = {0, {'proc', node()}, fun true_pred/1, Funs, []},
    PidTree = configuration:create(Node, dependencies(), SinkPid),
    {{_HeadNodePid, HeadMailboxPid}, _} = PidTree,

    %% Set up where will the input arrive
    Input = big_input_example(),
    _Producer1 = spawn_link(?MODULE, source, [Input, HeadMailboxPid]),

    SinkPid ! finished.

distr_big() ->
    ExecPid = spawn_link(?MODULE, distr_big_conf, [self()]),
    util:sink().

distr_big_conf(SinkPid) ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, {'proc_a1', node()}, fun isA1/1, Funs, []},
    NodeA2 = {0, {'proc_a2', node()}, fun isA2/1, Funs, []},
    NodeB  = {0, {'proc_b', node()}, fun true_pred/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    {A1, A2, Bs} = big_input_distr_example(),
    {{_HeadNodePid, HeadMailboxPid},
     [{{_NP1, MPA1}, []}, 
      {{_NP2, MPA2}, []}]} = PidTree,

    BsProducer = spawn_link(node(), ?MODULE, source, [Bs, HeadMailboxPid]),

    A1producer = spawn_link(node(), ?MODULE, source, [A1, MPA1]),
    A2producer = spawn_link(node(), ?MODULE, source, [A2, MPA2]),

    SinkPid ! finished.



%% This is what our compiler would come up with
distributed() ->
    ExecPid = spawn_link(?MODULE, distributed_conf, [self()]),
    util:sink().

distributed_conf(SinkPid) ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, {'proc_a1', node()}, fun isA1/1, Funs, []},
    NodeA2 = {0, {'proc_a2', node()}, fun isA2/1, Funs, []},
    NodeB  = {0, {'proc_b', node()}, fun true_pred/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    Input = input_example2(),
    {{_HeadNodePid, HeadMailboxPid}, _} = PidTree,
    Producer = spawn_link(?MODULE, source, [Input, HeadMailboxPid]),

    io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
    SinkPid ! finished,
    ok.

distributed_1() ->
    ExecPid = spawn_link(?MODULE, distributed_conf_1, [self()]),
    util:sink().

distributed_conf_1(SinkPid) ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, {'proc_a1', node()}, fun isA1/1, Funs, []},
    NodeA2 = {0, {'proc_a2', node()}, fun isA2/1, Funs, []},
    NodeB  = {0, {'proc_b', node()}, fun true_pred/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    Input = input_example(),
    {{_HeadNodePid, HeadMailboxPid}, _} = PidTree,
    Producer = spawn_link(?MODULE, source, [Input, HeadMailboxPid]),

    %% io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
    SinkPid ! finished,
    ok.

real_distributed() ->
    ExecPid = spawn_link(?MODULE, real_distributed_conf, [self()]),
    util:sink().

real_distributed_conf(SinkPid) ->

    A1NodeName = 'a1node@Work-PC',
    A2NodeName = 'a2node@Work-PC',

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, {'proc_a1', A1NodeName}, fun isA1/1, Funs, []},
    NodeA2 = {0, {'proc_a2', A2NodeName}, fun isA2/1, Funs, []},
    NodeB  = {0, {'proc_b', node()}, fun true_pred/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    {{_HeadNodePid, HeadMailboxPid},
     [{{_NP1, MPA1}, []}, 
      {{_NP2, MPA2}, []}]} = PidTree,

    BsInput = bs_input_example(),
    {A1input, A2input} = as_input_example(),
    BsProducer = spawn_link(node(), ?MODULE, source, [BsInput, HeadMailboxPid]),
    A1producer = spawn_link(A1NodeName, ?MODULE, source, [A1input, MPA1]),
    A2producer = spawn_link(A2NodeName, ?MODULE, source, [A2input, MPA2]),


    io:format("Prod: ~p~nTree: ~p~n", [[BsProducer, A1producer, A2producer], PidTree]),
    SinkPid ! finished,
    ok.
    

%% The specification of the computation
update({{a,_}, Ts, Value}, Sum, SendTo) ->
    %% This is here for debugging purposes
    %% SendTo ! {self(), a, Value, Ts},
    Sum + Value;
update({b, Ts, empty}, Sum, SendTo) ->
    SendTo ! {sum, Sum, Ts},
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

%% The predicates
isA1({{a,1}, _, _}) -> true;
isA1(_) -> false.

isA2({{a,2}, _, _}) -> true;
isA2(_) -> false.

isB({b, _, _}) -> true;
isB(_) -> false.    

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
input_example() ->
    [gen_a(V) || V <- lists:seq(1, 1000)] ++ [{b, 1001, empty}]
	++ [gen_a(V) || V <- lists:seq(1002, 2000)] ++ [{b, 2001, empty}]
	++ [{heartbeat, {{a,1},2005}}, {heartbeat, {{a,2},2005}}, {heartbeat, {b,2005}}].

big_input_example() ->
    lists:flatten(
      [[gen_a(T + (1000 * BT)) || T <- lists:seq(1, 999) ] ++ [{b, 1000 + (1000 * BT), empty}]
		  || BT <- lists:seq(1,1000)]) 
	++ [{heartbeat, {{a,1},10000000}}, 
	    {heartbeat, {{a,2},10000000}}, 
	    {heartbeat, {b,10000000}}].

big_input_distr_example() ->
    A1 = lists:flatten(
	   [[{{a,1}, T + (1000 * BT), T + (1000 * BT)} 
	     || T <- lists:seq(1, 999, 2)]
	    || BT <- lists:seq(1,1000)])
	++ [{heartbeat, {{a,1},10000000}}], 

    A2 = lists:flatten(
	   [[{{a,2}, T + (1000 * BT), T + (1000 * BT)} 
	     || T <- lists:seq(2, 998, 2)]
	    || BT <- lists:seq(1,1000)])
	++ [{heartbeat, {{a,2},10000000}}],

    Bs = [{b, 1000 + (1000 * BT), empty} || BT <- lists:seq(1,1000)]
	++ [{heartbeat, {b,10000000}}],
    {A1, A2, Bs}.
    

bs_input_example() ->
    [{b, 1001, empty},
     {b, 2001, empty},
     {heartbeat, {b,2005}}].

as_input_example() ->
    AllAs = 
	[gen_a(V) || V <- lists:seq(1, 1000, 2)] ++ 
	[gen_a(V) || V <- lists:seq(1002, 2000, 2)],
    {A1s, A2s} = lists:partition(fun({{a,Id},_,_}) -> Id =:= 1 end, AllAs),
    {A1s ++ [{heartbeat, {{a,1},2005}}], 
     A2s ++ [{heartbeat, {{a,2},2005}}]}. 

gen_a(V) ->
    Id = random:uniform(2),
    {{a, Id}, V, V}.

input_example2() ->
    [{{a,1}, 1, 1},
     {b, 2, empty},
     {{a,1}, 3, 5},
     {{a,1}, 4, 3},
     {b, 5, empty},
     {heartbeat, {{a,1}, 5}},
     {heartbeat, {{a,2}, 5}},
     {{a,2}, 6, 6},
     {{a,1}, 7, 7},
     {{a,2}, 8, 5},
     {heartbeat, {b, 7}},
     {b, 9, empty},
     {{a,2}, 10, 6},
     {heartbeat, {b, 9}},
     {heartbeat, {{a,2}, 10}},
     {heartbeat, {{a,1}, 10}},
     {{a,2}, 11, 5},
     {{a,2}, 12, 1},
     {{a,2}, 13, 0},
     {{a,2}, 14, 9},
     {{a,1}, 15, 3},
     {b, 16, empty},
     {heartbeat, {{a,1}, 20}},
     {heartbeat, {{a,2}, 20}},
     {heartbeat, {b, 20}}].

%% -------- TESTS -------- %%

input_example_output() ->
    [{sum,500500,1001},
     {sum,1999999,2001}].

input_example_test_() ->
    Rounds = lists:seq(1,100),
    Names = ['proc_a1', 'proc_a2', 'proc_b'],
    {"Input example test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> util:unregister_names(Names) end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf_1}, input_example_output()))
      end} || _ <- Rounds]}.

input_example2_output() ->
    [{sum,1,2},
     {sum,9,5},
     {sum,27,9},
     {sum,51,16}].

input_example2_test_() ->
    Rounds = lists:seq(1,100),
    Names = ['proc_a1', 'proc_a2', 'proc_b'],
    {"Input example2 test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> util:unregister_names(Names) end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf}, input_example2_output()))
      end} || _ <- Rounds]}.

