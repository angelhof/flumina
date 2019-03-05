-module(abexample).

-export([distributed/0,
	 distributed_conf/1,
	 distributed_1/0,
	 distributed_conf_1/1,
	 source/2]).

-include_lib("eunit/include/eunit.hrl").

-include("type_definitions.hrl").

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

%% This is what our compiler would come up with
distributed() ->
    ExecPid = spawn_link(?MODULE, distributed_conf, [self()]),
    util:sink().

distributed_conf(SinkPid) ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/2, fun merge/2},
    NodeA1 = {0, fun isA1/1, Funs, []},
    NodeA2 = {0, fun isA2/1, Funs, []},
    NodeB  = {0, fun true_pred/1, Funs, [NodeA1, NodeA2]},
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
    NodeA1 = {0, fun isA1/1, Funs, []},
    NodeA2 = {0, fun isA2/1, Funs, []},
    NodeB  = {0, fun true_pred/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, dependencies(), SinkPid),

    %% Set up where will the input arrive
    Input = input_example(),
    {{_HeadNodePid, HeadMailboxPid}, _} = PidTree,
    Producer = spawn_link(?MODULE, source, [Input, HeadMailboxPid]),

    %% io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
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
    {"Input example test",
     [?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf_1}, input_example_output()))
      || _ <- Rounds]}.

input_example2_output() ->
    [{sum,1,2},
     {sum,9,5},
     {sum,27,9},
     {sum,51,16}].

input_example2_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example2 test",
     [?_assertEqual(ok, testing:test_mfa({?MODULE, distributed_conf}, input_example2_output()))
      || _ <- Rounds]}.		   

