-module(abexample).

-export([main/0,
	 source/2]).

main() ->
    distributed().

%% This is what our compiler would come up with
distributed() ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/1, fun merge/2},
    NodeA1 = {0, fun isA/1, Funs, []},
    NodeA2 = {0, fun isA/1, Funs, []},
    NodeB  = {0, fun isB/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, dependencies(), self()),

    %% Set up where will the input arrive
    Input = input_example2(),
    {HeadPid, _} = PidTree,
    Producer = spawn_link(?MODULE, source, [Input, HeadPid]),

    io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
    sink().

%% The specification of the computation
update({a, Ts, Value}, Sum, SendTo) ->
    %% This is here to see 
    SendTo ! {self(), a, Value, Ts},
    Sum + Value;
update({b, Ts, empty}, Sum, SendTo) ->
    SendTo ! {sum, Sum, Ts},
    Sum.

merge(Sum1, Sum2) ->
    Sum1 + Sum2.

split(Sum) ->
    {Sum, 0}.

dependencies() ->
    #{a => [b],
      b => [a, b]
     }.

%% The predicates
isA({a, _, _}) -> true;
isA(_) -> false.

isB({b, _, _}) -> true;
isB(_) -> false.    

%% Source and Sink

source([], _SendTo) ->
    ok;
source([Msg|Rest], SendTo) ->
    case Msg of
	{heartbeat, _} ->
	    SendTo ! Msg;
	_ ->
	    SendTo ! {msg, Msg}
    end,
    source(Rest, SendTo).

sink() ->
    receive
	Msg ->
	    io:format("~p~n", [Msg]),
	    sink()
    end.


%% Some input examples
input_example() ->
    [{a, V, V} || V <- lists:seq(1, 1000)] ++ [{b, 1001, empty}]
	++ [{a, V, V} || V <- lists:seq(1002, 2000)] ++ [{b, 2001, empty}].

input_example2() ->
    [{a, 1, 1},
     {b, 2, empty},
     {a, 3, 5},
     {a, 4, 3},
     {b, 5, empty},
     {heartbeat, {a, 5}},
     {a, 6, 6},
     {a, 7, 7},
     {a, 8, 5},
     {heartbeat, {b, 7}},
     {b, 9, empty},
     {a, 10, 6},
     {heartbeat, {b, 9}},
     {heartbeat, {a, 10}},
     {a, 11, 5},
     {a, 12, 1},
     {a, 13, 0},
     {a, 14, 9},
     {a, 15, 3},
     {b, 16, empty},
     {heartbeat, {a, 20}},
     {heartbeat, {b, 20}}].
