-module(abexample).

-export([main/0]).

main() ->
    distributed().

%% This is what our compiler would come up with
distributed() ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/1, fun merge/2},
    NodeA1 = {0, fun isA/1, Funs, []},
    NodeA2 = {0, fun isA/1, Funs, []},
    NodeB  = {0, fun isB/1, Funs, [NodeA1, NodeA2]},
    PidTree = configuration:create(NodeB, self()),

    %% Set up where will the input arrive
    Input = input_example2(),
    {HeadPid, _} = PidTree,
    Producer = producer:main(Input, HeadPid),

    io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
    output().

%% The specification of the computation
update({a, Value, Ts}, Sum, SendTo) ->
    SendTo ! {self(), a, Value, Ts},
    Sum + Value;
update({b, _Ts}, Sum, SendTo) ->
    SendTo ! Sum,
    Sum.

merge(Sum1, Sum2) ->
    Sum1 + Sum2.

split(Sum) ->
    {Sum, 0}.


%% The predicates
isA({a, _, _}) -> true;
isA(_) -> false.

isB({b, _}) -> true;
isB(_) -> false.    

output() ->
    receive
	Msg ->
	    io:format("~p~n", [Msg]),
	    output()
    end.


%% Some input examples
input_example() ->
    [{a, V, V} || V <- lists:seq(1, 1000)] ++ [{b, 1001}]
	++ [{a, V, V} || V <- lists:seq(1002, 2000)] ++ [{b, 2001}].

input_example2() ->
    [{a, 1, 1},
     {b, 2},
     {a, 5, 3},
     {a, 3, 4},
     {b, 5},
     {a, 6, 6},
     {a, 7, 7},
     {a, 5, 8},
     {b, 9},
     {a, 6, 10},
     {a, 5, 11},
     {a, 1, 12},
     {a, 0, 13},
     {a, 9, 14},
     {a, 3, 15},
     {b, 16}].
