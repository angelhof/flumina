-module(abexample).

-export([main/0, node/2]).

%% This is the dataflow graph of this simple program.
%% It has to be created from the end so that Pids
%% can be given to the nodes

main() ->
    distributed().

sequential() ->
    Input = input_example(),
    Processor = spawn_link(?MODULE, node, [0, self()]),
    Producer = producer:main(Input, Processor),
    output().

distributed() ->
    %% Create the nodes
    ProcA1 = node:node(0, fun isA/1, [], {fun update/3, fun split/1, fun merge/2}, self()),
    ProcA2 = node:node(0, fun isA/1, [], {fun update/3, fun split/1, fun merge/2}, self()),
    ProcB = node:node(0, fun isB/1, [ProcA1, ProcA2], {fun update/3, fun split/1, fun merge/2}, self()),
    %% Create the configuration tree and initialize the router
    LeafA1 = {node, ProcA1, fun isA/1, []},
    LeafA2 = {node, ProcA2, fun isA/1, []},
    Tree = {node, ProcB, fun isB/1, [LeafA1, LeafA2]},
    router:init(Tree),

    Input = input_example2(),
    Producer = producer:main(Input, ProcB),
    io:format("Prod: ~p, B: ~p, A1: ~p, A2: ~p~n", [Producer, ProcB, ProcA1, ProcA2]),
    output().

%% The computation
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

node(Sum, SendTo) ->
    receive
	{msg, Msg} ->
	    Sum1 = update(Msg, Sum, SendTo),
	    node(Sum1, SendTo)
    end.	    



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

%% main(File, SendTo) ->
%%     {ok, Contents} = file:read_file(File),
%%     Lines = string:split(Contents, "\n"),
%%     Messages = [parse_line(Line) || Line <- Lines],
%%     loop(Messages, SendTo).

%% parse_line(Line) ->
%%     ok.
