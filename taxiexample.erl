-module(taxiexample).

-export([main/0,
	 source/2]).

main() ->
    distributed().

%% Note:
%% =====
%% At the moment we assume that everything written in this module
%% is correct. Normally we would typecheck the specification of
%% the computation but for now we can assume that it is correct.

%% This is what our compiler would come up with
distributed() ->

    %% Configuration Tree
    Funs = {fun update/3, fun split/1, fun merge/2},
    Node  = {0, fun true_pred/1, Funs, []},
    PidTree = configuration:create(Node, dependencies(), self()),
    {HeadPid, _} = PidTree,

    %% Set up where will the input arrive
    Input1 = id1_input_with_heartbeats(),
    Producer1 = spawn_link(?MODULE, source, [Input1, HeadPid]),

    Input2 = id2_input_with_heartbeats(),
    Producer2 = spawn_link(?MODULE, source, [Input2, HeadPid]),

    Input3 = hour_markets_input(),
    Producer3 = spawn_link(?MODULE, source, [Input3, HeadPid]),

    %% io:format("Prod: ~p~nTree: ~p~n", [Producer, PidTree]),
    sink().

%% The specification of the computation
update({Tag, Ts, Value}, Sum, SendTo) ->
    %% This is here for debugging purposes
    SendTo ! {"Time", Ts, Tag, Value},
    Sum.

merge(Sum1, Sum2) ->
    Sum1 + Sum2.

split(Sum) ->
    {Sum, 0}.

dependencies() ->
    #{id1 => [hour],
      id2 => [hour],
      hour => [id1, id2, hour]
     }.

%% The predicates
isA({a, _, _}) -> true;
isA(_) -> false.

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

sink() ->
    receive
	Msg ->
	    io:format("~p~n", [Msg]),
	    sink()
    end.


%% Some input examples

hour_markets_input() ->
    Input = [{hour, T * 60, marker} || T <- lists:seq(1, 10)],
    producer:interleave_heartbeats(Input, #{hour => 60}, 500).

id1_input_with_heartbeats() ->
    producer:interleave_heartbeats(id1_input(), #{id1 => 10}, 500).

id2_input_with_heartbeats() ->
    producer:interleave_heartbeats(id2_input(), #{id2 => 10}, 500).

id1_input() ->
    Inputs = 
	[{1, 15},
	 {10, 20},
	 {14, 28},
	 {25, 10},
	 {45, 21},
	 {75, 15},
	 {100, 23},
	 {121, 10},
	 {150, 34},
	 {174, 12},
	 {210, 21},
	 {234, 15},
	 {250, 12}],
    [{id1, Ts, Tip} || {Ts, Tip} <- Inputs].

id2_input() ->
    Inputs = 
	[{5, 25},
	 {32, 26},
	 {41, 10},
	 {53, 24},
	 {59, 30},
	 {71, 15},
	 {84, 29},
	 {103, 21},
	 {125, 18},
	 {156, 12},
	 {189, 21},
	 {195, 15},
	 {210, 18},
	 {231, 12},
	 {245, 21},
	 {268, 15},
	 {290, 12}],
    [{id2, Ts, Tip} || {Ts, Tip} <- Inputs].    

