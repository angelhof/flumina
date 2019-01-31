-module(router).

-export([init/1, 
	 loop/1,
	 msg/2
	]).

%% This is the router process that
%% holds the configuration tree in its state
%% and knows which node has to receive which message

%% At the moment it is implemented as a global process but that clearly is not efficient
%% so it has to be implemented in some other way



%% Interface
msg(Router, Msg) ->
    Router ! {self(), {msg, Msg}},
    receive
	{ok, Pid} ->
	    Pid
    end.


init(Tree) -> 
    Router = spawn_link(?MODULE, loop, [Tree]),
    register(router, Router),
    Router.

%% Internal Function

loop(Tree) ->
    receive
	{ReplyTo, {msg, Msg}} ->
	    Pid = find_responsible(Tree, Msg),
	    ReplyTo ! {ok, Pid},
	    loop(Tree);
	{ReplyTo, {orsplit, Placeholder}} ->
	    %% TODO: To implement
	    ReplyTo ! ok;
	{ReplyTo, {merge, Placeholder}} ->
	    %% TODO: To implement
	    ReplyTo ! ok;
	{ReplyTo, {andsplit, Placeholder}} ->
	    %% TODO: To implement LATER
	    ReplyTo ! ok
    end.


%% This is a very bad find_responsible implementation
%% because it locally finds all responsible nodes
%% Also it only finds the or-split responsible nodes
find_responsible(Tree, Msg) ->
    Responsibles = find_responsibles(Tree, Msg),
    %% Extremely Inefficient
    Index = rand:uniform(length(Responsibles)),
    lists:nth(Index,Responsibles).

find_responsibles({node, Pid, Pred, Children}, Msg) ->
    case lists:flatmap(fun(C) -> find_responsibles(C, Msg) end, Children) of
	[] ->
	    %% None of my children are responsible so I could be
	    case Pred(Msg) of
		true -> [Pid];
		false -> []
	    end;
	Responsibles ->
	    Responsibles
    end.
		    
