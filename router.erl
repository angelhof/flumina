-module(router).

-export([init/1, 
	 loop/1,
	 or_route/2,
	 and_route/2,
	 heartbeat_route/2
	]).

%% This is the router process that
%% holds the configuration tree in its state
%% and knows which node has to receive which message

%% WARNING/TODO:
%% At the moment it is implemented as a global process but that clearly is not efficient
%% so it has to be implemented in some other way.

%% Interface
or_route(Msg, Tree) ->
    find_one_responsible(Tree, Msg).

and_route(Router, Msg) ->
    Router ! {self(), {and_route, Msg}},
    receive
	{ok, Pids} ->
	    Pids
    end.

heartbeat_route(Msg, Tree) ->
    find_responsibles(Tree, Msg).

init(Tree) -> 
    Router = spawn_link(?MODULE, loop, [Tree]),
    register(router, Router),
    Router.

%% Internal Function

loop(Tree) ->
    receive
	{ReplyTo, {or_route, Msg}} ->
	    Pid = find_one_responsible(Tree, Msg),
	    ReplyTo ! {ok, Pid},
	    loop(Tree);
	{ReplyTo, {and_route, Msg}} ->
	    Pids = find_lowest_responsibles(Tree, Msg),
	    ReplyTo ! {ok, Pids},
	    loop(Tree);
	{ReplyTo, {heartbeat_route, Msg}} ->
	    Pids = find_responsibles(Tree, Msg),
	    ReplyTo ! {ok, Pids},
	    loop(Tree)
    end.


%% This is a very bad find_responsible implementation
%% because it locally finds all responsible nodes
%% Also it only finds the or-split responsible nodes
find_one_responsible(Tree, Msg) ->
    Responsibles = find_lowest_responsibles(Tree, Msg),
    %% Extremely Inefficient
    Index = rand:uniform(length(Responsibles)),
    lists:nth(Index,Responsibles).

%% This is used to find the first lower responsibles for this message,
%% so the lowest nodes that can process it.
find_lowest_responsibles({node, _NodePid, MailboxPid, Pred, Children}, Msg) ->
    case lists:flatmap(fun(C) -> find_lowest_responsibles(C, Msg) end, Children) of
	[] ->
	    %% None of my children are responsible so I could be
	    case Pred(Msg) of
		true -> [MailboxPid];
		false -> []
	    end;
	Responsibles ->
	    Responsibles
    end.

find_responsibles({node, _NodePid, MailboxPid, Pred, Children}, Msg) ->
    ChildrenResponsibles = lists:flatmap(fun(C) -> find_responsibles(C, Msg) end, Children),
    case Pred(Msg) of
	true -> [MailboxPid|ChildrenResponsibles];
	false -> ChildrenResponsibles
    end.

		    
all_pids({node, _NodePid, MailboxPid, _, Children}) ->
    [MailboxPid|lists:flatmap(fun all_pids/1, Children)].
