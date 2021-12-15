-module(router).

-export([init/1,
	 loop/1,
	 or_route/2,
	 and_route/2,
	 heartbeat_route/2,
         find_responsible_subtree_child_father_pids/2,
	 find_responsible_subtree_pids/2,
         find_responsible_subtree_root/2
	]).

-include("type_definitions.hrl").
-include("config.hrl").

%% This is the router process that
%% holds the configuration tree in its state
%% and knows which node has to receive which message

%% WARNING/TODO:
%% At the moment it is implemented as a global process but that clearly is not efficient
%% so it has to be implemented in some other way.

%% Interface
%% -spec or_route(message(), configuration()) ->
or_route(Msg, Tree) ->
    find_one_responsible(Tree, Msg).

and_route(Router, Msg) ->
    Router ! {self(), {and_route, Msg}},
    receive
	{ok, Pids} ->
	    Pids
    end.

-spec heartbeat_route(gen_impl_message(), configuration()) -> [mailbox()].
heartbeat_route(Msg, Tree) ->
    find_responsibles(Tree, Msg).
    %% Subtrees = find_lowest_responsible_subtrees(Tree, Msg),
    %% lists:flatmap(fun configuration:find_node_mailbox_pids/1, Subtrees).

-spec init(configuration()) -> pid().
init(Tree) -> 
    Router = spawn_link(?MODULE, loop, [Tree]),
    true = register(router, Router),
    Router.

%% Internal Function
-spec loop(configuration()) -> no_return().
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

%% This function returns the subtree pids together with their father's
%% Node! pid. This is used for state exchange at forks-joins, when
%% nodes talk directly without their mailboxes.
-spec find_responsible_subtree_child_father_pids(configuration(), gen_impl_message())
                                                -> {{mailbox(), 'root'},
                                                    [{mailbox(), NodeFatherName::mailbox()}]}.
find_responsible_subtree_child_father_pids(Tree, Msg) ->
    Subtree = find_responsible_subtree(Tree, Msg),
    configuration:find_node_mailbox_father_pid_pairs(Subtree).

-spec find_responsible_subtree_pids(configuration(), gen_impl_message())
				   -> [mailbox()].
find_responsible_subtree_pids(Tree, Msg) ->
    Subtree = find_responsible_subtree(Tree, Msg),
    {Root, Rest} = configuration:find_node_mailbox_father_pid_pairs(Subtree),
    [Child || {Child, _Father} <- [Root|Rest]].


-spec find_responsible_subtree_root(configuration(), gen_impl_message())
				   -> mailbox().
find_responsible_subtree_root(Tree, Msg) ->
    [SendTo|_] = find_responsible_subtree_pids(Tree, Msg),
    SendTo.


%% This functions finds and returns **ONE OF** the responsible
%% subtrees for this message
-spec find_responsible_subtree(configuration(), gen_impl_message()) -> configuration().
find_responsible_subtree(Tree, Msg) ->
    %% [ResponsibleSubtree] = find_lowest_responsible_subtrees(Tree, Msg),
    %% ResponsibleSubtree.
    %% TODO: Change that to only give one subtree.
    ResponsibleSubtrees = find_lowest_responsible_subtrees(Tree, Msg),
    %% WARNING: Extremely Inefficient
    Index = rand:uniform(length(ResponsibleSubtrees)),
    lists:nth(Index,ResponsibleSubtrees).

%% This is a very bad find_responsible implementation
%% because it locally finds all responsible nodes
%% Also it only finds the or-split responsible nodes
-spec find_one_responsible(configuration(), gen_impl_message()) -> mailbox().
find_one_responsible(Tree, Msg) ->
    Responsibles = find_lowest_responsibles(Tree, Msg),
    %% Extremely Inefficient
    Index = rand:uniform(length(Responsibles)),
    lists:nth(Index,Responsibles).

%% This is used to find the first lower responsibles for this message,
%% so the lowest nodes that can process it.
-spec find_lowest_responsibles(configuration(), gen_impl_message()) -> [mailbox()].
find_lowest_responsibles(Tree, Msg) ->
    [MboxNameNode || {node, _NP, _NNN, MboxNameNode, _Preds, _Cs} <- find_lowest_responsible_subtrees(Tree, Msg)].

-spec find_lowest_responsible_subtrees(configuration(), gen_impl_message()) -> [configuration()].
find_lowest_responsible_subtrees({node, _NodePid, _NNN, _MailboxPid, {_SpecPred, Pred}, Children} = Node, Msg) ->
    case lists:flatmap(fun(C) -> find_lowest_responsible_subtrees(C, Msg) end, Children) of
	[] ->
	    %% None of my children are responsible so I could be
	    case Pred(Msg) of
		true -> [Node];
		false -> []
	    end;
	Responsibles ->
	    Responsibles
    end.

-spec find_responsibles(configuration(), gen_impl_message()) -> [mailbox()].
find_responsibles({node, _NodePid, _NNN, MboxNameNode, {_SpecPred, Pred}, Children}, Msg) ->
    ChildrenResponsibles = lists:flatmap(fun(C) -> find_responsibles(C, Msg) end, Children),
    case Pred(Msg) of
	true -> [MboxNameNode|ChildrenResponsibles];
	false -> ChildrenResponsibles
    end.


% -spec all_pids(configuration()) -> [mailbox()].
% all_pids({node, _NodePid, _NNN, MboxNameNode, _, Children}) ->
%     [MboxNameNode|lists:flatmap(fun all_pids/1, Children)].
