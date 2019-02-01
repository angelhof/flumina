-module(configuration).

-export([create/2]).

%%
%% This function creates the configuration
%% from a tree specification.
%% - It spawns and creates the nodes
%% - It initializes the router
%%
create(Tree, OutputPid) ->

    %% Spawns the nodes
    PidsTree = spawn_nodes(Tree, OutputPid),

    %% Create the configuration tree and initialize the router
    RouterTree = prepare_router_tree(PidsTree, Tree),
    router:init(RouterTree),

    PidsTree.

%% Spawns the nodes based on the tree configuration
spawn_nodes({State, Pred, Funs, Children}, OutputPid) ->
    ChildrenPidTrees = [spawn_nodes(C, OutputPid) || C <- Children],
    ChildrenPids = [P || {P, _} <- ChildrenPidTrees],
    MyPid = node:node(State, Pred, ChildrenPids, Funs, OutputPid),
    {MyPid, ChildrenPidTrees}.

    
%% Prepares the router tree
prepare_router_tree({Pid, ChildrenPids}, {State, Pred, Funs, Children}) ->
    ChildrenTrees = [prepare_router_tree(P, N) || {P, N} <- lists:zip(ChildrenPids, Children)],
    {node, Pid, Pred, ChildrenTrees}.
