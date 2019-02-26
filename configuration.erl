-module(configuration).

-export([create/3,
	 find_node/2,
	 find_children/2,
	 find_children_mbox_pids/2,
	 find_children_node_pids/2,
	 find_children_preds/2,
	 find_descendant_preds/2,
	 find_node_mailbox_pid_pairs/1,
	 find_node_mailbox_father_pid_pairs/1,
	 get_relevant_predicates/2]).

-include("type_definitions.hrl").

%%
%% This function creates the configuration
%% from a tree specification.
%% - It spawns and creates the nodes
%% - It initializes the router
%%

-spec create(temp_setup_tree(), dependencies(), pid()) -> pid_tree().
create(Tree, Dependencies, OutputPid) ->

    %% Spawns the nodes
    PidsTree = spawn_nodes(Tree, Dependencies, OutputPid),

    %% Create the configuration tree
    ConfTree = prepare_configuration_tree(PidsTree, Tree),
    io:format("Configuration:~n~p~n", [ConfTree]),

    %% Send the configuration tree to all nodes' mailboxes
    send_conf_tree(ConfTree, PidsTree),
    
    PidsTree.

%% Spawns the nodes based on the tree configuration
-spec spawn_nodes(temp_setup_tree(), dependencies(), pid()) -> pid_tree().
spawn_nodes({State, Pred, Funs, Children}, Dependencies, OutputPid) ->
    ChildrenPidTrees = [spawn_nodes(C, Dependencies, OutputPid) || C <- Children],
    ChildrenPids = [MP || {{_NP, MP}, _} <- ChildrenPidTrees],
    {NodePid, MailboxPid} = node:node(State, Pred, Funs, Dependencies, OutputPid),
    {{NodePid, MailboxPid}, ChildrenPidTrees}.

    
%% Prepares the router tree
-spec prepare_configuration_tree(pid_tree(), temp_setup_tree()) -> configuration().
prepare_configuration_tree({{NodePid, MailboxPid}, ChildrenPids}, {State, Pred, Funs, Children}) ->
    ChildrenTrees = [prepare_configuration_tree(P, N) || {P, N} <- lists:zip(ChildrenPids, Children)],
    {node, NodePid, MailboxPid, Pred, ChildrenTrees}.

%% Sends the conf tree to all children in a Pid tree
-spec send_conf_tree(configuration(), pid_tree()) -> ok.
send_conf_tree(ConfTree, {{_NodePid, MailboxPid}, ChildrenPids}) ->
    MailboxPid ! {configuration, ConfTree},
    [send_conf_tree(ConfTree, CP) || CP <- ChildrenPids],
    ok.



%%
%% Functions to use the configuration tree
%% WARNING: They are implemented in a naive way
%%          searching again and again top down
%%          in the tree.
%%

%% This function finds a node in the configuration tree
-spec find_node(pid(), configuration()) -> configuration().
find_node(Pid, ConfTree) ->
    [Node] = find_node0(Pid, ConfTree),
    Node.

-spec find_node0(pid(), configuration()) -> [configuration()].
find_node0(Pid, {node, Pid, _MboxPid,  _Pred, _Children} = Node) ->
    [Node];
find_node0(Pid, {node, _Pid, _MboxPid, _Pred, Children}) ->
    lists:flatten([find_node0(Pid, CN) || CN <- Children]).

%% This function returns the children of some node in the pid tree
-spec find_children(pid(), configuration()) -> [configuration()].
find_children(Pid, ConfTree) ->
    {node, Pid, _MboxPid, _Pred, Children} = find_node(Pid, ConfTree),
    Children.

%% This function returns the pids of the children nodes of a node in the tree
-spec find_children_mbox_pids(pid(), configuration()) -> [pid()].
find_children_mbox_pids(Pid, ConfTree) ->
    [MPid || {node, _, MPid,  _, _} <- find_children(Pid, ConfTree)].

-spec find_children_node_pids(pid(), configuration()) -> [pid()].
find_children_node_pids(Pid, ConfTree) ->
    [NPid || {node, NPid, _,  _, _} <- find_children(Pid, ConfTree)].

%% This function returns the predicates of the pids of the children nodes
-spec find_children_preds(pid(), configuration()) -> [message_predicate()].
find_children_preds(Pid, ConfTree) ->
    [CPred || {node, _, _, CPred, _} <- find_children(Pid, ConfTree)].

%% This function returns the predicates of the pids of all the descendant nodes
-spec find_descendant_preds(pid(), configuration()) -> [message_predicate()].
find_descendant_preds(Pid, ConfTree) ->
    ChildrenDescendants = 
	lists:flatten(
	  [find_descendant_preds(CPid, ConfTree) 
	   || CPid <- find_children_node_pids(Pid, ConfTree)]),
    ChildrenPreds = find_children_preds(Pid, ConfTree),
    ChildrenPreds ++ ChildrenDescendants.

%% Returns a list with all the pairs of node and mailbox pids
-spec find_node_mailbox_pid_pairs(configuration()) -> [{pid(), pid()}].
find_node_mailbox_pid_pairs({node, NPid, MPid, _Pred, Children}) ->
    ChildrenPairs = lists:flatten([find_node_mailbox_pid_pairs(C) || C <- Children]),
    [{NPid, MPid}|ChildrenPairs].

-spec union_children_preds([configuration()]) -> message_predicate().
union_children_preds(Children) ->
    fun(Msg) ->
	    lists:any(
	      fun({node, _N, _M, Pred, _C}) ->
		      Pred(Msg)
	      end, Children)
    end.

-spec is_found({'found' | 'not_found', message_predicate()}) -> boolean().
is_found({found, _}) -> true;
is_found(_) -> false.

-spec get_relevant_predicates(pid(), configuration()) -> {'found' | 'not_found', message_predicate()}.
get_relevant_predicates(Attachee, {node, Attachee, _MPid, Pred, Children}) ->
    ChildrenPred = union_children_preds(Children),
    ReturnPred =
	fun(Msg) ->
		Pred(Msg) andalso not ChildrenPred(Msg)
	end,
    {found, ReturnPred};
get_relevant_predicates(Attachee, {node, _NotAttachee, _MPid, Pred, Children}) ->
    ChildrenPredicates = [get_relevant_predicates(Attachee, C) || C <- Children],
    case lists:partition(fun(C) -> is_found(C) end, ChildrenPredicates) of
	{[], _} ->
	    %% No child matches the Attachee pid in this subtree
	    {not_found, Pred};
	{[{found, ChildPred}], Rest} ->
	    %% One child matches thhe Attachee pid
	    ReturnPred =
		fun(Msg) ->
			%% My child's pred, or my predicate without the other children predicates
			ChildPred(Msg) 
			    orelse (Pred(Msg) andalso 
				    not lists:any(fun({not_found, Pr}) -> Pr(Msg) end, Rest))
		end,
	    {found, ReturnPred}
    end.

%% It returns the pairs of mailbox and father ids
-spec find_node_mailbox_father_pid_pairs(configuration()) -> [{pid(), pid() | 'undef'}].
find_node_mailbox_father_pid_pairs({node, NPid, MPid, _Pred, Children}) ->
    ChildrenPairs = lists:flatten([find_node_mailbox_father_pid_pairs(C) || C <- Children]),
    [{MPid, undef}|[add_father_if_undef(ChildPair, NPid) || ChildPair <- ChildrenPairs]].
    
-spec add_father_if_undef({pid(), pid() | 'undef'}, pid()) -> {pid(), pid()}.
add_father_if_undef({ChildMPid, undef}, Father) -> {ChildMPid, Father};
add_father_if_undef(ChildPair, _Father) -> ChildPair.
    
