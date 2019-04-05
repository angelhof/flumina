-module(configuration).

-export([create/3,
	 create/4,
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
-spec create(temp_setup_tree(), dependencies(), mailbox()) -> configuration().
create(Tree, Dependencies, OutputPid) ->
    LogTriple = log_mod:no_log_triple(),
    create(Tree, Dependencies, LogTriple, OutputPid).

-spec create(temp_setup_tree(), dependencies(), num_log_triple(), mailbox()) -> configuration().
create(Tree, Dependencies, LogTriple, OutputPid) ->

    %% Spawns the nodes
    NameSeed = make_name_seed(),
    {PidsTree, _NewNameSeed} = spawn_nodes(Tree, NameSeed, LogTriple, Dependencies, OutputPid),

    %% Create the configuration tree
    ConfTree = prepare_configuration_tree(PidsTree, Tree),
    io:format("Configuration:~n~p~n", [ConfTree]),

    %% Send the configuration tree to all nodes' mailboxes
    send_conf_tree(ConfTree, PidsTree),
    
    ConfTree.

%% Spawns the nodes based on the tree configuration
-spec spawn_nodes(temp_setup_tree(), name_seed(), num_log_triple(), 
		  dependencies(), mailbox()) -> {pid_tree(), name_seed()}.
spawn_nodes({State, Node, Pred, Funs, Children}, NameSeed, LogTriple, Dependencies, OutputPid) ->
    {ChildrenPidTrees, NewNameSeed} = 
	lists:foldr(
	 fun(C, {AccTrees, NameSeed0}) ->
		 {CTree, NameSeed1} =
		     spawn_nodes(C, NameSeed0, LogTriple, Dependencies, OutputPid),
		 {[CTree|AccTrees], NameSeed1}
	 end, {[], NameSeed}, Children),
    ChildrenPids = [MP || {{_NP, MP}, _} <- ChildrenPidTrees],
    {Name, FinalNameSeed} = gen_proc_name(NewNameSeed),
    {NodePid, NameNode} = node:node(State, {Name, Node}, Pred, Funs, LogTriple, Dependencies, OutputPid),
    {{{NodePid, NameNode}, ChildrenPidTrees}, FinalNameSeed}.

-spec make_name_seed() -> name_seed().
make_name_seed() ->
    make_name_seed(0).

-spec make_name_seed(name_seed()) -> name_seed().
make_name_seed(0) ->
    0.

-spec gen_proc_name(name_seed()) -> {atom(), name_seed()}.
gen_proc_name(Seed) ->
    {list_to_atom("proc_" ++ integer_to_list(Seed)), Seed + 1}.
    
%% Prepares the router tree
-spec prepare_configuration_tree(pid_tree(), temp_setup_tree()) -> configuration().
prepare_configuration_tree({{NodePid, {_, Node} = MboxNameNode}, ChildrenPids}, 
			   {State, Node, Pred, Funs, Children}) ->
    ChildrenTrees = [prepare_configuration_tree(P, N) || {P, N} <- lists:zip(ChildrenPids, Children)],
    {node, NodePid, MboxNameNode, Pred, ChildrenTrees}.

%% Sends the conf tree to all children in a Pid tree
-spec send_conf_tree(configuration(), pid_tree()) -> ok.
send_conf_tree(ConfTree, {{_NodePid, MailboxNameNode}, ChildrenPids}) ->
    MailboxNameNode ! {configuration, ConfTree},
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
-spec find_children_mbox_pids(pid(), configuration()) -> [mailbox()].
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
-spec find_node_mailbox_pid_pairs(configuration()) -> [{pid(), mailbox()}].
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

-spec is_acc({'acc' | 'rest', message_predicate()}) -> boolean().
is_acc({acc, _}) -> true;
is_acc(_) -> false.

-spec get_relevant_predicates(pid(), configuration()) -> {'ok', message_predicate()}.
get_relevant_predicates(Attachee, ConfTree) ->
    [{acc, RelevantPred}] =
	lists:filter(fun is_acc/1, get_relevant_predicates0(Attachee, ConfTree)),
    {ok, RelevantPred}.

-spec get_relevant_predicates0(pid(), configuration()) -> [{'acc' | 'rest', message_predicate()}].
get_relevant_predicates0(Attachee, {node, Attachee, _MPid, Pred, Children}) ->
    ChildrenPred = union_children_preds(Children),
    ReturnPred =
	fun(Msg) ->
		Pred(Msg) andalso not ChildrenPred(Msg)
	end,
    [{acc, ReturnPred}, {rest, Pred}];
get_relevant_predicates0(Attachee, {node, _NotAttachee, _MPid, Pred, Children}) ->
    ChildrenPredicates = 
	lists:flatten([get_relevant_predicates0(Attachee, C) || C <- Children]),
    case lists:partition(fun(C) -> is_acc(C) end, ChildrenPredicates) of
	{[], _} ->
	    %% No child matches the Attachee pid in this subtree
	    [{rest, Pred}];
	{[{acc, ChildPred}], Rest} ->
	    %% One child matches thhe Attachee pid
	    ReturnPred =
		fun(Msg) ->
			%% My child's pred, or my predicate without the other children predicates
			ChildPred(Msg) 
			    orelse 
			      (Pred(Msg) andalso 
			       not lists:any(fun({rest, Pr}) -> Pr(Msg) end, Rest))
		end,
	    [{acc, ReturnPred}, {rest, Pred}]
    end.

%% It returns the pairs of mailbox and father ids
-spec find_node_mailbox_father_pid_pairs(configuration()) -> [{mailbox(), mailbox() | 'undef'}].
find_node_mailbox_father_pid_pairs({node, _NPid, MboxNameNode, _Pred, Children}) ->
    ChildrenPairs = lists:flatten([find_node_mailbox_father_pid_pairs(C) || C <- Children]),
    [{MboxNameNode, undef}|[add_father_if_undef(ChildPair, MboxNameNode) || ChildPair <- ChildrenPairs]].
    
-spec add_father_if_undef({mailbox(), mailbox() | 'undef'}, mailbox()) -> {mailbox(), mailbox()}.
add_father_if_undef({ChildMPid, undef}, Father) -> {ChildMPid, Father};
add_father_if_undef(ChildPair, _Father) -> ChildPair.
    
