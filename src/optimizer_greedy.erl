-module(optimizer_greedy).

-export([generate_setup_tree/2]).

-include("type_definitions.hrl").

%%
%% This is a very simple greedy optimizer. It sorts tags based on increasing
%% order of rates, and tries to remove tags in this order, until the dependency graph
%% becomes disconnected. When it becomes disconnected, it tries to find a sequence of splits
%% that will lead it to the separate components in the dependency graph.
%%


-type tag_vertices() :: #{tag() := digraph:vertex()}.
-type tag_root_tree() :: {tags(), [tag_root_tree()]}.
-type root_tree() :: {{tags(), mailbox()}, [root_tree()]}.
-type set_root_tree() :: {{sets:set(tag()), mailbox()}, [set_root_tree()]}.
-type holed_setup_tree() :: {'left' | 'right', split_merge_fun(), temp_setup_tree(), 
			     state_type_pair(), sets:set(tag()), [set_root_tree()]}.

%%
%% This returns a setup tree (from which a configuration tree is derivable).
%% There are three steps in the generation process:
%% 1. Split the tags into legal subsets using the dependency relation.
%%    This assumes that the dependency relation is true (and has been 
%%    previously checked). It greedily removes tags with lowest rates 
%%    and tries to split the remaining tags into connected components.
%%    In the end it returns a root tree (without the physical mapping).
%% 2. Map the root tree to include a physical mapping for each node.
%%    This is done at the moment with the DP algorithm that finds
%%    the optimal mapping of a root tree to physical nodes. Optimal
%%    here means that the least amount of messages is exchanged during 
%%    the computation.
%% 3. Given the complete root tree, derive a legal configuration tree 
%%    using the splits and merges. This procedure shouldn't be greedy
%%    so that it always finds a possible configuration tree if it exists.
%%    It is supposed to give a warning when there doesn't exist a split
%%    to derive the configuration tree.
%%          
-spec generate_setup_tree(specification(), topology()) -> temp_setup_tree().
generate_setup_tree(Specification, Topology) ->
    Dependencies = conf_gen:get_dependencies(Specification),
    
    %% Make the dependency graph
    {DepGraph, TagsVertices} = make_dependency_graph(Dependencies),
    print_graph(DepGraph),

    %% Get the nodes-tags-rates association list
    NodesRates = conf_gen:get_nodes_rates(Topology),
    SortedTags = sort_tags_by_rate_ascending(NodesRates),
    io:format("Sorted Tags: ~p~n", [SortedTags]),
    
    TagsRootTree = iterative_greedy_split(SortedTags, NodesRates, TagsVertices, DepGraph),
    io:format("Tags root tree: ~n~p~n", [TagsRootTree]),
    
    %% Now we have to run the DP algorithm that given a root tree
    %% returns its optimal mapping to physical nodes. (By optimal
    %% it means less messages exchanged.)
    RootTree = root_tree_physical_mapping(TagsRootTree, Topology),
    io:format("Root tree: ~n~p~n", [RootTree]),

    %% Now that we have the root tree we only need to 
    %% find a sequence of splits to reach this root tree
    
    %% Map over the tree, having (a set of options at each stage)
    %% 1. How to find one option for each time.
    %%    - Choose one child tag, and see if it fits in the left (or right)
    %%      child state of some split pair. 
    %%    - Map the father and the chosen child and iterate on the other child.
    %%    - This way we make a binary tree with a hole, and we iterate on the hole.
    %%      The hole is also tagged, with a state type, and all the remaining tags.
    %% 
    %% Is this easily generalizable to hold a set of binary trees with a hole?
    %% We would certainly want this procedure to not be greedy, so that it
    %% always finds a possible split sequence if it does exist.
    SetupTree = root_tree_to_setup_tree(RootTree, Specification),
    io:format("Setup tree: ~n~p~n", [SetupTree]),
    
    SetupTree.

%% This algorithm, given a root tree returns its optimal mapping 
%% to physical nodes (based on the message metric). 
-spec root_tree_physical_mapping(tag_root_tree(), topology()) -> root_tree().
root_tree_physical_mapping(TagRootTree, Topology) ->
    %% TODO: Implement this
    %% 
    %% WARNING: For the moment just assign the sink physical node 
    %% to every node in the root tree
    SinkNode = conf_gen:get_sink_pid(Topology),
    map_physical_node_root_tree_constant(SinkNode, TagRootTree).
    
-spec map_physical_node_root_tree_constant(mailbox(), tag_root_tree()) -> root_tree().
map_physical_node_root_tree_constant(SinkNode, {Tags, Children}) ->
    MappedChildren = [map_physical_node_root_tree_constant(SinkNode, C) || C <- Children],
    {{Tags, SinkNode}, MappedChildren}.

%% Map over the tree, having (a set of options at each stage)
%% 1. How to find one option (greedy) for each time.
%%    - Choose one child tag, and see if it fits in the left (or right)
%%      child state of some split pair. 
%%    - Map the father and the chosen child and iterate on the other child.
%%    - This way we make a binary tree with a hole, and we iterate on the hole.
%%      The hole is also tagged, with a state type, and all the remaining tags.
%% 
%% 2. Is this easily generalizable to hold a set of binary trees with a hole?
%%    We would certainly want this procedure to not be greedy, so that it
%%    always finds a possible split sequence if it does exist.
%% 3. For now we assume that one split is enough, and that there are no
%%    type conversions or splits needed to reach the option to do the split.
-spec root_tree_to_setup_tree(root_tree(), specification()) -> temp_setup_tree().
root_tree_to_setup_tree(RootTree, Specification) ->    
    InitState = conf_gen:get_init_state(Specification),
    
    %% First modify the root tree to contain the union of
    %% all tags handled in each subtree, instead of only
    %% the ones at the top node.
    UnionRootTree = union_root_tree(RootTree),
    greedy_root_tree_to_setup_tree({InitState, UnionRootTree}, Specification).
				
%% TODO: Combine the common part when there exist children or not		
%%
%% TODO: Find a way to recurse without bringing up everything, but rather by 
%%       returning some continuation function or something.		   
-spec greedy_root_tree_to_setup_tree({state_type_pair(), set_root_tree()}, specification()) 
				    -> temp_setup_tree().    
greedy_root_tree_to_setup_tree({StateTypePair, {{HTags, Node}, []}}, Specification) ->
    {StateType, State} = StateTypePair,
    {_Ts, UpdateFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    Predicate = opt_lib:tags_to_predicate(sets:to_list(HTags)),
    Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    {State, Node, Predicate, Funs, []};
%% Having one child in the root tree left is a special case because it
%% means that it was left behind from a split. In this case, HTags,
%% should be equal to the Child's HTag's.
%% TODO: Add this as an assertion.
%% 
%% WARNING: We either have to handle the two children case specially, 
%%          or the one child case. I don't know which one is better though.
%%
%% TODO: Probably it is better to handle two children together, as then,
%%       if they can't be handled, then we can just return the whole tree,
%%       instead of being stuck with a child that we cannot do anything about.
greedy_root_tree_to_setup_tree({StateTypePair, {{HTags, Node}, [Child]}}, Specification) ->
    {StateType, State} = StateTypePair,
    case opt_lib:can_state_type_handle_tags(StateType, HTags, Specification) of
	true ->
	    greedy_root_tree_to_setup_tree({StateTypePair, Child}, Specification);
	false ->
	    util:err("Got stuck into an invalid split :(~n", []),
	    util:crash(0,0)
    end;
greedy_root_tree_to_setup_tree({StateTypePair, {{HTags, Node}, Children}}, Specification) ->
    %% Is there any split state type triple that starts from the state that we are
    %% now (which is supposed to be able to handle HTags) that goes to any state that
    %% can handle any child's subtree tags.
    SplitMergeFuns = conf_gen:get_split_merge_funs(Specification),
    {StateType, State} = StateTypePair,    

    %% WARNING: For now just keep one setup tree and ignore the rest
    %%          Here is where we should normally return that a split
    %%          is missing.
    [HoledSetupTree|_] = 
	filter_splits_satisfy_any_child(StateTypePair, SplitMergeFuns, Children, Specification),
    {LeftRight, SplitMergeFun, TempSetupTree, RestStateTypePair, RestTags, RestSetRootTrees} =
	HoledSetupTree,
    io:format("~p: ~p~n", [LeftRight, TempSetupTree]),
    %% WARNING: We assume that StateType must be the same as our current type.
    %%          That means that there are no chains of splits merges without
    %%          letting behind any tag.
    {{StateType, _LStateType, _RStateType}, {SplitFun, MergeFun}} = SplitMergeFun,

    {_Ts, UpdateFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    Predicate = opt_lib:tags_to_predicate(sets:to_list(HTags)),
    Funs = {UpdateFun, SplitFun, MergeFun},
    %% Here we ideally have to iterate either left or right, depending
    %% on whether the child matches on the left or on the right
    %% side of the split. However, as we are searching for any child,
    %% we can always iterate right. (The greedy search for splits
    %% is incomplete anyway.

    %% Recurse on the rest of the root tree that is not already turned into 
    %% a setup tree. The new node is a fake node, as it will just be used to make the 
    %% tree binary, so it will have the same node as the parent.
    Recurse =
	greedy_root_tree_to_setup_tree({RestStateTypePair, {{RestTags, Node}, RestSetRootTrees}}, 
				       Specification),
    case LeftRight of
	left ->
	    {State, Node, Predicate, Funs, [TempSetupTree, Recurse]};
	right ->
	    {State, Node, Predicate, Funs, [Recurse, TempSetupTree]}
    end.

-spec union_root_tree(root_tree()) -> set_root_tree().
union_root_tree({{HTags, Node}, Children}) ->
    UnionChildren = [union_root_tree(C) || C <- Children],
    AllChildrenTagsSet = 
        sets:union([Tags || {{Tags, _N}, _} <- UnionChildren]),
    TotalTags = sets:union(sets:from_list(HTags), AllChildrenTagsSet),
    {{TotalTags, Node}, UnionChildren}.



%% This function returns all possible pairs of split-merge and set root trees
%% that can be handled as their children. In essence, what it returns is a 
%% list of temp setup trees with a hole, that will be filled with the rest
%% of the tags and root trees by the recursive procedure.
%%
%% WARNING: It assumes that each tag appears in exactly one root tree.
-spec filter_splits_satisfy_any_child(state_type_pair(), split_merge_funs(), 
				      [set_root_tree()], specification()) 
				     -> [holed_setup_tree()].
filter_splits_satisfy_any_child(StateTypePair, SplitMergeFuns, SetRootTrees, Specification) ->
    DeepHoledSetupTrees =
	util:map_focus(
	 fun(Curr, Rest) ->
		 filter_splits_satisfy_any_child_focus(StateTypePair, SplitMergeFuns, Curr, Rest, Specification)
	 end, SetRootTrees),
    lists:flatten(DeepHoledSetupTrees).
    


%% This function focuses on one of the root trees and filters all the splits
%% that can handle it. It also returns the rest of the root trees, and tags
%% so that the caller knows how to recurse further down the root tree.
-spec filter_splits_satisfy_any_child_focus(state_type_pair(), split_merge_funs(), set_root_tree(), 
					    [set_root_tree()], specification()) 
					   -> [holed_setup_tree()].
filter_splits_satisfy_any_child_focus(StateTypePair, SplitMergeFuns, CurrRootTree, 
				      RestRootTrees, Specification) ->    
    RestTags = 
        sets:union([Tags || {{Tags, _N}, _} <- RestRootTrees]),
    FilteredSplitMergeFuns = 
	filter_splits_satisfy_child(StateTypePair, SplitMergeFuns, CurrRootTree, RestTags, Specification),
    [{LeftRight, SplitMergeFun, TempSetupTree, RestStateTypePair, RestTags, RestRootTrees} 
     || {LeftRight, SplitMergeFun, TempSetupTree, RestStateTypePair} <- FilteredSplitMergeFuns].
    

%% This function, is given a list of splits-merges-and their state triples,
%% and a root tree, and checks whether any of those splits merges, can handle
%% this root tree as one of their childs. It filters and returns those that
%% can handle it, together with whether they handled it as their left or right child.
-spec filter_splits_satisfy_child(state_type_pair(), split_merge_funs(), 
				  set_root_tree(), sets:set(tag()), specification()) 
				 -> [{'left' | 'right', split_merge_fun(), 
				      temp_setup_tree(), state_type_pair()}].
filter_splits_satisfy_child({StateType, State}, SplitMergeFuns, {{TagSet, _}, _} = SetRootTree, 
			    RestTags, Specification) ->
    lists:filtermap(
      fun({Triple, SplitMerge}) ->
	      {PStateType, LStateType, RStateType} = Triple,
	      case split_satisfies_requirements(Triple, TagSet, Specification) of
		  false -> 
		      false;
		  LeftRight when PStateType =:= StateType -> 
		      %% Now we know whether this tag set can be handled.
		      %% Because of that, we can split the current state,
		      %% and recurse.
		      {SplitFun, _MergeFun} = SplitMerge,
		      TagSetPred = opt_lib:tags_to_predicate(sets:to_list(TagSet)),
		      RestTagsPred = opt_lib:tags_to_predicate(sets:to_list(RestTags)),
		      {NewStateTypePair, RestStateTypePair} =
			  case LeftRight of
			      left ->
				  {New, Rest} = SplitFun({TagSetPred, RestTagsPred}, State),
				  {{LStateType, New}, {RStateType, Rest}};
			      right ->
				  {Rest, New} = SplitFun({RestTagsPred, TagSetPred}, State),
				  {{RStateType, New}, {LStateType, Rest}}
			  end,
		      %% Now that we know that this tagset can be handled by one
		      %% of the children states of this split triple, we recurse
		      %% to find how would its children be handled
		      TempSetupTree = 
			  greedy_root_tree_to_setup_tree({NewStateTypePair, SetRootTree}, Specification),
		      {true, {LeftRight, {Triple, SplitMerge}, TempSetupTree, RestStateTypePair}};
		  _ ->
		      false
	      end
      end, SplitMergeFuns).

%% This function, given a split state type triple, returns whether a set of tags
%% can be handled by the left or the right child of the split state type triple.
-spec split_satisfies_requirements(state_type_triple(), sets:set(tag()), specification()) 
				   -> 'left' | 'right' | 'false'.
split_satisfies_requirements({_Parent, Left, Right}, TagSet, Specification) -> 
    case opt_lib:can_state_type_handle_tags(Left, TagSet, Specification) of
	true ->
	    left;
	false ->
	    case opt_lib:can_state_type_handle_tags(Right, TagSet, Specification) of
		true ->
		    right;
		false ->
		    false
	    end
    end.

%%
%% This greedy algorithm, greedily chooses the tags with the lowest rates
%% that disconnect the dependency graph and then removes them, iterating.
%%
%% WARNING: It doesn't care whether a split function exists or not,
%%          but it returns the "optimal" greedy tree.
%% MORE IMPORTANT WARNING: It deletes the original graph
%% 
-spec iterative_greedy_split(tags(), nodes_rates(), tag_vertices(), digraph:graph()) -> tag_root_tree().
iterative_greedy_split(Tags, NodesRates, TagsVertices, Graph) ->
    {TopTags, TagsCCs} = best_greedy_split(Tags, TagsVertices, Graph),
    %% WARNING: Naive sorting of Tags based on rates. A better way would be
    %%          to keep the rates and use them to sort here. Or keep the rates
    %%          for each tag in a map
    SortedTagsCCs =
	lists:map(
	  fun(TagsCC) ->
		  FilteredTags = filter_tags_in_nodes_rates(TagsCC, NodesRates),
		  sort_tags_by_rate_ascending(FilteredTags)
	  end, TagsCCs),
    ChildrenRootTrees = 
	lists:map(
	  fun(SortedTagsCC) ->
		  Vertices = [maps:get(Tag, TagsVertices) || Tag <- SortedTagsCC],
		  Subgraph = digraph_utils:subgraph(Graph, Vertices),
		  iterative_greedy_split(SortedTagsCC, NodesRates, TagsVertices, Subgraph)
	  end, SortedTagsCCs),

    %% WARNING: Delete the graph because the ETS is not garbage collected
    true = digraph:delete(Graph),
    {TopTags, ChildrenRootTrees}.
	

%% This function returns the minimal set of tags that disconnects
%% the dependency graph.
-spec best_greedy_split(tags(), tag_vertices(), digraph:graph()) -> {tags(), [tags()]}.
best_greedy_split(Tags, TagsVertices, Graph) ->
    best_greedy_split(Tags, TagsVertices, Graph, []).

-spec best_greedy_split(tags(), tag_vertices(), digraph:graph(), tags()) -> {tags(), [tags()]}.
best_greedy_split([], TagsVertices, Graph, Acc) ->
    {Acc, []};
best_greedy_split([Tag|Tags], TagsVertices, Graph, Acc) ->
    Vertex = maps:get(Tag, TagsVertices),
    case does_disconnect(Vertex, Graph) of
	{disconnected, Components} ->
	    TagCCs = 
		[[get_label(V, Graph) || V <- Component] 
		 || Component <- Components],
	    {[Tag|Acc], TagCCs};
	still_connected ->
	    best_greedy_split(Tags, TagsVertices, Graph, [Tag|Acc])
    end.

-spec does_disconnect(digraph:vertex(), digraph:graph()) -> 
			     {'disconnected', [[digraph:vertex()]]} |
			     'still_connected'.
does_disconnect(Vertex, Graph) ->
    %% Delete the vertex from the graph
    true = digraph:del_vertex(Graph, Vertex),
    %% Check the number of the connected components
    %% after the removal of Vertex
    case digraph_utils:components(Graph) of
	[] ->
	    still_connected;
	[_Component] ->
	    still_connected;
	Components ->
	    {disconnected, Components}
    end.


-spec filter_tags_in_nodes_rates(tags(), nodes_rates()) -> nodes_rates().
filter_tags_in_nodes_rates(Tags, NodesRates) ->
    TagsSet = sets:from_list(Tags),
    [{Node, Tag, Rate} || {Node, Tag, Rate} <- NodesRates, sets:is_element(Tag, TagsSet)].

-spec make_dependency_graph(dependencies()) -> {digraph:graph(), tag_vertices()}.
make_dependency_graph(Dependencies) ->
    Graph = digraph:new(),
    Tags = maps:keys(Dependencies),
    TagsVertices = add_tags_in_dependency_graph(Tags, Graph),
    ok = add_edges_in_dependency_graph(Dependencies, Graph, TagsVertices),
    {Graph, TagsVertices}.
    
-spec add_tags_in_dependency_graph(tags(), digraph:graph()) -> tag_vertices().
add_tags_in_dependency_graph(Tags, Graph) ->
    TagsVerticesList = 
	lists:map(
	  fun(Tag) ->
		  V = digraph:add_vertex(Graph),
		  V = digraph:add_vertex(Graph, V, Tag),
		  {Tag, V}
	  end, Tags),
    maps:from_list(TagsVerticesList).
    
-spec add_edges_in_dependency_graph(dependencies(), digraph:graph(), tag_vertices()) -> ok.
add_edges_in_dependency_graph(Dependencies, Graph, TagsVerts) ->
    lists:foreach(
      fun({Tag, DTags}) ->
	      lists:foreach(
		fun(DTag) ->
			V1 = maps:get(Tag, TagsVerts),
			V2 = maps:get(DTag, TagsVerts),
		        digraph:add_edge(Graph, V1, V2)
		end, DTags)
      end, maps:to_list(Dependencies)).

-spec sort_tags_by_rate_ascending(nodes_rates()) -> tags().
sort_tags_by_rate_ascending(NodesRates) ->
    SortedTagsRate =
	lists:sort(
	  fun({_Node1, _Tag1, Rate1}, {_Node2, _Tag2, Rate2}) ->
		  Rate1 =< Rate2
	  end, NodesRates),
    [Tag || {_Node, Tag, _Rate} <- SortedTagsRate].

-spec print_graph(digraph:graph()) -> ok.
print_graph(Graph) ->
    Edges = digraph:edges(Graph),
    FullEdges = [digraph:edge(Graph, E) || E <- Edges],
    LabelEdges = [{get_label(V1, Graph), get_label(V2, Graph)} || {_, V1, V2, _} <- FullEdges],
    io:format("Dependency Graph: ~n~p~n", [LabelEdges]),
    ok.
    
-spec get_label(digraph:vertex(), digraph:graph()) -> tag().
get_label(V, G) ->
    {V, Label} = digraph:vertex(G, V),
    Label.
