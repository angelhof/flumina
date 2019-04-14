-module(optimizer_greedy).

-export([generate_setup_tree/2]).

-include("type_definitions.hrl").

%%
%% This is a very simple greedy optimizer. It sorts tags based on increasing
%% order of rates, and tries to remove tags in this order, until the dependency graph
%% becomes disconnected. When it becomes disconnected, it tries to find a sequence of splits
%% that will lead it to the separate components in the dependency graph.
%%


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
    
    %% TODO: Rename to iterative greedy disconnect
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
    %% WARNING: For the moment just assign each root tree node
    %%          to the node with the highest rate for the tags
    %%          handled by the root node.
    NodesRates = conf_gen:get_nodes_rates(Topology),
    
    opt_lib:map_physical_node_root_tree_max_rate(NodesRates, TagRootTree).


%% Note: For now we assume that one split is enough, and that there are no
%%       type conversions or splits needed to reach the option to do the split.
-spec root_tree_to_setup_tree(root_tree(), specification()) -> temp_setup_tree().
root_tree_to_setup_tree(RootTree, Specification) ->    
    InitState = conf_gen:get_init_state(Specification),
    
    %% First modify the root tree to contain the union of
    %% all tags handled in each subtree, instead of only
    %% the ones at the top node.
    UnionRootTree = union_root_tree(RootTree),
    
    %% Find all the possible setup trees
    PossibleSetupTrees = 
	complete_root_tree_to_setup_tree({InitState, UnionRootTree, fun(X) -> X end}, Specification),

    %% Return the shortest one
    {_Height, ShortestSetupTree} =
	lists:min([{opt_lib:temp_setup_tree_height(T), T} || T <- PossibleSetupTrees]),
    ShortestSetupTree.


%% Complete root_tree_to_setup_tree
%% 
%% This is a naive algorithm (in the sense that it doesn't compress at all
%% and might be re-searching the same trees) root_tree_to_setup_tree.
%% Also it seems that the trees shold be exponential in the number of splits
%% so we should be careful. However it is complete.
%%
%% WARNING: The number of trees is huge if we recount, so there might be a problem.
-spec complete_root_tree_to_setup_tree(hole_setup_tree(), specification()) 
				      -> [temp_setup_tree()].    
complete_root_tree_to_setup_tree({StateTypePair, {{HTags, Node}, []}, HoleTree}, Specification) ->
    {StateType, State} = StateTypePair,
    {_Ts, UpdateFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    Predicate = opt_lib:tags_to_predicate(sets:to_list(HTags)),
    Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    NewTree = {State, Node, Predicate, Funs, []},
    [HoleTree(NewTree)];
complete_root_tree_to_setup_tree({StateTypePair, {{HTags, _Node}, [Child]}, HoleTree}, Specification) ->
    {StateType, _State} = StateTypePair,
    case opt_lib:can_state_type_handle_tags(StateType, HTags, Specification) of
	true ->
	    complete_root_tree_to_setup_tree({StateTypePair, Child, HoleTree}, Specification);
	false ->
	    %% We got stuck and this child can not be handled by the specific
	    %% state type.
	    []
    end;
complete_root_tree_to_setup_tree({StateTypePair, {{HTags, Node}, Children}, HoleTree}, Specification) ->
    %% Is there any split state type triple that starts from the state that we are
    %% now (which is supposed to be able to handle HTags) that goes to any state that
    %% can handle any child's subtree tags.
    SplitMergeFuns = conf_gen:get_split_merge_funs(Specification),
    HoledSetupTrees = 
        filter_splits_satisfy_any_child(StateTypePair, {HTags, Node}, SplitMergeFuns, 
					    Children, Specification),
    lists:flatmap(
      fun({HoleStateTypePair, HoleRootTree, HoleHoleTree}) ->
	      HoleSetupTrees = 
		  complete_root_tree_to_setup_tree({HoleStateTypePair, HoleRootTree, HoleHoleTree}, 
						   Specification),
	      [HoleTree(HoleSetupTree) || HoleSetupTree <- HoleSetupTrees]
      end, HoledSetupTrees).

%% This function returns all possible pairs of split-merge and set root trees
%% that can be handled as their children. In essence, what it returns is a 
%% list of temp setup trees with a hole, that will be filled with the rest
%% of the tags and root trees by the recursive procedure.
%%
%% WARNING: It assumes that each tag appears in exactly one root tree.
-spec filter_splits_satisfy_any_child(state_type_pair(), {sets:set(tag()), node()}, split_merge_funs(), 
				      [set_root_tree()], specification()) 
				     -> [hole_setup_tree()].
filter_splits_satisfy_any_child(StateTypePair, TagsNode, SplitMergeFuns, SetRootTrees, Specification) ->
    DeepHoledSetupTrees =
	util:map_focus(
	  fun(Curr, Rest) ->
		  filter_splits_satisfy_child(StateTypePair, TagsNode, 
					      SplitMergeFuns, Curr, Rest, Specification)
	  end, SetRootTrees),
    lists:flatten(DeepHoledSetupTrees).


%% This function, is given a list of splits-merges-and their state triples,
%% and a root tree, and checks whether any of those splits merges, can handle
%% this root tree as one of their childs. It filters and returns those that
%% can handle it, together with whether they handled it as their left or right child.
-spec filter_splits_satisfy_child(state_type_pair(), {sets:set(tag()), node()}, 
				  split_merge_funs(), set_root_tree(), 
				  [set_root_tree()], specification()) 
				 -> [hole_setup_tree()].
filter_splits_satisfy_child({StateType, State}, {HTags, Node}, SplitMergeFuns, 
				{{TagSet, _}, _} = SetRootTree, 
				RestRootTrees, Specification) ->
    RestTags = 
        sets:union([Tags || {{Tags, _N}, _} <- RestRootTrees]),
    %% Filter only to triples where the parent state type is
    %% the same as the current state type
    FilteredSplitMergeFuns =
	[SMF || {{PStateType, _LST, _RST}, _SM} = SMF <- SplitMergeFuns, PStateType =:= StateType],
    lists:flatmap(
      fun({Triple, SplitMerge}) ->
	      LeftRightMatches = split_satisfies_requirements(Triple, TagSet, Specification),	      
	      %% For each possible match (left | right) return all the possible
	      %% setup trees for the matches sub root tree.
	      lists:flatmap(
		fun(LeftRight) ->
			%% First we have to finalize the side of the setup
			%% tree that matched the split, and then we can 
			%% create the hole on the other side.
			{NewStateTypePair, RestStateTypePair} =
			    split_left_or_right(LeftRight, {Triple, SplitMerge}, TagSet, RestTags, State),
		
			%% Make all possible setup trees for the matched size.
			%% Give an empty hole tree, as we can locally make this search.
			MatchedSideTempSetupTrees = 
			    complete_root_tree_to_setup_tree({NewStateTypePair, SetRootTree, fun(X) -> X end}, 
							     Specification),
			
			%% Now that we have the matched trees from one side, we can create the hole on
			%% the other side
			finalize_split_hole_setup_trees(LeftRight, {StateType, State}, 
							{HTags, Node}, SplitMerge, RestTags,
							RestStateTypePair, RestRootTrees,
							MatchedSideTempSetupTrees, Specification)
		end, LeftRightMatches)
      end, FilteredSplitMergeFuns).

-spec split_left_or_right('left' | 'right', split_merge_fun(), sets:set(tag()), 
			  sets:set(tag()), State::any()) 
			 -> {state_type_pair(), state_type_pair()}.	 
split_left_or_right(LeftRight, {Triple, SplitMerge}, CurrTags, RestTags, State) ->
    {SplitFun, MergeFun} = SplitMerge,
    {_PST, LStateType, RStateType} = Triple,
    CurrTagsPred = opt_lib:tags_to_predicate(sets:to_list(CurrTags)),
    RestTagsPred = opt_lib:tags_to_predicate(sets:to_list(RestTags)),
    case LeftRight of
	left ->
	    {New, Rest} = SplitFun({CurrTagsPred, RestTagsPred}, State),
	    {{LStateType, New}, {RStateType, Rest}};
	right ->
	    {Rest, New} = SplitFun({RestTagsPred, CurrTagsPred}, State),
	    {{RStateType, New}, {LStateType, Rest}}
    end.

-spec finalize_split_hole_setup_trees('left' | 'right', state_type_pair(), {sets:set(tag()), node()},
				      split_merge(), sets:set(tag()), 
				      state_type_pair(), [set_root_tree()], 
				      [temp_setup_tree()], specification()) 
				     -> [hole_setup_tree()].
finalize_split_hole_setup_trees(LeftRight, {StateType, State}, {HTags, Node}, 
				SplitMerge, RestTags,
				RestStateTypePair, RestRootTrees, 
				MatchedSideTempSetupTrees, Specification) ->
    {_Ts, UpdateFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    {SplitFun, MergeFun} = SplitMerge,
    Funs = {UpdateFun, SplitFun, MergeFun},
    HTagsPred = opt_lib:tags_to_predicate(sets:to_list(HTags)),
    lists:map(
      fun(MatchedSideTempSetupTree) ->
	      FinalHoleTree = 
		  fun(HoleSetupTree) ->
			  FinalChildren =
			      case LeftRight of
				  left ->
				      %% This means that the left child
				      %% was matched with split, and therefore
				      %% the hole goes right.
				      [MatchedSideTempSetupTree, HoleSetupTree];
				  right ->
				      [HoleSetupTree, MatchedSideTempSetupTree]
			      end,
			  {State, Node, HTagsPred, Funs, FinalChildren}
		  end,
	      %% The root tree now has an empty parent node, as it is really
	      %% a forest of root trees. So just assign it to the current Node.
	      {RestStateTypePair, {{RestTags, Node}, RestRootTrees}, FinalHoleTree}
      end, MatchedSideTempSetupTrees).


%% This function, given a split state type triple, returns whether a set of tags
%% can be handled by the left or the right child of the split state type triple.
-spec split_satisfies_requirements(state_type_triple(), sets:set(tag()), specification()) 
				  -> ['left' | 'right'].
split_satisfies_requirements({_Parent, Left, Right}, TagSet, Specification) -> 
    L1 = 
	case opt_lib:can_state_type_handle_tags(Left, TagSet, Specification) of
	    true -> [left];
	    false -> []
	end,
    L2 = 
	case opt_lib:can_state_type_handle_tags(Right, TagSet, Specification) of
	    true -> [right];
	    false -> []
	end,		
    L1 ++ L2.


-spec union_root_tree(root_tree()) -> set_root_tree().
union_root_tree({{HTags, Node}, Children}) ->
    UnionChildren = [union_root_tree(C) || C <- Children],
    AllChildrenTagsSet = 
        sets:union([Tags || {{Tags, _N}, _} <- UnionChildren]),
    TotalTags = sets:union(sets:from_list(HTags), AllChildrenTagsSet),
    {{TotalTags, Node}, UnionChildren}.




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
		  FilteredTags = opt_lib:filter_tags_in_nodes_rates(TagsCC, NodesRates),
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
best_greedy_split([], _TagsVertices, _Graph, Acc) ->
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
