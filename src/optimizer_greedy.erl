-module(optimizer_greedy).

-export([generate_setup_tree/2,
         generate_setup_tree/3,

         generate_root_tree/2,
         generate_root_tree/3]).

-include("type_definitions.hrl").
-include("config.hrl").

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
    generate_setup_tree(Specification, Topology, edge).

%% The third argument dictates whether the workers will be mapped in
%% one central node, or in edge nodes close to the sources.
-spec generate_setup_tree(specification(), topology(), 'edge' | 'centralized') -> temp_setup_tree().
generate_setup_tree(Specification, Topology, IsCentralized) ->
    RootTree = generate_root_tree(Specification, Topology, IsCentralized),
    io:format("Root tree: ~n~p~n", [RootTree]),

    %% First modify the root tree to contain the union of
    %% all tags handled in each subtree, instead of only
    %% the ones at the top node.
    UnionRootTree = union_root_tree(RootTree),

    %% Note: This is experimental
    %% BinaryRootTree = generate_binary_root_tree(RootTree),
    %% io:format("Binary root tree:~n~p~n", [BinaryRootTree]),

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
    SetupTree = root_tree_to_setup_tree(UnionRootTree, Specification),
    %% io:format("Setup tree: ~n~p~n", [SetupTree]),

    SetupTree.

-spec generate_root_tree(specification(), topology()) -> root_tree().
generate_root_tree(Specification, Topology) ->
    generate_root_tree(Specification, Topology, edge).

-spec generate_root_tree(specification(), topology(), 'edge' | 'centralized') -> root_tree().
generate_root_tree(Specification, Topology, IsCentralized) ->
    Dependencies = conf_gen:get_dependencies(Specification),
    RawImplTags = conf_gen:get_implementation_tags(Topology),
    %% io:format("Impl Tags:~p~n", [RawImplTags]),

    %% Remove duplicate implementation tags
    %% TODO: Properly support multiple streams of the same tag in the same node instead of merging.
    ImplTags = remove_duplicate_impl_tags(RawImplTags),

    %% Make the dependency graph
    {DepGraph, TagsVertices} = make_impl_dependency_graph(Dependencies, ImplTags),
    print_graph(DepGraph),

    %% TODO: The dependency graph is empty for some reason.

    %% Get the nodes-tags-rates association list
    NodesRates = conf_gen:get_nodes_rates(Topology),
    MergedNodesRates = merge_rates_for_same_node_tag(NodesRates),
    SortedImplTags = sort_tags_by_rate_ascending(MergedNodesRates),
    %% io:format("Sorted Tags: ~p~n", [SortedImplTags]),

    %% TODO: Rename to iterative greedy disconnect
    TagsRootTree = iterative_greedy_disconnect(SortedImplTags, NodesRates, TagsVertices, DepGraph),
    %% io:format("Tags root tree: ~n~p~n", [TagsRootTree]),

    %% Now we have to run the DP algorithm that given a root tree
    %% returns its optimal mapping to physical nodes. (By optimal
    %% it means less messages exchanged.)
    RootTree = root_tree_physical_mapping(TagsRootTree, Topology, IsCentralized),
    RootTree.

%% This algorithm, given a root tree returns its optimal mapping
%% to physical nodes (based on the message metric).
-spec root_tree_physical_mapping(tag_root_tree(), topology(), 'edge' | 'centralized') -> root_tree().
root_tree_physical_mapping(TagRootTree, Topology, IsCentralized) ->
    %% TODO: Implement this
    %%
    %% WARNING: For the moment just assign each root tree node
    %%          to the node with the highest rate for the tags
    %%          handled by the root node.
    NodesRates = conf_gen:get_nodes_rates(Topology),

    case IsCentralized of
        edge ->
            opt_lib:map_physical_node_root_tree_max_rate(NodesRates, TagRootTree);
        centralized ->
            opt_lib:map_physical_node_root_tree_constant(node(), TagRootTree)
    end.

%% Note: For now we assume that one split is enough, and that there are no
%%       type conversions or splits needed to reach the option to do the split.
-spec root_tree_to_setup_tree(set_root_tree(), specification()) -> temp_setup_tree().
root_tree_to_setup_tree(UnionRootTree, Specification) ->
    InitState = conf_gen:get_init_state(Specification),

    %% Find all the possible setup trees
    PossibleSetupTrees =
	complete_root_tree_to_setup_tree({InitState, UnionRootTree, fun(X) -> X end}, Specification),

    io:format("Possible setup trees: ~p~n", [length(PossibleSetupTrees)]),
    %% io:format("Possible setup trees: ~n~p~n", [lists:sublist(PossibleSetupTrees, 10)]),

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
    Predicate = opt_lib:impl_tags_to_predicate(sets:to_list(HTags)),
    SpecPredicate = opt_lib:impl_tags_to_spec_predicate(sets:to_list(HTags)),
    Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    NewTree = {State, Node, {SpecPredicate, Predicate}, Funs, []},
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

    %% TODO: Optimization: Instead of just passing each child as a set
    %% root tree, maybe we could pass child combinations, like a root
    %% tree that contains one of the children and the rest as its children.
    %%
    %% The most important of those is the binary balanced tree.
    %% ExtendedChildren = generate_deeper_root_trees(Children),

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%% Experimental
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% %% This function returns a binary root tree given a normal one. It
% %% doesn't check whether it is possible to generate that root tree
% %% from splits and merges though.
% -spec generate_binary_root_tree(root_tree()) -> root_tree().
% generate_binary_root_tree({Node, Children}) ->
%     BinarizedSubtrees = [generate_binary_root_tree(Child) || Child <- Children],
%     BinaryRootTree = {Node, BinarizedSubtrees},
%     FinalBinaryRootTree = binarize_root_tree_layer(BinaryRootTree),
%     %% io:format("Binary root tree:~n~p~n", [FinalBinaryRootTree]),
%     FinalBinaryRootTree.

% -spec binarize_root_tree_layer(root_tree()) -> root_tree().
% binarize_root_tree_layer({{_Tags, _Node}, []} = RootTree) ->
%     RootTree;
% binarize_root_tree_layer({{Tags, Node}, Children}) ->
%     %% TODO: Make a nested binary list from the inital list
%     {{[], Node}, NestedBinaryChildren} = binarize_root_tree_list(Node, Children),
%     %% io:format("Nested Children:~n~p~n", [NestedBinaryChildren]),
%     {{Tags, Node}, NestedBinaryChildren}.

%% TODO: Optimize to not always put the extra level on top of the big subtrees
%% -spec binarize_root_tree_list([root_tree()]) -> [root_tree()].
%% binarize_root_tree_list(RootTrees) ->
%%     Len = length(RootTrees),
%%     case Len =< 2 of
%%         true ->
%%             RootTrees;
%%         false ->
%%             HalfSize = Len div 2,
%%             {FirstHalf, Rest} = lists:split(HalfSize, RootTrees),
%%             {SecondHalf, Rem} = lists:split(HalfSize, Rest),
%%             BinarizedFirstHalf = binarize_root_tree_list(FirstHalf),
%%             BinarizedSecondHalf = binarize_root_tree_list(SecondHalf),
%%             BinarizedRem = binarize_root_tree_list(Rem),
%%             [[BinarizedFirstHalf, BinarizedSecondHalf], BinarizedRem]
%%     end.

% -spec binarize_root_tree_list(node(), [root_tree()]) -> root_tree().
% binarize_root_tree_list(Node, RootTrees) ->
%     Len = length(RootTrees),

%     %% Take a full binary tree for the biggest power of 2
%     BiggestPowerExponent = util:biggest_power_of_two_less_than_number(Len),
%     BiggestPower = util:intpow(2, BiggestPowerExponent),
%     {FirstPart, Rest} = lists:split(BiggestPower, RootTrees),
%     FirstPartRootTree = make_total_binary_root_tree_from_children_list(Node, FirstPart),
%     case Rest of
%         [] ->
%             FirstPartRootTree;
%         _ ->
%             SecondPartRootTree = binarize_root_tree_list(Node, Rest),
%             {{[], Node}, [FirstPartRootTree, SecondPartRootTree]}
%     end.

%     %% case Len =< 1 of
%     %%     true ->
%     %%         RootTrees;
%     %%     false ->
%     %%         HalfSize = Len div 2,
%     %%         {FirstHalf, Rest} = lists:split(HalfSize, RootTrees),
%     %%         {SecondHalf, Rem} = lists:split(HalfSize, Rest),
%     %%         BinarizedFirstHalf = binarize_root_tree_list(FirstHalf),
%     %%         BinarizedSecondHalf = binarize_root_tree_list(SecondHalf),
%     %%         BinarizedRem = binarize_root_tree_list(Rem),
%     %%         [[BinarizedFirstHalf, BinarizedSecondHalf], BinarizedRem]
%     %% end.


% make_total_binary_root_tree_from_children_list(Node, [_|_] = RootTrees) ->
%     Len = length(RootTrees),
%     case Len =< 1 of
%         true ->
%             [RootTree] = RootTrees,
%             RootTree;
%         false->
%             HalfSize = Len div 2,
%             %% Assert that the input is a full power of two
%             0 = Len rem 2,
%             {FirstHalf, SecondHalf} = lists:split(HalfSize, RootTrees),
%             BinarizedFirstHalf = make_total_binary_root_tree_from_children_list(Node, FirstHalf),
%             BinarizedSecondHalf = make_total_binary_root_tree_from_children_list(Node, SecondHalf),
%             {{[], Node}, [BinarizedFirstHalf, BinarizedSecondHalf]}
%     end.

%     %% case RootTrees of
%     %%     [] ->
%     %%         [];
%     %%     [Child] ->
%     %%         [Child];
%     %%     [Child1, Child2] ->

%     %% {Node, NestedBinaryChildren}.

%% This function returns all possible pairs of split-merge and set root trees
%% that can be handled as their children. In essence, what it returns is a
%% list of temp setup trees with a hole, that will be filled with the rest
%% of the tags and root trees by the recursive procedure.
%%
%% WARNING: It assumes that each tag appears in exactly one root tree.
-spec filter_splits_satisfy_any_child(state_type_pair(), {sets:set(impl_tag()), node()}, split_merge_funs(),
				      [set_root_tree()], specification())
				     -> [hole_setup_tree()].
filter_splits_satisfy_any_child(StateTypePair, TagsNode, SplitMergeFuns, SetRootTrees, Specification) ->
    %% io:format("Set Root Trees: ~p~n", [SetRootTrees]),
    UniqueRootTrees = unique_root_trees_focus(SetRootTrees),
    DeepHoledSetupTrees =
        [filter_splits_satisfy_child(StateTypePair, TagsNode, SplitMergeFuns, Curr, Rest, Specification)
	 || {Curr, Rest} <- UniqueRootTrees],
    lists:flatten(DeepHoledSetupTrees).

%% ==========================================================================
%%
%% Setup Tree redundancy optimization
%%
%% ==========================================================================

%% This function is given a list of root trees for which we have to check if they match
%% or not as children of a split on the current state.
%% However, it doesn't naively check whether any root tree can match as a child of
%% a split on the current state.
%%
%% Instead, it gathers all root trees in
%% similar sets (similarity is not yet well defined) and then checks for
%% only one member of each similar group.
%%
%% This is done, to reduce the number of generated setup trees when the children
%% root trees are the same (an example is when we have b -> (a1, a2, a3, a4),
%% the previous algorithm was checking whether a1 matches, whether a2 matches, etc.
%% However most of those are redundant because it doesnt matter whether a1 or a2 or...
%% is the first child etc.
%%
%% It is clear that the more coarse grained this similarity is, the less redundant
%% setup trees we will have in the end. However we have to make sure that the similarity
%% is fine grained enough so that the algorithm stays complete).
%%
%% For now, I will just define similarity as: Root trees r1 and r2 are similar,
%% if they are both leaves, and they have the same specification tag.
%%
%% ASSUMPTION:
%% This assumes that the handled set of tags for each state type contains
%% all or none the implementation tags of a specification tag (so regarding
%% the sets of tags that a state type can handle, implementation tags of
%% the same specificaation tag are equivalent.
%%
%% WARNING: (Not sure) As the similarity doesn't take node into account,
%% it might be the case that this returns a suboptimal configuration tree,
%% that is one where there are some unnecessary message back and forths between nodes.
-spec unique_root_trees_focus([set_root_tree()])
			     -> [{set_root_tree(), [set_root_tree()]}].
unique_root_trees_focus(RootTrees) ->
    SetOfUniqueRootTrees =
	sets:from_list(one_of_each_similar(RootTrees)),
    CurrRestRootTreePairs =
	util:map_focus(
	  fun(Curr, Rest) ->
		  case sets:is_element(Curr, SetOfUniqueRootTrees) of
		      true ->
			  %% Only keep the unique (one of each similarity class)
			  %% root trees
			  [{Curr, Rest}];
		      false ->
			  []
		  end
	  end, RootTrees),
    lists:flatten(CurrRestRootTreePairs).


%% This function returns one root tree of each similarity class
-spec one_of_each_similar([set_root_tree()]) -> [set_root_tree()].
one_of_each_similar(RootTrees) ->
    lists:foldl(
     fun(RootTree, RootTreesAcc) ->
	     case any_similar_list(RootTree, RootTreesAcc) of
		 true ->
		     %% If it is similar with one in the list drop it
		     RootTreesAcc;
		 false ->
		     %% Else keep it
		     [RootTree|RootTreesAcc]
	     end
     end, [], RootTrees).


-spec any_similar_list(set_root_tree(), [set_root_tree()]) -> boolean().
any_similar_list(RootTree1, RootTrees) ->
    lists:any(
      fun(RootTree2) ->
	      similar(RootTree1, RootTree2)
      end, RootTrees).

%% TODO: Think of the largest correct similarity relation
%% For now it is heuristic based.
-spec similar(set_root_tree(), set_root_tree()) -> boolean().
similar({{TagSet1, _Node1}, []}, {{TagSet2, _}, []} ) ->
    %% Only leaf nodes with implementation tags of the same specification tag
    %% in their tag sets are considered similar.
    %% Clearly this is not complete, but for now this solves the case where
    %% the algorithm doesn't scale.
    SpecTagSet1 = specification_tags(TagSet1),
    SpecTagSet2 = specification_tags(TagSet2),
    sets:is_subset(SpecTagSet1, SpecTagSet2)
	andalso sets:is_subset(SpecTagSet2, SpecTagSet1);
similar(_RootTree1, _RootTree2) ->
    false.

%% This lifts the function from below to sets of implementation tags
-spec specification_tags(sets:set(impl_tag())) -> sets:set(tag()).
specification_tags(TagSet) ->
    sets:fold(
      fun(ImplTag, SpecTagsAcc) ->
	      sets:add_element(specification_tag(ImplTag), SpecTagsAcc)
      end ,sets:new(),TagSet).

%% TODO: Move this function in a more general library
%% This assumes that the specification tag is the first element of the
%% implementation tag tuple, (if the implementation tag is a tuple)
%% WARNING: NOT SURE ABOUT THIS NOW THAT WE HAVE
%%          MOVED IMPLEMENTATION TAGS TO BE INDICATED BY NODE
%% TODO: This should be renamed and generalized appropriately
%% according to our discussion.
-spec specification_tag(impl_tag()) -> tag().
specification_tag({{Tag, _Id}, _Node}) ->
    Tag;
specification_tag({Tag, _Node}) ->
    Tag.



%% ==========================================================================


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
			MatchedSideTempSetupTrees0 =
			    complete_root_tree_to_setup_tree({NewStateTypePair, SetRootTree, fun(X) -> X end},
							     Specification),
			%% WARNING:
			%% Hack that might work, to not have duplicate setup trees
			%% It doesn't seem to offer anything, but let's keep it here.
			MatchedSideTempSetupTreesSet = sets:from_list(MatchedSideTempSetupTrees0),
			MatchedSideTempSetupTrees = sets:to_list(MatchedSideTempSetupTreesSet),


			%% Now that we have the matched trees from one side, we can create the hole on
			%% the other side
			finalize_split_hole_setup_trees(LeftRight, {StateType, State},
							{HTags, Node}, SplitMerge, RestTags,
							RestStateTypePair, RestRootTrees,
							MatchedSideTempSetupTrees, Specification)
		end, LeftRightMatches)
      end, FilteredSplitMergeFuns).

-spec split_left_or_right('left' | 'right', split_merge_fun(), sets:set(impl_tag()),
			  sets:set(impl_tag()), State::any())
			 -> {state_type_pair(), state_type_pair()}.
split_left_or_right(LeftRight, {Triple, SplitMerge}, CurrTags, RestTags, State) ->
    {SplitFun, _MergeFun} = SplitMerge,
    {_PST, LStateType, RStateType} = Triple,
    CurrTagsPred = opt_lib:impl_tags_to_spec_predicate(sets:to_list(CurrTags)),
    RestTagsPred = opt_lib:impl_tags_to_spec_predicate(sets:to_list(RestTags)),
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
    HTagsPred = opt_lib:impl_tags_to_predicate(sets:to_list(HTags)),
    HSpecTagsPred = opt_lib:impl_tags_to_spec_predicate(sets:to_list(HTags)),
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
			  {State, Node, {HSpecTagsPred, HTagsPred}, Funs, FinalChildren}
		  end,
	      %% The root tree now has an empty parent node, as it is really
	      %% a forest of root trees. So just assign it to the current Node.
	      {RestStateTypePair, {{RestTags, Node}, RestRootTrees}, FinalHoleTree}
      end, MatchedSideTempSetupTrees).


%% This function, given a split state type triple, returns whether a set of tags
%% can be handled by the left or the right child of the split state type triple.
-spec split_satisfies_requirements(state_type_triple(), sets:set(tag()), specification())
				  -> ['left' | 'right'].
split_satisfies_requirements({_Parent, Left, Right}, TagSet, Specification)
  when Left =:= Right ->
    %% Optimization:
    %% When the right and left states are the same, it doesn't make sense
    %% to return both matches, as both will certainly match
    case opt_lib:can_state_type_handle_tags(Left, TagSet, Specification) of
	true -> [left];
	false -> []
    end;
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
-spec iterative_greedy_disconnect(impl_tags(), nodes_rates(), tag_vertices(), digraph:graph()) -> tag_root_tree().
iterative_greedy_disconnect(ImplTags, NodesRates, TagsVertices, Graph) ->
    {TopTags, TagsCCs} = best_greedy_disconnect(ImplTags, TagsVertices, Graph),
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
		  iterative_greedy_disconnect(SortedTagsCC, NodesRates, TagsVertices, Subgraph)
	  end, SortedTagsCCs),

    %% WARNING: Delete the graph because the ETS is not garbage collected
    true = digraph:delete(Graph),
    {TopTags, ChildrenRootTrees}.


-spec best_greedy_disconnect(impl_tags(), tag_vertices(), digraph:graph())
                            -> {impl_tags(), [impl_tags()]}.
best_greedy_disconnect(ImplTags, TagsVertices, Graph) ->
    {TopTags, Components} =
        best_greedy_disconnect(ImplTags, TagsVertices, Graph, []),
    TagCCs =
        [[get_label(V, Graph) || V <- Component]
         || Component <- Components],
    {TopTags, TagCCs}.

-spec best_greedy_disconnect(impl_tags(), tag_vertices(), digraph:graph(), impl_tags())
                            -> {impl_tags(), [[digraph:vertex()]]}.
best_greedy_disconnect(ImplTags, TagsVertices, Graph, Acc) ->
    case is_disconnected(Graph) of
	{disconnected, Components} ->
	    {Acc, Components};
	still_connected ->
            case ImplTags of
                [] ->
                    {Acc, digraph_utils:components(Graph)};
                [ImplTag|RestImplTags] ->
                    %% Delete the vertex from the graph
                    Vertex = maps:get(ImplTag, TagsVertices),
                    true = digraph:del_vertex(Graph, Vertex),
                    best_greedy_disconnect(RestImplTags, TagsVertices, Graph, [ImplTag|Acc])
            end
    end.

-spec is_disconnected(digraph:graph()) -> {'disconnected', [[digraph:vertex()]]}
                                       |  'still_connected'.
is_disconnected(Graph) ->
    case digraph_utils:components(Graph) of
	[] ->
	    still_connected;
	[_Component] ->
	    still_connected;
	Components ->
	    {disconnected, Components}
    end.

-spec make_impl_dependency_graph(dependencies(), impl_tags()) -> {digraph:graph(), tag_vertices()}.
make_impl_dependency_graph(Dependencies, ImplTags) ->
    Graph = digraph:new(),
    TagsVertices = add_tags_in_dependency_graph(ImplTags, Graph),
    ok = add_edges_in_dependency_graph(Dependencies, Graph, TagsVertices, ImplTags),
    {Graph, TagsVertices}.

-spec add_tags_in_dependency_graph(impl_tags(), digraph:graph()) -> tag_vertices().
add_tags_in_dependency_graph(ImplTags, Graph) ->
    TagsVerticesList =
	lists:map(
	  fun(ImplTag) ->
		  V = digraph:add_vertex(Graph),
		  V = digraph:add_vertex(Graph, V, ImplTag),
		  {ImplTag, V}
	  end, ImplTags),
    maps:from_list(TagsVerticesList).

-spec add_edges_in_dependency_graph(dependencies(), digraph:graph(), tag_vertices(), impl_tags()) -> ok.
add_edges_in_dependency_graph(Dependencies, Graph, TagsVerts, ImplTags) ->
    lists:foreach(
      fun({STag, SDTags}) ->
	      %% All the implementation tags that come from
	      %% this spec tag
	      ITags = spec_tag_to_impl_tags(STag, ImplTags),
	      IDTags =
		  lists:flatten([spec_tag_to_impl_tags(SDT, ImplTags) || SDT <- SDTags]),
	      lists:foreach(
		fun(IT) ->
			lists:foreach(
			  fun(IDT) ->
				  V1 = maps:get(IT, TagsVerts),
				  V2 = maps:get(IDT, TagsVerts),
				  digraph:add_edge(Graph, V1, V2)
			  end, IDTags)
		end, ITags)
      end, maps:to_list(Dependencies)).

%% WARNING: It seems that this function behaves well for
%% stream-table-join for T =:= STag. But before it worked and it was
%% IT =:= STag. Figure out if it creates any issues.
-spec spec_tag_to_impl_tags(tag(), impl_tags()) -> impl_tags().
spec_tag_to_impl_tags(STag, ImplTags) ->
    [IT || {T, _} = IT <- ImplTags, T =:= STag].
%% TODO: WHY IS THIS LIKE THAT. It should use T, and not IT? I should
%% make sure to explain this.


%% -spec make_dependency_graph(dependencies()) -> {digraph:graph(), tag_vertices()}.
%% make_dependency_graph(Dependencies) ->
%%     Graph = digraph:new(),
%%     Tags = maps:keys(Dependencies),
%%     TagsVertices = add_tags_in_dependency_graph(Tags, Graph),
%%     ok = add_edges_in_dependency_graph(Dependencies, Graph, TagsVertices),
%%     {Graph, TagsVertices}.

%% -spec add_tags_in_dependency_graph(tags(), digraph:graph()) -> tag_vertices().
%% add_tags_in_dependency_graph(Tags, Graph) ->
%%     TagsVerticesList =
%% 	lists:map(
%% 	  fun(Tag) ->
%% 		  V = digraph:add_vertex(Graph),
%% 		  V = digraph:add_vertex(Graph, V, Tag),
%% 		  {Tag, V}
%% 	  end, Tags),
%%     maps:from_list(TagsVerticesList).

%% -spec add_edges_in_dependency_graph(dependencies(), digraph:graph(), tag_vertices()) -> ok.
%% add_edges_in_dependency_graph(Dependencies, Graph, TagsVerts) ->
%%     lists:foreach(
%%       fun({Tag, DTags}) ->
%% 	      lists:foreach(
%% 		fun(DTag) ->
%% 			V1 = maps:get(Tag, TagsVerts),
%% 			V2 = maps:get(DTag, TagsVerts),
%% 		        digraph:add_edge(Graph, V1, V2)
%% 		end, DTags)
%%       end, maps:to_list(Dependencies)).

-spec sort_tags_by_rate_ascending(nodes_rates()) -> impl_tags().
sort_tags_by_rate_ascending(NodesRates) ->
    SortedTagsRate =
	lists:sort(
	  fun({_Node1, _Tag1, Rate1}, {_Node2, _Tag2, Rate2}) ->
		  Rate1 =< Rate2
	  end, NodesRates),
    SortedTagsList = [{Tag, Node} || {Node, Tag, _Rate} <- SortedTagsRate],
    SortedTagsList.

-spec remove_duplicate_impl_tags([impl_tag()]) -> [impl_tag()].
remove_duplicate_impl_tags(ImplTags) ->
    sets:to_list(sets:from_list(ImplTags)).

-spec merge_rates_for_same_node_tag(nodes_rates()) -> nodes_rates().
merge_rates_for_same_node_tag(NodesRates) ->
    MergedRates =
        lists:foldl(
          fun({Node, Tag, Rate}, Map) ->
                  maps:update_with({Node, Tag},
                                   fun(OldRate) -> OldRate + Rate end,
                                   Rate, Map)
          end, #{}, NodesRates),
    MergedRatesList = maps:to_list(MergedRates),
    case length(MergedRatesList) < length(NodesRates) of
        true ->
            io:format("~n~n -- !! WARNING: Some of the rates where merged "
                      "since they were about the same tag and the same node~n~n~n", []);
        false ->
            ok
    end,
    [ {Node, Tag, Rate} || {{Node, Tag}, Rate} <- MergedRatesList].


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
