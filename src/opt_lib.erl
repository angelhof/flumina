-module(opt_lib).

-export([impl_tags_to_predicate/1,
	 impl_tags_to_spec_predicate/1,
	 can_state_type_handle_tags/3,
	 max_rate_node/1,
	 map_physical_node_root_tree_constant/2,
	 map_physical_node_root_tree_max_rate/2,
	 filter_tags_in_nodes_rates/2,
	 temp_setup_tree_height/1]).

-include("type_definitions.hrl").

%%
%% This is a library with code that is common to 
%% several optimizers
%%

%% TODO: This will eventually disappear when predicates 
%%       become sets of tags
-spec impl_tags_to_predicate(impl_tags()) -> impl_message_predicate().
impl_tags_to_predicate(Tags) ->
    fun({{MTag, _}, MNode, _}) ->
	    lists:any(
	      fun({Tag, Node}) -> 
		      MTag =:= Tag andalso MNode =:= Node
	      end,Tags)
    end.

-spec impl_tags_to_spec_predicate(impl_tags()) -> tag_predicate().
impl_tags_to_spec_predicate(Tags) ->
    fun(MTag) ->
	    lists:any(
	      fun({Tag, _Node}) -> 
		      MTag =:= Tag
	      end,Tags)
    end.


-spec can_state_type_handle_tags(state_type_name(), sets:set(impl_tag()), specification()) -> boolean().
can_state_type_handle_tags(StateType, ImplTags, Specification) ->
    {HandledTagsSet, _UpdFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    Tags = 
	sets:from_list([Tag || {Tag, Node} <- sets:to_list(ImplTags)]),
    sets:is_subset(Tags, HandledTagsSet).

-spec max_rate_node(nodes_rates()) -> node().
max_rate_node(NodesRates) ->
    NodesTotalRates = 
	lists:foldl(
	  fun({Node, _Tag, Rate}, Acc) ->
	      maps:update_with(
		Node, fun(Rate0) ->
			      Rate0 + Rate
		      end, Rate, Acc)
	  end, #{}, NodesRates),
    SortedNodesTotalRates =
	lists:sort(
	 fun({Node1, Rate1}, {Node2, Rate2}) ->
		 Rate1 > Rate2
	 end, maps:to_list(NodesTotalRates)),
    [{MaxNode, _MaxRate}|_] = SortedNodesTotalRates,
    MaxNode.

%% This function maps the same physical node to all nodes in the root tree
%% WARNING: This never works, because mapping the same name and node means
%% that many processes will try to be registered with the same name 
%% (which clearly fails)
-spec map_physical_node_root_tree_constant(node(), tag_root_tree()) -> root_tree().
map_physical_node_root_tree_constant(SinkNode, {ImplTags, Children}) ->
    MappedChildren = [map_physical_node_root_tree_constant(SinkNode, C) || C <- Children],
    {{ImplTags, SinkNode}, MappedChildren}.

%% This function maps each root tree node to the node which has the highest rate
%% for the tags handled by this node.
-spec map_physical_node_root_tree_max_rate(nodes_rates(), tag_root_tree()) -> root_tree().
map_physical_node_root_tree_max_rate(NodesRates, {ImplTags, Children}) ->
    MappedChildren = [map_physical_node_root_tree_max_rate(NodesRates, C) || C <- Children],
    RelevantNodesRates = filter_tags_in_nodes_rates(ImplTags, NodesRates),
    MaxRateNode = max_rate_node(RelevantNodesRates),
    {{ImplTags, MaxRateNode}, MappedChildren}.

%% This function filters and keeps only the node-tag-rate triples that contain
%% a tag in the given list of tags.
-spec filter_tags_in_nodes_rates(impl_tags(), nodes_rates()) -> nodes_rates().
filter_tags_in_nodes_rates(ImplTags, NodesRates) ->
    ImplTagsSet = sets:from_list(ImplTags),
    [{Node, Tag, Rate} || {Node, Tag, Rate} <- NodesRates, sets:is_element({Tag, Node}, ImplTagsSet)].

%% Returns the height of a temp_setup_tree
-spec temp_setup_tree_height(temp_setup_tree()) -> integer().
temp_setup_tree_height({_State, _Node, _Predicate, _Funs, []}) ->
    0;
temp_setup_tree_height({_State, _Node, _Predicate, _Funs, Children}) ->
    1 + lists:max([temp_setup_tree_height(C) || C <- Children]).
