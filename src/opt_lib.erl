-module(opt_lib).

-export([tags_to_predicate/1,
	 can_state_type_handle_tags/3,
	 max_rate_node/1,
	 map_physical_node_root_tree_constant/2,
	 map_physical_node_root_tree_max_rate/2,
	 filter_tags_in_nodes_rates/2]).

-include("type_definitions.hrl").

%%
%% This is a library with code that is common to 
%% several optimizers
%%

%% TODO: This will eventually disappear when predicates 
%%       become sets of tags
-spec tags_to_predicate(tags()) -> message_predicate().
tags_to_predicate(Tags) ->
    fun({MTag, _, _}) ->
	    lists:any(
	      fun(Tag) -> 
		      MTag =:= Tag
	      end,Tags)
    end.

-spec can_state_type_handle_tags(state_type_name(), sets:set(tag()), specification()) -> boolean().
can_state_type_handle_tags(StateType, Tags, Specification) ->
    {HandledTagsSet, _UpdFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    sets:is_subset(Tags, HandledTagsSet).

-spec max_rate_node(nodes_rates()) -> mailbox().
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
-spec map_physical_node_root_tree_constant(mailbox(), tag_root_tree()) -> root_tree().
map_physical_node_root_tree_constant(SinkNode, {Tags, Children}) ->
    MappedChildren = [map_physical_node_root_tree_constant(SinkNode, C) || C <- Children],
    {{Tags, SinkNode}, MappedChildren}.

%% This function maps each root tree node to the node which has the highest rate
%% for the tags handled by this node.
-spec map_physical_node_root_tree_max_rate(nodes_rates(), tag_root_tree()) -> root_tree().
map_physical_node_root_tree_max_rate(NodesRates, {Tags, Children}) ->
    MappedChildren = [map_physical_node_root_tree_max_rate(NodesRates, C) || C <- Children],
    RelevantNodesRates = filter_tags_in_nodes_rates(Tags, NodesRates),
    MaxRateNode = max_rate_node(RelevantNodesRates),
    {{Tags, MaxRateNode}, MappedChildren}.

%% This function filters and keeps only the node-tag-rate triples that contain
%% a tag in the given list of tags.
-spec filter_tags_in_nodes_rates(tags(), nodes_rates()) -> nodes_rates().
filter_tags_in_nodes_rates(Tags, NodesRates) ->
    TagsSet = sets:from_list(Tags),
    [{Node, Tag, Rate} || {Node, Tag, Rate} <- NodesRates, sets:is_element(Tag, TagsSet)].
