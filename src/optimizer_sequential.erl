-module(optimizer_sequential).

-export([generate_setup_tree/2]).

-include("type_definitions.hrl").

%%
%% This is the trivial sequential optimizer that just allocates
%% a sequential configuration at the node with the highest input rate
%%

-spec generate_setup_tree(specification(), topology()) -> temp_setup_tree().
generate_setup_tree(Specification, Topology) ->
    NodesRates = conf_gen:get_nodes_rates(Topology),
    MaxRateNode = max_rate_node(NodesRates),
    AllTags = conf_gen:get_implementation_tags(Topology),
    {InitStateType, InitState} = conf_gen:get_init_state(Specification),
    %% Assert that the initial state type can handle all tags
    true = can_state_type_handle_tags(InitStateType, AllTags, Specification),
    {_, UpdateFun} = conf_gen:get_state_type_tags_upd(InitStateType, Specification),
    
    %% Make the setup tree
    Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    AllTagsPredicate = tags_to_predicate(AllTags),
    Node = {InitState, MaxRateNode, AllTagsPredicate, Funs, []},
    Node.
    
    
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
    

-spec can_state_type_handle_tags(state_type_name(), tags(), specification()) -> boolean().
can_state_type_handle_tags(StateType, Tags, Specification) ->
    {HandledTags, _UpdFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    HandledTagsSet = sets:from_list(HandledTags),
    TagsSet = sets:from_list(Tags),
    sets:is_subset(TagsSet, HandledTagsSet).

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
