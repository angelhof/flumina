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
    MaxRateNode = opt_lib:max_rate_node(NodesRates),
    AllImplTags = conf_gen:get_implementation_tags(Topology),
    AllTags = [Tag || {Tag, Node} <- AllImplTags],
    AllImplTagsSet = sets:from_list(AllImplTags),
    {InitStateType, InitState} = conf_gen:get_init_state(Specification),
    %% Assert that the initial state type can handle all tags
    true = opt_lib:can_state_type_handle_tags(InitStateType, AllImplTagsSet, Specification),
    {_, UpdateFun} = conf_gen:get_state_type_tags_upd(InitStateType, Specification),
    %% Make the setup tree
    Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    AllImplTagsPredicate = opt_lib:impl_tags_to_predicate(AllImplTags),
    AllSpecTagsPredicate = opt_lib:impl_tags_to_spec_predicate(AllImplTags),
    Node = {InitState, MaxRateNode, {AllSpecTagsPredicate, AllImplTagsPredicate}, Funs, []},
    Node.
    
    
    
