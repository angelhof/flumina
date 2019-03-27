-module(configuration_gen).

-export([generate/2,
	 make_specification/4,
	 make_topology/2]).

-include("type_definitions.hrl").

-type state_type_name() :: atom().
-type state_type_triple() :: {state_type_name(), 
			      state_type_name(), 
			      state_type_name()}.
-type split_merge() :: {split_fun(), merge_fun()}.
-type split_merge_fun() :: {state_type_triple(), split_merge()}.
-type split_merge_funs() :: [split_merge_fun()].

%% TODO: I am not sure whether this should be about processes in nodes,
%%       or whether it should talk about whole nodes
-type nodes_rates() :: [{mailbox(), tag(), non_neg_integer()}].
-type state_types_map() :: #{state_type_name() := {tags(), update_fun()}}.
-type state_type_pair() :: {state_type_name(), State::any()}.
-type tags() :: [tag()].


-type specification() :: 
        { %% For each tag there exists a maximum subset
	  %% of tags that it can handle, as well as an
	  %% update function
	  state_types_map(), 
	  %% A possibly empty list of splits and merges,
	  %% and the state types that they are done from
	  %% and to.
	  split_merge_funs(),
	  %% The dependencies
	  dependencies(),
	  %% The initial state, and its type
	  state_type_pair()
	}.
-type topology() ::
	{ %% An association list containing triples of
	  %% a node pid, an implementation tag, and the input rate
	  nodes_rates(),
	  %% The sink pid
	  mailbox()
	}.


-spec generate(specification(), topology()) -> pid_tree().
generate(Specification, Topology) ->
    SetupTree = gen_setup_tree(Specification, Topology),
    Dependencies = get_dependencies(Specification),
    SinkPid = get_sink_pid(Topology),
    configuration:create(SetupTree, Dependencies, SinkPid).


-spec gen_setup_tree(specification(), topology()) -> temp_setup_tree().
gen_setup_tree(Specification, Topology) ->
    %% At the moment it just calls the trivial sequential optimizer
    sequential_setup_tree(Specification, Topology).

-spec sequential_setup_tree(specification(), topology()) -> temp_setup_tree().
sequential_setup_tree(Specification, Topology) ->
    NodesRates = get_nodes_rates(Topology),
    MaxRateNode = max_rate_node(NodesRates),
    AllTags = get_implementation_tags(Topology),
    {InitStateType, InitState} = get_init_state(Specification),
    %% Assert that the initial state type can handle all tags
    true = can_state_type_handle_tags(InitStateType, AllTags, Specification),
    {_, UpdateFun} = get_state_type_tags_upd(InitStateType, Specification),
    
    %% Make the setup tree
    Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    AllTagsPredicate = tags_to_predicate(AllTags),
    Node = {InitState, MaxRateNode, AllTagsPredicate, Funs, []},
    Node.
    
    
-spec max_rate_node(nodes_rates()) -> mailbox().
max_rate_node([{NameNode, _Tag, _Rate}|_]) ->
    %% TODO: Implement this by summing the rate for each
    %%       node, and returning the one with the maximum rate
    NameNode.

-spec can_state_type_handle_tags(state_type_name(), tags(), specification()) -> boolean().
can_state_type_handle_tags(StateType, Tags, Specification) ->
    {HandledTags, _UpdFun} = get_state_type_tags_upd(StateType, Specification),
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

%%
%% Getters (and Setters??) for Specification and Topology structures
%%


-spec make_specification(state_types_map(), 
			 split_merge_funs(), 
			 dependencies(), 
			 state_type_pair()) 
			-> specification().
make_specification(StateTypesMap, SplitMergeFuns, Dependencies, StateTypePair) ->
    {StateTypesMap, SplitMergeFuns, Dependencies, StateTypePair}.

-spec make_topology(nodes_rates(), 
		    mailbox()) 
		   -> topology().
make_topology(NodesRates, SinkNode) ->
    {NodesRates, SinkNode}.

-spec get_state_type_tags_upd(state_type_name(), specification()) -> {tags(), update_fun()}.
get_state_type_tags_upd(StateType, Specification) ->
    StateTypesMap = get_state_types_map(Specification),
    maps:get(StateType, StateTypesMap).

-spec get_state_types_map(specification()) -> state_types_map().
get_state_types_map({StateTypes, _SplitMerges, _Dependencies, _InitState}) ->
    StateTypes.

-spec get_dependencies(specification()) -> dependencies().
get_dependencies({_StateTypes, _SplitMerges, Dependencies, _InitState}) ->
    Dependencies.

-spec get_init_state(specification()) -> state_type_pair().
get_init_state({_StateTypes, _SplitMerges, _Dependencies, InitState}) ->
    InitState.

-spec get_nodes_rates(topology()) -> nodes_rates().
get_nodes_rates({NodesRates, _SinkPid}) ->
    NodesRates.

-spec get_implementation_tags(topology()) -> tags().
get_implementation_tags(Topology) ->
    NodesRates = get_nodes_rates(Topology),
    [Tag || {_Node, Tag, _Rate} <- NodesRates].

-spec get_sink_pid(topology()) -> mailbox().
get_sink_pid({_Rates, SinkPid}) ->
    SinkPid.
