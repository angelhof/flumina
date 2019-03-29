-module(conf_gen).

-export([generate/3,
	 make_specification/4,
	 make_topology/2,
	 get_state_type_tags_upd/2,
	 get_state_types_map/1, 
	 get_split_merge_funs/1,
	 get_dependencies/1, 
	 get_init_state/1, 
	 get_nodes_rates/1, 
	 get_implementation_tags/1,
	 get_sink_pid/1
	]).

-include("type_definitions.hrl").

%%
%% This module is the configuration generation frontend
%% and contains the functions related to the data structures
%% specifying the computation and the topology of the network.
%%

-spec generate(specification(), topology(), atom()) -> pid_tree().
generate(Specification, Topology, OptimizerModule) ->
    SetupTree = gen_setup_tree(Specification, Topology, OptimizerModule),
    Dependencies = get_dependencies(Specification),
    SinkPid = get_sink_pid(Topology),
    configuration:create(SetupTree, Dependencies, SinkPid).


-spec gen_setup_tree(specification(), topology(), atom()) -> temp_setup_tree().
gen_setup_tree(Specification, Topology, OptimizerModule) ->
    %% This calls the given optimizer to generate a setup tree
    OptimizerModule:generate_setup_tree(Specification, Topology).

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

-spec get_state_type_tags_upd(state_type_name(), specification()) -> {sets:set(tag()), update_fun()}.
get_state_type_tags_upd(StateType, Specification) ->
    StateTypesMap = get_state_types_map(Specification),
    maps:get(StateType, StateTypesMap).

-spec get_state_types_map(specification()) -> state_types_map().
get_state_types_map({StateTypes, _SplitMerges, _Dependencies, _InitState}) ->
    StateTypes.

-spec get_split_merge_funs(specification()) -> split_merge_funs().
get_split_merge_funs({_StateTypes, SplitMerges, _Dependencies, _InitState}) ->
    SplitMerges.

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
