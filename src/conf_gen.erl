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
	 get_sink_pid/1,
	 default_options/0,
	 no_checkpoint/2,
	 always_checkpoint/2,
	 binary_always_checkpoint/2
	]).

-include("type_definitions.hrl").

%%
%% This module is the configuration generation frontend
%% and contains the functions related to the data structures
%% specifying the computation and the topology of the network.
%%
-spec generate(specification(), topology(), conf_gen_options()) -> configuration().
generate(Specification, Topology, Options) ->
    OptionsRecord = update_options(Options, default_options()),
    generate0(Specification, Topology, OptionsRecord).

-spec generate0(specification(), topology(), conf_gen_options_rec()) -> configuration().
generate0(Specification, Topology, #options{optimizer = OptimizerModule} = OptionsRec) ->
    SetupTree = gen_setup_tree(Specification, Topology, OptimizerModule),
    Dependencies = get_dependencies(Specification),
    SinkPid = get_sink_pid(Topology),
    ImplTags = get_implementation_tags(Topology),
    configuration:create(SetupTree, Dependencies, OptionsRec, SinkPid, ImplTags).


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

-spec get_implementation_tags(topology()) -> impl_tags().
get_implementation_tags(Topology) ->
    NodesRates = get_nodes_rates(Topology),
    [{Tag, Node} || {Node, Tag, _Rate} <- NodesRates].

-spec get_sink_pid(topology()) -> mailbox().
get_sink_pid({_Rates, SinkPid}) ->
    SinkPid.

-spec update_options(conf_gen_options(), conf_gen_options_rec())
		    -> conf_gen_options_rec().
update_options(Options, OptionsRecord) ->
    lists:foldl(fun update_option/2, OptionsRecord, Options).

-spec update_option(conf_gen_option(), conf_gen_options_rec())
		    -> conf_gen_options_rec().
update_option({optimizer, Value}, OptionsRecord) ->
    OptionsRecord#options{optimizer = Value};
update_option({log_triple, Value}, OptionsRecord) ->
    OptionsRecord#options{log_triple = Value};
update_option({checkpoint, Value}, OptionsRecord) ->
    OptionsRecord#options{checkpoint = Value};
update_option(Option, OptionsRecord) ->
    util:err("Unknown option ~p in module ~p~n", [Option, ?MODULE]),
    util:crash(1,1).

-spec default_options() -> conf_gen_options_rec().
default_options() ->
    #options{
       optimizer = optimizer_greedy,
       log_triple = log_mod:no_log_triple(),
       checkpoint = fun no_checkpoint/2}.


-spec no_checkpoint(gen_message_or_merge(), State::any()) -> checkpoint_predicate().
no_checkpoint(_Msg, _State) ->
    fun(Msg, State) ->
	    no_checkpoint(Msg, State)
    end.

%% This function checkpoints the state to a file in a human readable form
-spec always_checkpoint(gen_message_or_merge(), State::any()) -> checkpoint_predicate().
always_checkpoint({MsgOrMerge, {_Tag, Ts, _V}}, State) ->
    Filename =
        io_lib:format("logs/checkpoint_~s_~s_messages.log",
		      [pid_to_list(self()), atom_to_list(node())]),
    ok = file:write_file(Filename, io_lib:format("~p.~n", [{Ts, State}])),
    fun(Msg1, State1) ->
	    always_checkpoint(Msg1, State1)
    end.

%% This function checkpoints the state to a file in a binary form.
%% It is faster than the one above, but it is not readable.
-spec binary_always_checkpoint(gen_message_or_merge(), State::any()) -> checkpoint_predicate().
binary_always_checkpoint({MsgOrMerge, {_Tag, Ts, _V}}, State) ->
    Filename =
        io_lib:format("logs/binary_checkpoint_~s_~s_messages.log",
		      [pid_to_list(self()), atom_to_list(node())]),
    ok = file:write_file(Filename, term_to_binary({Ts, State})),
    fun(Msg1, State1) ->
	    binary_always_checkpoint(Msg1, State1)
    end.
