-module(configuration_gen).

-export([generate/2]).

-include("type_definitions.hrl").

-type state_type_name() :: atom().
-type state_type_triple() :: {state_type_name(), 
			      state_type_name(), 
			      state_type_name()}.
-type split_merge_funs() :: {split_fun(), merge_fun()}.
-type tags() :: [tag()].

-type specification() :: 
        { %% For each tag there exists a maximum subset
	  %% of tags that it can handle, as well as an
	  %% update function
	  #{state_type_name() := {tags(), update_fun()}}, 
	  %% A possibly empty list of splits and merges,
	  %% and the state types that they are done from
	  %% and to.
	  [{state_type_triple(), split_merge_funs()}],
	  %% The dependencies
	  dependencies(),
	  %% The initial state
	  any()
	}.
-type topology() ::
	{ %% An association list containing triples of
	  %% a node pid, an implementation tag, and the input rate
	  [{mailbox(), tag(), non_neg_integer()}],
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
    undef.

-spec get_dependencies(specification()) -> dependencies().
get_dependencies({_StateTypes, _SplitMerges, Dependencies, _InitState}) ->
    Dependencies.

-spec get_sink_pid(topology()) -> mailbox().
get_sink_pid({_Rates, SinkPid}) ->
    SinkPid.
