
%%
%% Experiments
%%

-type experiment_opt() :: {'sink_name', mailbox()}
                        | {'optimizer_type', optimizer_type()}
                        | {'producer_options', producer_options()}
                        | {'sink_options', sink_options()}
                        | {'experiment_args', any()}.
-type experiment_opts() :: [experiment_opt()].

%%
%% Specification
%%

-type tag() :: any().

%% These are the specification messages that the user sees
-type message(Tag, Payload) :: {Tag, Payload}.
-type gen_message() :: message(tag(), any()).

-type predicate() :: fun((...) -> boolean()).
-type message_predicate() :: fun((gen_message()) -> boolean()).
-type tag_predicate() :: fun((tag()) -> boolean()).
-type split_pred() :: tag_predicate().
-type split_preds() :: {split_pred(), split_pred()}.

-type update_fun() :: fun((gen_message(), State::any(), mailbox()) -> State::any()).
-type split_fun() :: fun((split_preds(), State::any()) -> {State::any(), State::any()}).
-type merge_fun() :: fun((State::any(), State::any()) -> State::any()).
-type spec_functions() :: {update_fun(), split_fun(), merge_fun()}.

-type dependencies() :: #{tag() := [tag()]}.

-type state_type_name() :: atom().
-type state_type_triple() :: {state_type_name(),
			      state_type_name(),
			      state_type_name()}.
-type split_merge() :: {split_fun(), merge_fun()}.
-type split_merge_fun() :: {state_type_triple(), split_merge()}.
-type split_merge_funs() :: [split_merge_fun()].

-type state_types_map() :: #{state_type_name() := {sets:set(tag()), update_fun()}}.
-type state_type_pair() :: {state_type_name(), State::any()}.
-type tags() :: [tag()].

%%
%% Implementation
%%

-type timestamp() :: integer().

%% These are the internal messages, extending simple messages with timestamps and node id
-type impl_message(Tag, Payload) :: {message(Tag, Payload), node(), timestamp()}.
-type gen_impl_message() :: impl_message(tag(), any()).

-type merge_request() :: {'merge', {{tag(), Father::pid()}, node(), timestamp()}}.

-type message_or_merge(Tag, Payload) :: {'msg', impl_message(Tag, Payload)} | merge_request().
-type gen_message_or_merge() :: message_or_merge(tag(), any()).

-type heartbeat(ImplTag) :: {'heartbeat', {ImplTag, timestamp()}}.
-type iheartbeat(ImplTag) :: {'iheartbeat', {ImplTag, timestamp()}}.
-type gen_heartbeat() :: heartbeat(impl_tag()).

-type message_or_heartbeat(Tag, Payload) :: impl_message(Tag, Payload) | heartbeat(Tag).
-type gen_message_or_heartbeat() :: message_or_heartbeat(tag(), any()).

-type imessage_or_iheartbeat(Tag, Payload) :: {'imsg', impl_message(Tag, Payload)}
					    | iheartbeat(Tag).
-type gen_imessage_or_iheartbeat() :: imessage_or_iheartbeat(tag(), any()).

-type impl_tag() :: {tag(), node()}.
-type impl_tags() :: [impl_tag()].

-type impl_message_predicate() :: fun((gen_impl_message()) -> boolean()).

-type timers() :: #{impl_tag() := [integer()]}.
-type buffer() :: queue:queue(gen_message_or_merge()).
-type buffers() :: #{impl_tag() := buffer()}.
-type buffers_timers() :: {buffers(), timers()}.

-type mailbox() :: {Name::atom(), node()}.
-type node_and_mailbox() :: {NodeName::atom(), MailboxName::atom(), node()}.

-type children_predicates() :: {[message_predicate()], [impl_message_predicate()]}.

-type impl_dependencies() :: #{impl_tag() := [impl_tag()]}.

%% The configuration tree, contains the pid and the mailbox pid of each node
%% as well as a predicate that represents which messages does this node process.
-type configuration() :: {'node', Node::pid(), NodeNameNode::mailbox(), MboxNameNode::mailbox(),
			  {tag_predicate(), impl_message_predicate()}, [configuration()]}.

-type pid_tree() :: {{pid(), node_and_mailbox()}, [pid_tree()]}.
-type temp_setup_tree() :: {State::any(), node(), {tag_predicate(), impl_message_predicate()},
			    spec_functions(), [temp_setup_tree()]}.

%%
%% Mailbox State
%%

%% This is a record of the mailbox state
-record(mb_st, {buffers :: buffers_timers(),
                deps :: impl_dependencies(),
                pred :: impl_message_predicate(),
                attachee :: pid(),
                conf :: configuration()}).
-type mailbox_state() :: #mb_st{}.

%%
%% Worker State
%%

-record(wr_funs, {upd = undefined :: update_fun(),
                  spl = undefined :: split_fun(),
                  mrg = undefined :: merge_fun()}).

-record(wr_st, {state :: any(),
                funs :: #wr_funs{},
                log  :: num_log_triple(),
                cp_pred :: checkpoint_predicate(),
                output :: mailbox(),
                child_preds :: children_predicates(),
                conf :: configuration()}).
-type worker_state() :: #wr_st{}.


%%
%% Configuration Generator
%%

-type optimizer_type() :: module().
-type checkpoint_predicate() :: fun((gen_message_or_merge(), State::any()) -> checkpoint_predicate()).

%% This record contains all the configurable
%% options of the configuration generator.
-record(options, {optimizer  :: optimizer_type(),
		  log_triple :: num_log_triple(),
		  checkpoint :: checkpoint_predicate()}).

-type conf_gen_option() :: {'optimizer', optimizer_type()}
			 | {'log_triple', num_log_triple()}
			 | {'checkpoint', checkpoint_predicate()}
                         | {'specification_arg', any()}.
-type conf_gen_options() :: [conf_gen_option()].
-type conf_gen_options_rec() :: #options{}.

%% TODO: I am not sure whether this should be about processes in nodes,
%%       or whether it should talk about whole nodes
-type nodes_rates() :: [{node(), tag(), non_neg_integer()}].

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

-type tag_vertices() :: #{impl_tag() := digraph:vertex()}.
-type tag_root_tree() :: {impl_tags(), [tag_root_tree()]}.
-type root_tree() :: {{impl_tags(), node()}, [root_tree()]}.
-type set_root_tree() :: {{sets:set(impl_tag()), node()}, [set_root_tree()]}.
-type hole_setup_tree() :: {state_type_pair(), set_root_tree(),
			    fun((temp_setup_tree()) -> temp_setup_tree())}.

-type name_seed() :: integer().

%%
%% Logger
%%

-type message_logger_state() :: { sets:set(tag()), %% The set of tags to log
				  file:io_device() %% The file at which the event will be logged
				}.

-type message_logger_init_fun() :: fun(() -> message_logger_log_fun()).
-type message_logger_log_fun() :: fun((gen_impl_message()) -> 'ok').

-type num_log_state() :: integer().
-type num_log_fun() :: fun((gen_impl_message() | merge_request(), num_log_state()) -> num_log_state()).
-type reset_num_log_fun() :: fun((num_log_state()) -> num_log_state()).
%% TODO: Make a general log triple type
-type num_log_triple() :: {num_log_fun(), reset_num_log_fun(), num_log_state()}.

%%
%% Producer
%%

-type msg_generator() :: fun(() -> 'done' | {gen_message_or_heartbeat(),  msg_generator()}).
-type msg_generator_init() :: {fun((...) -> msg_generator()), Args::[any()]}.
-type producer_type() :: 'constant'
		       | 'timestamp_based'
		       | 'steady_retimestamp_old'
		       | 'steady_retimestamp'
		       | 'steady_timestamp'
		       | 'steady_sync_timestamp'.
-type producer_init(Tags) :: [{msg_generator_init(), {Tags, node()}, integer()}].
-type gen_producer_init() :: producer_init(tag()).


-type producer_option() :: {'producer_type', producer_type()}
                         | {'log_tags', [tag()]}
                         | {'message_logger_init_fun', message_logger_init_fun()}
                         | {'producers_begin_time', integer()}.

-type producer_options() :: [producer_option()].

-type sink_option() :: {'log_tags', [tag()]}
                     | {'message_logger_init_fun', message_logger_init_fun()}
                     | {'sink_wait_time', integer()}.
-type sink_options() :: [sink_option()].

-type input_file_parser() :: fun((string()) -> gen_message_or_heartbeat()).

%%
%% Experiment setup
%%

%% Tag, number of nodes with this tag, rate, and heartbeat rate.
-type node_setup_info() :: [{non_neg_integer(), tag(), float(), float()}].
-type expanded_node_setup_info() :: [{tag(), float(), float()}].
