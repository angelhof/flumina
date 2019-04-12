
-type tag() :: any().

-type message(Tag, Payload) :: {Tag, integer(), Payload}.
-type gen_message() :: message(tag(), any()).

-type merge_request(Tag) :: {'merge', {Tag, integer(), Father::pid()}}. 
-type gen_merge_request() :: merge_request(tag()).

-type message_or_merge(Tag, Payload) :: {'msg', message(Tag, Payload)} | merge_request(Tag).
-type gen_message_or_merge() :: message_or_merge(tag(), any()).

-type heartbeat(Tag) :: {'heartbeat', {Tag, integer()}}.
-type iheartbeat(Tag) :: {'iheartbeat', {Tag, integer()}}.
-type gen_heartbeat() :: heartbeat(tag()).

-type message_or_heartbeat(Tag, Payload) :: message(Tag, Payload) | heartbeat(Tag).
-type gen_message_or_heartbeat() :: message_or_heartbeat(tag(), any()).

-type imessage_or_iheartbeat(Tag, Payload) :: {'imsg', message(Tag, Payload)} 
					    | iheartbeat(Tag).
-type gen_imessage_or_iheartbeat() :: imessage_or_iheartbeat(tag(), any()).

-type predicate() :: fun((...) -> boolean()).
-type message_predicate() :: fun((gen_message()) -> boolean()).
-type split_pred() :: message_predicate().
-type split_preds() :: {split_pred(), split_pred()}.

-type update_fun() :: fun((gen_message(), State::any(), mailbox()) -> State::any()).
-type split_fun() :: fun((split_preds(), State::any()) -> {State::any(), State::any()}).
-type merge_fun() :: fun((State::any(), State::any()) -> State::any()).
-type spec_functions() :: {update_fun(), split_fun(), merge_fun()}.



-type dependencies() :: #{tag() := [tag()]}.
-type timers() :: #{tag() := [integer()]}.
-type buffer() :: queue:queue(gen_message_or_merge()).
-type buffers() :: #{tag() := buffer()}.
-type buffers_timers() :: {buffers(), timers()}.

-type mailbox() :: {Name::atom(), node()}.

%% The configuration tree, contains the pid and the mailbox pid of each node
%% as well as a predicate that represents which messages does this node process.
-type configuration() :: {'node', Node::pid(), mailbox(), message_predicate(), [configuration()]}.

-type pid_tree() :: {{pid(), mailbox()}, [pid_tree()]}.
-type temp_setup_tree() :: {State::any(), node(), 
			    message_predicate(), spec_functions(), [temp_setup_tree()]}.

%%
%% Configuration Generator
%%

-type state_type_name() :: atom().
-type state_type_triple() :: {state_type_name(), 
			      state_type_name(), 
			      state_type_name()}.
-type split_merge() :: {split_fun(), merge_fun()}.
-type split_merge_fun() :: {state_type_triple(), split_merge()}.
-type split_merge_funs() :: [split_merge_fun()].

%% TODO: I am not sure whether this should be about processes in nodes,
%%       or whether it should talk about whole nodes
-type nodes_rates() :: [{node(), tag(), non_neg_integer()}].
-type state_types_map() :: #{state_type_name() := {sets:set(tag()), update_fun()}}.
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

-type tag_vertices() :: #{tag() := digraph:vertex()}.
-type tag_root_tree() :: {tags(), [tag_root_tree()]}.
-type root_tree() :: {{tags(), node()}, [root_tree()]}.
-type set_root_tree() :: {{sets:set(tag()), node()}, [set_root_tree()]}.
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
-type message_logger_log_fun() :: fun((gen_message()) -> 'ok').

-type num_log_state() :: integer().
-type num_log_fun() :: fun((gen_message() | gen_merge_request(), num_log_state()) -> num_log_state()).
-type reset_num_log_fun() :: fun((num_log_state()) -> num_log_state()).
%% TODO: Make a general log triple type
-type num_log_triple() :: {num_log_fun(), reset_num_log_fun(), num_log_state()}.

%%
%% Producer
%%

-type msg_generator() :: fun(() -> 'done' | {gen_message_or_heartbeat(),  msg_generator()}).
-type producer_type() :: 'constant' 
		       | 'timestamp_based' 
		       | 'steady_timestamp'.
