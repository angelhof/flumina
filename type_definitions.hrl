
-type predicate() :: fun((...) -> boolean()).
-type message_predicate() :: fun((message()) -> boolean()).
-type tag() :: any().
-type message() :: {tag(), integer(), Payload::any()}.
-type merge_request() :: {'merge', {tag(), integer(), Father::pid()}}. 
-type message_or_merge() :: {'msg', message()} | merge_request().
-type heartbeat() :: {heartbeat, {tag(), integer()}}.
-type update_fun() :: fun((message(), State::any(), pid()) -> State::any()).
-type split_fun() :: fun(({message_predicate(), message_predicate()}, State::any()) 
			 -> {State::any(), State::any()}).
-type merge_fun() :: fun((State::any(), State::any()) -> State::any()).
-type spec_functions() :: {update_fun(), split_fun(), merge_fun()}.

-type dependencies() :: #{tag() := [tag()]}.
-type timers() :: #{tag() := [integer()]}.
-type message_buffer() :: {[message_or_merge()], timers()}.
-type buffers() :: #{tag() := [message_or_merge()]}.
-type buffers_timers() :: {buffers(), timers()}.

%% The configuration tree, contains the pid and the mailbox pid of each node
%% as well as a predicate that represents which messages does this node process.
-type configuration() :: {'node', Node::pid(), Mailbox::pid(), message_predicate(), [configuration()]}.

-type pid_tree() :: {{pid(), pid()}, [pid_tree()]}.
-type temp_setup_tree() :: {State::any(), message_predicate(), spec_functions(), [temp_setup_tree()]}.
