
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
-type temp_setup_tree() :: {State::any(), mailbox(), message_predicate(), spec_functions(), [temp_setup_tree()]}.
