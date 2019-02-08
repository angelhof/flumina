
-type predicate() :: fun((...) -> boolean()).
-type message() :: {atom(), integer(), Payload::any()}.
-type heartbeat() :: {heartbeat, {atom(), integer()}}.
-type update_fun() :: fun((message(), State::any(), pid()) -> State::any()).
-type split_fun() :: fun(({predicate(), predicate()}, State::any()) -> {State::any(), State::any()}).
-type merge_fun() :: fun((State::any(), State::any()) -> State::any()).
-type spec_functions() :: {update_fun(), split_fun(), merge_fun()}.

-type dependencies() :: #{atom() := [atom()]}.
-type timers() :: #{atom() := [integer()]}.
-type message_buffer() :: {[message()], timers()}.

-type configuration() :: {'node', Node::pid(), Mailbox::pid(), predicate(), [configuration()]}.

