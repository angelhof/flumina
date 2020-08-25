-module(log_mod).

-export([initialize_message_logger_state/2,
         initialize_message_logger_state/3,
	 maybe_log_message/3,
	 no_message_logger/0,

         message_logger/1,

	 init_num_log_state/0,
	 reset_num_log_state/1,
	 incr_num_log_state/2,
	 make_num_log_triple/0,
	 no_log_triple/0,

	 num_logger_process/2,

	 init_debug_log/0,
	 debug_log/2]).

-include("type_definitions.hrl").
-include("config.hrl").

-spec initialize_message_logger_state(string(), sets:set(tag())) -> message_logger_log_fun().
initialize_message_logger_state(Prefix, Tags) ->
    initialize_message_logger_state(Prefix, Tags, node()).

-spec initialize_message_logger_state(string(), sets:set(tag()), node()) -> message_logger_log_fun().
initialize_message_logger_state(Prefix, Tags, Node) ->
    Filename =
        io_lib:format("~s/~s_~s_~s_messages.log",
		      [?LOG_DIR, Prefix, pid_to_list(self()), atom_to_list(node())]),
    Pid = spawn_link(Node, ?MODULE, message_logger, [Filename]),
    fun(Msg) ->
	    maybe_log_message(Msg, Tags, Pid)
    end.

%% Generalize the predicate to be anything instead of just a tag set
%% WARNING: At the moment this only logs messages, not heartbeats
-spec maybe_log_message(gen_impl_message(), sets:set(tag()), pid()) -> 'ok'.
maybe_log_message({{Tag, _}, _, _} = Msg, Tags, Pid) ->
    case sets:is_element(Tag, Tags) of
	true ->
            CurrentTimestamp = ?GET_SYSTEM_TIME(),
            PidNode = {self(), node()},
            Pid ! {Msg, PidNode, CurrentTimestamp},
            ok;
	false ->
	    ok
    end;
maybe_log_message(_Msg, _Tags, _Pid) ->
    ok.

%% Obsolete synchronous logging code
%% %% Generalize the predicate to be anything instead of just a tag set
%% %% WARNING: At the moment this only logs messages, not heartbeats
%% -spec old_maybe_log_message(gen_impl_message(), message_logger_state()) -> 'ok'.
%% old_maybe_log_message({{Tag, _}, _, _} = Msg, {Tags, _} = LoggerState) ->
%%     case sets:is_element(Tag, Tags) of
%% 	true ->
%% 	    log_message(Msg, LoggerState);
%% 	false ->
%% 	    ok
%%     end;
%% old_maybe_log_message(Msg, LoggerState) ->
%%     ok.

%% -spec log_message(gen_impl_message(), message_logger_state()) -> 'ok'.
%% log_message(Msg, {_Tags, File}) ->
%%     CurrentTimestamp = ?GET_SYSTEM_TIME(),
%%     %% CurrentTimestamp = erlang:system_time(nanosecond),
%%     PidNode = {self(), node()},
%%     Data = io_lib:format("~w~n", [{Msg, PidNode, CurrentTimestamp}]),
%%     ok = file:write(File, Data).

%% Have a message logger to asynchronously log messages
-spec message_logger(file:filename()) -> ok.
message_logger(Filename) ->
    %% Trap exits to empty buffer if the producer exits
    process_flag(trap_exit, true),
    filelib:ensure_dir(Filename),
    {ok, IoDevice} = file:open(Filename, [append, raw]),
    ok = file:truncate(IoDevice),
    message_logger_loop(IoDevice, [], 0).

-type message_logger_buffer() :: [string()].
-spec message_logger_loop(file:io_device(), message_logger_buffer(), integer()) -> ok.
message_logger_loop(IoDevice, Buffer, N) when N >= ?ASYNC_MESSAGE_LOGGER_BUFFER_SIZE->
    OrderedBuffer = lists:reverse(Buffer),
    FlatOutput = lists:flatten(OrderedBuffer),
    ok = file:write(IoDevice, FlatOutput),
    message_logger_loop(IoDevice, [], 0);
message_logger_loop(IoDevice, Buffer, N) ->
    receive
        {'EXIT', _, _} ->
            %% If some linked process exits, then we have to empty buffer
            message_logger_loop(IoDevice, Buffer, ?ASYNC_MESSAGE_LOGGER_BUFFER_SIZE);
        MsgToLog ->
            Data = io_lib:format("~w~n", [MsgToLog]),
            message_logger_loop(IoDevice, [Data|Buffer], N+1)
    end.

-spec no_message_logger() -> message_logger_log_fun().
no_message_logger() ->
    fun(_Msg) -> ok end.

%%
%% Number of messages loggers
%%

-spec init_num_log_state() -> integer().
init_num_log_state() -> 
    0.

-spec reset_num_log_state(num_log_state()) -> integer().
reset_num_log_state(_) -> 
    0.

-spec incr_num_log_state(gen_message() | merge_request(), num_log_state()) -> num_log_state().
incr_num_log_state(_Msg, Num) ->
    Num + 1.

-spec make_num_log_triple() -> num_log_triple().
make_num_log_triple() ->
    {fun log_mod:incr_num_log_state/2, fun log_mod:reset_num_log_state/1, init_num_log_state()}. 

-spec no_log_triple() -> num_log_triple().
no_log_triple() ->
    {fun(_,_) -> 0 end, fun(_) -> 0 end, 0}.

-spec num_logger_process(string(), configuration()) -> ok.
num_logger_process(Prefix, Configuration) ->
    io:format(" -- !!WARNING!! -- This is an obsolete method of logging throughput.~n"
              "                   Instead log total time and total number of messages!~n", []),
    register('num_messages_logger_process', self()),
    Filename =
        io_lib:format("~s/~s_~s_~s_num_messages.log", 
		      [?LOG_DIR, Prefix, pid_to_list(self()), atom_to_list(node())]),
    filelib:ensure_dir(Filename),
    {ok, IoDevice} = file:open(Filename, [append]),
    ok = file:truncate(IoDevice),
    num_logger_process_loop(IoDevice, Configuration).


-spec num_logger_process_loop(file:io_device(), configuration()) -> ok.
num_logger_process_loop(IoDevice, Configuration) ->
    %% It should be messages every 500 ms
    timer:sleep(500),

    %% Send the get_log_message to all mailboxes in the configuration
    PidMboxPairs = configuration:find_node_mailbox_pid_pairs(Configuration),
    {registered_name, MyName} = erlang:process_info(self(), registered_name),
    RequestMessage = {get_message_log, {MyName, node()}},
    [Mbox ! RequestMessage || {_Node, Mbox} <- PidMboxPairs],

    %% Receive the answer from all mailboxes in the configuration
    ReceivedLogs = receive_message_logs(PidMboxPairs, []),

    %% Log all the answers in the file (with a current timestamp)
    CurrentTimestamp = ?GET_SYSTEM_TIME(),
    append_logs_in_file(ReceivedLogs, CurrentTimestamp, IoDevice),
    num_logger_process_loop(IoDevice, Configuration).

-spec receive_message_logs([{pid(), mailbox()}], [{mailbox(), num_log_state()}])
			  -> [{mailbox(), num_log_state()}].
receive_message_logs([], Received) ->
    Received;
receive_message_logs([{NodePid, {MboxName, Node}}|Rest], Received) ->
    receive
	{message_log, {NodePid, Node}, LogState} ->
	    receive_message_logs(Rest, [{{MboxName, Node}, LogState}|Received])
    end.

-spec append_logs_in_file([{mailbox(), num_log_state()}], integer(), file:io_device()) -> ok.
append_logs_in_file(ReceivedLogs, CurrentTimestamp, IoDevice) ->
    lists:foreach(
      fun(MboxLog) ->
	      append_log_in_file(MboxLog, CurrentTimestamp, IoDevice)
      end, ReceivedLogs).

-spec append_log_in_file({mailbox(), num_log_state()}, integer(), file:io_device()) -> ok.
append_log_in_file({Mbox, Log}, CurrentTimestamp, IoDevice) ->
    Data = io_lib:format("~w~n", [{Mbox, CurrentTimestamp, Log}]),
    ok = file:write(IoDevice, Data).

%%
%% These functions are for debug logging.
%% The names of the files are generated from the pid and node
%%

%% This function creates and truncates the debug log file
-spec init_debug_log() -> ok.
-if(?DEBUG =:= true).
init_debug_log() ->
    Filename =
        io_lib:format("~s/debug_~s_~s.log",
		      [?LOG_DIR, pid_to_list(self()), atom_to_list(node())]),
    filelib:ensure_dir(Filename),
    {ok, IoDevice} = file:open(Filename, [write]),
    ok = file:truncate(IoDevice),
    ok = file:close(IoDevice).
-else.
init_debug_log() ->
    ok.
-endif.

-spec debug_log(string(), [any()]) -> ok.
-if(?DEBUG =:= true).
debug_log(Format, Args) ->
    Filename =
        io_lib:format("~s/debug_~s_~s.log",
		      [?LOG_DIR, pid_to_list(self()), atom_to_list(node())]),
    Data = io_lib:format(Format, Args),
    filelib:ensure_dir(Filename),
    ok = file:write_file(Filename, Data, [append]).
-else.
debug_log(_Format, _Args) ->
    ok.
-endif.
