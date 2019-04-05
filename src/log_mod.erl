-module(log_mod).

-export([initialize_message_logger_state/2,
	 maybe_log_message/2,
	 no_message_logger/0]).

-include("type_definitions.hrl").

-spec initialize_message_logger_state(string(), sets:set(tag())) -> message_logger_log_fun().
initialize_message_logger_state(Prefix, Tags) ->
    Filename =
        io_lib:format("logs/~s_~s_~s_messages.log", 
		      [Prefix, pid_to_list(self()), atom_to_list(node())]), 
    {ok, IoDevice} = file:open(Filename, [append]),
    ok = file:truncate(IoDevice),
    fun(Msg) ->
	    maybe_log_message(Msg, {Tags, IoDevice})
    end.
    
%% Generalize the predicate to be anything instead of just a tag set
%% WARNING: At the moment this only logs messages, not heartbeats
-spec maybe_log_message(gen_message(), message_logger_state()) -> 'ok'.
maybe_log_message({Tag, _, _} = Msg, {Tags, _} = LoggerState) ->
    case sets:is_element(Tag, Tags) of
	true ->
	    log_message(Msg, LoggerState);
	false ->
	    ok
    end;
maybe_log_message(Msg, LoggerState) ->
    ok.

-spec log_message(gen_message(), message_logger_state()) -> 'ok'.
log_message(Msg, {_Tags, File}) ->
    %% WARNING: The timestamp is monotonic, so we should only 
    %%          compare timestamps taken on the same machine
    %% TODO: Maybe we should change this to os:timestamp
    CurrentTimestamp = erlang:monotonic_time(),
    PidNode = {self(), node()},
    Data = io_lib:format("~w~n", [{Msg, PidNode, CurrentTimestamp}]),
    ok = file:write(File, Data),
    ok.


-spec no_message_logger() -> message_logger_log_fun().
no_message_logger() ->
    fun(_Msg) -> ok end.

