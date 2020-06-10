-module(util).

-export([err/2,
	 crash/2,
	 exec/1,
	 sink/0,
	 sink/1,
	 sink_no_log/1,
         sink/2,
         run_experiment/3,
         log_time_and_number_of_messages_before_producers_spawn/2,
	 merge_with/3,
	 take_at_most/2,
	 map_focus/2,
	 nothing/0,
	 always_ok/1,
         unregister_if_registered/0,
         reregister_if_registered/1,
	 unregister_names/1,
	 local_timestamp/0,
	 list_to_number/1,
	 do_n_times/2,
	 do_n_times/3,
	 intfloor/1,
	 intmod/2,
	 intdiv/2,
	 split_map/2,
         is_subset/2,
         is_sublist/2,
         cartesian/1]).

-include("type_definitions.hrl").
-include("config.hrl").

err(Format, Args) ->
    io:format(" -- ERROR: " ++ Format, Args).

crash(_, _) ->
    err("CrashFunction/2 was called!!", []),
    erlang:halt(1).

exec([ModuleStr, FunctionStr, ArgsStr]) ->
    Module = parse(ModuleStr),
    Function = parse(FunctionStr),
    Args = parse(ArgsStr),
    apply(Module, Function, Args).

parse(Str) ->
    {ok,Tokens,_EndLine} = erl_scan:string(Str),
    {ok,AbsForm} = erl_parse:parse_exprs(Tokens),
    {value,Value,_Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    Value.

-spec sink_get_option(atom(), sink_options()) -> sink_option().
sink_get_option(message_logger_init_fun, Options) ->
    case sink_get_option0(log_tags, Options) of
        {log_tags, []} ->
            sink_get_option0(message_logger_init_fun, Options);
        {log_tags, Tags} ->
            %% It shouldn't be the case that both log tags and
            %% message logger init fun have been given.
            false = lists:keyfind(message_logger_init_fun, 1, Options),
            {message_logger_init_fun,
             fun() ->
                     log_mod:initialize_message_logger_state("sink", sets:from_list(Tags))
             end}
    end;
sink_get_option(Option, Options) ->
    sink_get_option0(Option, Options).

-spec sink_get_option0(atom(), sink_options()) -> sink_option().
sink_get_option0(Option, Options) ->
    case lists:keyfind(Option, 1, Options) of
        false ->
            {Option, sink_default_option(Option)};
        Result ->
            Result
    end.

-spec sink_default_option(atom()) -> any().
sink_default_option(log_tags) ->
    [];
sink_default_option(sink_wait_time) ->
    ?SINK_WAITING_TIME_MS;
sink_default_option(message_logger_init_fun) ->
    fun log_mod:no_message_logger/0.


-spec sink() -> 'ok'.
sink() ->
    sink0([]).

-spec sink({'log_tags', [tag()]} | message_logger_init_fun() | sink_options()) -> 'ok'.
sink({log_tags, Tags}) ->
    sink0([{log_tags, Tags}]);
sink(SinkOptions) when is_list(SinkOptions) ->
    sink0(SinkOptions);
sink(MsgLoggerInitFun) ->
    sink0([{message_logger_init_fun, MsgLoggerInitFun}]).


sink_no_log(WaitTime) ->
    sink0([{sink_wait_time, WaitTime}]).

sink(MsgLoggerInitFun, WaitTime) ->
    sink0([{message_logger_init_fun, MsgLoggerInitFun},
          {sink_wait_time, WaitTime}]).

-spec sink0(sink_options()) -> 'ok'.
sink0(Options) ->
    {message_logger_init_fun, MsgLoggerInitFun} =
        sink_get_option(message_logger_init_fun, Options),
    {sink_wait_time, WaitTime} =
        sink_get_option(sink_wait_time, Options),
    LoggerFun = MsgLoggerInitFun(),
    receive
	finished ->
	    io:format("Configuration done~n", []),
            log_sink_configuration_finish_time(),
	    sink_loop(LoggerFun, WaitTime, undefined);
        {finished, SinkMetadata} ->
            io:format("Configuration done~n", []),
            log_sink_configuration_finish_time(),
            #sink_metadata{producer_pids=Pids, conf=ConfTree} = SinkMetadata,
            link_pids(Pids),
            maybe_setup_profiling(ConfTree),
	    sink_loop(LoggerFun, WaitTime, ConfTree)
    end.

sink_loop(LoggerFun, WaitTime, ConfTree) ->
    receive
	Msg ->
	    LoggerFun({Msg, fake_node, 0}),
	    io:format("~p~n", [Msg]),
	    sink_loop(LoggerFun, WaitTime, ConfTree)
    after
        WaitTime ->
            log_sink_finish_time(WaitTime),
            maybe_stop_profiling(ConfTree),
            io:format("Didn't receive anything for ~p seconds~n", [WaitTime]),
	    ok
    end.

-spec maybe_setup_profiling(configuration()) -> 'ok'.
maybe_setup_profiling(ConfTree) ->
    %% TODO: Gather all nodes and names from the configuration tree
    %% For each node, do an rpc

    %% %% Profile if compiled with that option
    %% maybe_profile(fun start_profiler/0, []),
    %% maybe_profile(fun profile_node_mailbox/1, [ConfTree]),

    Filename =
        io_lib:format("~s/profiling_~s_~s.log",
		      [?LOG_DIR, pid_to_list(self()), atom_to_list(node())]),
    eep:start_file_tracing(Filename),
    ok.

-spec maybe_stop_profiling(configuration() | 'undefined') -> 'ok'.
maybe_stop_profiling(ConfTree) ->
    %% TODO: Gather all nodes from the configuration tree
    %% For each node, do an rpc and ask to dump profiling info
    eep:stop_tracing(),
    %% Filename =
    %%     io_lib:format("~s/profiling_~s_~s.log",
    %%     	      [?LOG_DIR, pid_to_list(self()), atom_to_list(node())]),
    %% eep:convert_tracing(Filename),
    ok.


-spec maybe_profile(fun(), [any()]) -> 'ok'.
maybe_profile(Func, Args) ->
    case ?PROFILE of
        true ->
            apply(Func, Args);
        false ->
            ok
    end.

-spec start_profiler() -> 'ok'.
start_profiler() ->
    eprof:start(),
    Filename =
        io_lib:format("~s/profiling_~s_~s.log",
		      [?LOG_DIR, pid_to_list(self()), atom_to_list(node())]),
    eprof:log(Filename).

-spec profile_node_mailbox(configuration()) -> 'ok'.
profile_node_mailbox(ConfTree) ->
    %% Find pid of mailbox
    Self = self(),
    Node = configuration:find_node(Self, ConfTree),
    {MboxName, _MboxNode} = configuration:get_mailbox_name_node(Node),

    profiling = eprof:start_profiling([Self, MboxName]),
    ok.

-spec output_profiling(configuration()) -> 'ok'.
output_profiling(ConfTree) ->
    profiling_stopped = eprof:stop_profiling(),
    eprof:analyze(),
    profile_node_mailbox(ConfTree).




%% Interface function that runs an experiment with a sink
-spec run_experiment(module(), Fun::atom(), experiment_opts()) -> 'ok'.
run_experiment(Module, Function, Options) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    Options1 = [{sink_name, SinkName}|Options],
    io:format("Mod: ~p, Fun: ~p, Opts: ~p~n", [Module, Function, Options1]),
    _ExecPid = spawn_link(Module, Function, [Options1]),
    case lists:keyfind(sink_options, 1, Options1) of
        {sink_options, SinkOptions} ->
            util:sink(SinkOptions);
        false ->
            util:sink([])
    end.

-spec log_time_and_number_of_messages_before_producers_spawn(string(), integer()) -> 'ok'.
log_time_and_number_of_messages_before_producers_spawn(Prefix, NumberOfMessages) ->
    %% Setup the statistics filename
    StatsFilename = "logs/experiment_stats.log",
    %% Log the time before the producers have been spawned and the
    %% number of events that will be sent in total.
    MonotonicBeforeProducersTimestamp = erlang:monotonic_time(),
    BeforeProducersTimestamp = ?GET_SYSTEM_TIME(),
    StatsData = io_lib:format("~s -- Time before spawning producers: ~p, Number of messages: ~p~n"
                              "   -- Monotonic time before spawning producers: ~p~n",
                              [Prefix, BeforeProducersTimestamp, NumberOfMessages, BeforeProducersTimestamp]),
    ok = file:write_file(StatsFilename, StatsData).


%% This can be used as an approximation for throughput, but it is not
%% exact, since producers might have already started producing values,
%% or they might wait to output the first message.
-spec log_sink_configuration_finish_time() -> ok.
log_sink_configuration_finish_time() ->
    Filename =
        io_lib:format("~s/sink_stats.log",
		      [?LOG_DIR]),
    CurrentTimestamp = ?GET_SYSTEM_TIME(),
    Data = io_lib:format("Sink: ~p received a message that configuration finished at: ~p~n",
                         [self(), CurrentTimestamp]),
    ok = file:write_file(Filename, Data).


-spec log_sink_finish_time(integer()) -> ok.
log_sink_finish_time(WaitTime) ->
    Filename =
        io_lib:format("~s/sink_stats.log",
		      [?LOG_DIR]),
    FinalTimestamp = ?GET_SYSTEM_TIME(),
    WaitingTimestamp = erlang:convert_time_unit(WaitTime, millisecond, native),
    FinalTimeWithoutWait = FinalTimestamp - WaitingTimestamp,
    Data = io_lib:format("Sink: ~p finished at time(without wait): ~p~n",
                         [self(), FinalTimeWithoutWait]),
    ok = file:write_file(Filename, Data, [append]).


%% Links all pids to the current process
-spec link_pids([pid()]) -> 'ok'.
link_pids(Pids) ->
    [true = link(Pid) || Pid <- Pids],
    ok.

%% This function accepts a merging function that takes a 
%% key and the two associated values and then merges them.
%% It merges two maps, and in case they both have a key, 
%% it merges the two values based on the merge function.
merge_with(Fun, Map1, Map2) ->
    maps:fold(
      fun(K2, V2, Map) ->
	      maps:update_with(
		K2,
		fun(V1) ->
			Fun(K2, V1, V2)
		end, V2, Map)
      end, Map1, Map2).

-spec take_at_most(integer(), [any()]) -> {[any()], [any()]}. 
take_at_most(N, List) when N >= 0 ->
    take_at_most(N, List, []).

-spec take_at_most(integer(), [any()], [any()]) -> {[any()], [any()]}.
take_at_most(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
take_at_most(_, [], Acc) ->
    {lists:reverse(Acc), []};
take_at_most(N, [Msg|Rest], Acc) ->
    take_at_most(N-1, Rest, [Msg|Acc]).

%% This function applies a focus function 
%% to every element in the list. Focus means that
%% it takes as an argument the element, and the rest
%% of the list.
%% 
%% WARNING: The function shouldn't depend on the 
%%          ordering of the rest of the elements shouldn't matter.
-spec map_focus(fun((X, [X]) -> Y), [X]) -> [Y].
map_focus(Fun, [_|_] = List) ->
    map_focus(Fun, [], List, []).

-spec map_focus(fun((X, [X]) -> Y), [X], [X], [Y]) -> [Y].
map_focus(Fun, Prev, [X], Acc) ->
    lists:reverse([Fun(X, Prev)|Acc]);
map_focus(Fun, Prev, [X|Rest], Acc) ->
    map_focus(Fun, [X|Prev], Rest, [Fun(X, Prev ++ Rest)|Acc]).

%% Unregister if registered
-spec unregister_if_registered() -> 'not_registered' | {'registered_name', atom()}.
unregister_if_registered() ->
    case erlang:process_info(self(), registered_name) of
        {registered_name, Name} ->
            true = unregister(Name),
            {registered_name, Name};
        [] ->
            'not_registered'
    end.

-spec reregister_if_registered('not_registered' | {'registered_name', atom()}) -> 'ok'.
reregister_if_registered(NameIfRegistered) ->
    case NameIfRegistered of
        {registered_name, Name} ->
            true = register(Name, self()),
            ok;
        'not_registered' ->
            ok
    end.

%% Eunit setup
nothing() -> ok.

-spec always_ok(any()) -> 'ok'.
always_ok(_) ->
    ok.

unregister_names(Names) ->
    lists:foreach(
      fun(Name) ->
	      true = unregister(Name)
      end,Names).

local_timestamp() ->
    TS = {_,_,Micro} = os:timestamp(),
    {{Year,Month,Day},{Hour,Minute,Second}} =
	calendar:now_to_universal_time(TS),
    Mstr = element(Month,{"Jan","Feb","Mar","Apr","May","Jun","Jul",
			  "Aug","Sep","Oct","Nov","Dec"}),
    io_lib:format("~2w ~s ~4w ~2w:~2..0w:~2..0w.~6..0w",
		  [Day,Mstr,Year,Hour,Minute,Second,Micro]).

-spec list_to_number(string()) -> number().
list_to_number(List) ->
  try list_to_float(List)
  catch
    _:badarg -> list_to_integer(List)
  end.

do_n_times(N, Fun) ->
    fun (X) -> 
	    do_n_times(N, X, Fun)
    end.

do_n_times(0, Init, _Fun) ->
    Init;
do_n_times(N, Init, Fun) when N > 0 ->
    do_n_times(N - 1, Fun(Init), Fun).

%%% Integer Division etc.

intfloor(X) when X < 0 ->
    (-1) * trunc((-1) * X);
intfloor(X) -> 
    trunc(X) .

intdiv(A, B) ->
    intfloor(A / B).

intmod(X,Y) when X > 0 -> X rem Y;
intmod(X,Y) when X < 0 -> Y + (X rem Y);
intmod(0,_Y) -> 0.

%%% Map util

-spec split_map(#{KeyType := ValType}, fun((KeyType) -> boolean())) -> {#{KeyType := ValType}, #{KeyType := ValType}}.
split_map(MyMap, PartitionFun) ->
	MyIter = maps:iterator(MyMap),
	split_map_rec(MyIter, maps:new(), maps:new(), PartitionFun).

-spec split_map_rec(maps:iterator(), #{KeyType := ValType}, #{KeyType := ValType}, fun((KeyType) -> boolean())) -> {#{KeyType := ValType}, #{KeyType := ValType}}.
split_map_rec(MyIter, Map1SoFar, Map2SoFar, PartitionFun) ->
	Next = maps:next(MyIter),
	case Next of
		none -> {Map1SoFar, Map2SoFar};
		{Key, Value, NextIter} ->
			case (PartitionFun(Key)) of
				true ->
					NewMap1 = maps:put(Key, Value, Map1SoFar),
					split_map_rec(NextIter, NewMap1, Map2SoFar, PartitionFun);
				false ->
					NewMap2 = maps:put(Key, Value, Map2SoFar),
					split_map_rec(NextIter, Map1SoFar, NewMap2, PartitionFun)
			end
	end.

%% Not needed -- the below is exactly what std library maps:merge() does.
% -spec merge_maps(#{KeyType := ValType}, #{KeyType := ValType}) -> #{KeyType := ValType}.
% merge_maps(Map1, Map2) ->
%     maps:from_list(maps:to_list(Map1) ++ maps:to_list(Map2)).

-spec is_subset(tuple(), tuple()) -> boolean().
is_subset(S1, S2) ->
    L1 = tuple_to_list(S1),
    L2 = tuple_to_list(S2),
    is_sublist(L1, L2).

-spec is_sublist(list(), list()) -> boolean().
is_sublist([], _L2) ->
    true;
is_sublist([_|_], []) ->
    false;
is_sublist([H1|T1], [H2|T2]) ->
    case H1 =:= H2 of
        true ->
            is_sublist(T1, T2);
        false ->
            is_sublist([H1|T1], T2)
    end.

-spec cartesian([[X]]) -> [[X]].
cartesian([H])   -> [[A] || A <- H];
cartesian([H|T]) -> [[A|B] || A <- H, B <- cartesian(T)].
