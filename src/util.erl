-module(util).

-export([err/2,
	 crash/2,
	 exec/1,
	 sink/0,
	 sink/1,
	 sink_no_log/1,
         sink/2,
	 merge_with/3,
	 take_at_most/2,
	 map_focus/2,
	 nothing/0,
	 always_ok/1,
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

sink() ->
    sink(fun log_mod:no_message_logger/0).

sink(MsgLoggerInitFun) ->
    sink(MsgLoggerInitFun, 15000).

sink_no_log(WaitTime) ->
    sink(fun log_mod:no_message_logger/0, WaitTime).

sink(MsgLoggerInitFun, WaitTime) ->
    LoggerFun = MsgLoggerInitFun(),
    receive
	finished ->
	    io:format("Configuration done~n", []),
	    sink_loop(LoggerFun, WaitTime)
    end.

sink_loop(LoggerFun, WaitTime) ->
    receive
	Msg ->
	    LoggerFun({Msg, fake_node, 0}),
	    io:format("~p~n", [Msg]),
	    sink_loop(LoggerFun, WaitTime)
    after
        WaitTime ->
            io:format("Didn't receive anything for ~p seconds~n", [WaitTime]),
	    ok
    end.

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
