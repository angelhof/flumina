-module(util).

-export([err/2,
	 crash/2,
	 exec/1,
	 sink/0,
	 sink/1,
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
	 split_map/2]).

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
    LoggerFun = MsgLoggerInitFun(),
    receive
	finished ->
	    io:format("Configuration done~n", []),
	    sink_loop(LoggerFun)
    end.

sink_loop(LoggerFun) ->
    receive
	Msg ->
	    LoggerFun({Msg, fake_node, 0}),
	    io:format("~p~n", [Msg]),
	    sink_loop(LoggerFun)
    after 
	15000 ->
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

do_n_times(0, Init, Fun) ->
    Init;
do_n_times(N, Init, Fun) when N > 0 ->
    do_n_times(N - 1, Fun(Init), Fun).

%%% Integer Division etc.

intfloor(X) when X < 0 ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T - 1
    end;
intfloor(X) -> 
    trunc(X) .

intdiv(A, B) ->
    intfloor(A / B).

intmod(X,Y) when X > 0 -> X rem Y;
intmod(X,Y) when X < 0 -> Y + X rem Y;
intmod(0,Y) -> 0.

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
					NewMap1 = maps:update(Key, Value, Map1SoFar),
					split_map_rec(NextIter, NewMap1, Map2SoFar, PartitionFun);
				false ->
					NewMap2 = maps:update(Key, Value, Map2SoFar),
					split_map_rec(NextIter, Map1SoFar, NewMap2, PartitionFun)
			end
	end.
