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
	 unregister_names/1]).

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
    sink_loop(LoggerFun).

sink_loop(LoggerFun) ->
    receive
	Msg ->
	    LoggerFun(Msg),
	    io:format("~p~n", [Msg]),
	    sink_loop(LoggerFun)
    after 
	5000 ->
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
