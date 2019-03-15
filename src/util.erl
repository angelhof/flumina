-module(util).

-export([err/2,
	 crash/2,
	 exec/1,
	 sink/0,
	 merge_with/3,
	 take_at_most/2,
	 nothing/0,
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
    receive
	Msg ->
	    io:format("~p~n", [Msg]),
	    sink()
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

%% Eunit setup
nothing() -> ok.

unregister_names(Names) ->
    lists:foreach(
      fun(Name) ->
	      true = unregister(Name)
      end,Names).
