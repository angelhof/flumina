-module(util).

-export([err/2,
	 crash/2,
	 exec/1,
	 sink/0]).

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
    end.
