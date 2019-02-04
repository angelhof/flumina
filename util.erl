-module(util).

-export([err/2]).

err(Format, Args) ->
    io:format(" -- ERROR: " ++ Format, Args).
