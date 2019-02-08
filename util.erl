-module(util).

-export([err/2]).

-include("type_definitions.hrl").

err(Format, Args) ->
    io:format(" -- ERROR: " ++ Format, Args).
