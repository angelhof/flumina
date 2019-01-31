-module(producer).

-export([main/2, loop/2]).

%% It is given a list of objects and it sends them to 
%% the given pid
main(Messages, SendTo) ->
    spawn_link(?MODULE, loop, [Messages, SendTo]).

loop([], _SendTo) ->
    ok;
loop([Msg|Rest], SendTo) ->
    SendTo ! {msg, Msg},
    loop(Rest, SendTo).

