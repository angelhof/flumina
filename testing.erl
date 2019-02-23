-module(testing).

-export([test_mfa/2,
	 test_sink/2]).

-include_lib("eunit/include/eunit.hrl").

%% This function executes a function
%% that takes as a single argument the pid
%% of a sink
%% WARNING: The way testing is implemented now,
%%          it suceeds if we get at least as many messages as
%%          the expected output, if we get more it doesn't fail.
-spec test_mfa({atom(), atom()}, Messages::[any()]) -> 'ok'.
test_mfa({M, F}, ExpOutput) ->
    ExecPid = spawn_link(M, F, [self()]),
    test_sink(ExpOutput, testing).

%% This is just a sink function that compares whatever it gets 
%% with the expected correct output
-spec test_sink(Messages::[any()], 'testing' | 'done') -> 'ok'.
test_sink([], done) ->
    %% At this point our mailbox should be
    %% empty, as the process is done, if it isn't
    %% then the test should fail
    receive
	What ->
	    %% util:err("Test sink: ~p~n", [What]),
	    {error, What}
    after 
	0 -> ok
    end;	
test_sink(ExpOutput, Mode) ->
    receive
	finished when Mode =:= testing->
	    test_sink(ExpOutput, done);
        {'EXIT', _Pid, normal} ->
	    test_sink(ExpOutput, Mode);
	Msg ->
	    case ExpOutput of
		[ExpMsg|ExpRest] when Msg =:= ExpMsg ->
		    test_sink(ExpRest, Mode);
		_ ->
		    %% util:err("Test sink: ~p~n", [Msg]),
		    {error, Msg}
	    end
    after
    	3000 ->
    	    %% If we don't receive any message after 2 seconds,
    	    %% it means that something is stuck
	    %% util:err("Test sink timeout !!!~n", []),
    	    timeout
    end.
    
