-module(testing).

-export([test_mf/2,
         test_mfa/3,
	 test_sink/2,
	 unregister_names/0]).

-include_lib("eunit/include/eunit.hrl").

%% This function executes a function
%% that takes as a single argument the pid
%% of a sink
%% WARNING: The way testing is implemented now,
%%          it succeeds if we get at least as many messages as
%%          the expected output, if we get more it doesn't fail.
-spec test_mf({atom(), atom()}, Messages::[any()]) -> 'ok'.
test_mf({M, F}, ExpOutput) ->
    test_mfa({M, F}, [], ExpOutput).

-spec test_mfa({atom(), atom()}, Args::[any()], Messages::[any()]) -> 'ok'.
test_mfa({M, F}, Args, ExpOutput) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    ExecPid = spawn_link(M, F, [SinkName] ++ Args),
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
	    {error, didnt_expect, What}
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
		[ExpMsg|ExpRest] ->
		    %% util:err("Test sink: ~p~n", [Msg]),
		    {error, expected, ExpMsg, got, Msg};
		[] ->
		    {error, didnt_expect, Msg}
	    end
    after
    	3000 ->
    	    %% If we don't receive any message after 2 seconds,
    	    %% it means that something is stuck
	    %% util:err("Test sink timeout !!!~n", []),
    	    timeout
    end.
    
%% This used to call the unregister names with the addition of sink
%% which is the testing sink name. However, sink dies either way 
%% so it is unregistered automatically.
%%
%% WARNING: We assume that only our processes have proc_Num names
unregister_names() ->
    %% In order to find all the names to unregister,
    %% we look at all the registered names, and unregister
    %% all those that are 'proc_' ++ Number
    AllNames = [atom_to_list(Name) || Name <- registered()],
    Pattern = "proc_[0-9]+",
    FilteredNames =
	lists:filter(
	 fun(Name) ->
		 case re:run(Name, Pattern) of
		     {match, _} ->
			 true;
		     nomatch ->
			 false
		 end
	 end, AllNames),
    AtomFilteredNames = [list_to_atom(Name) || Name <- FilteredNames],
    util:unregister_names(AtomFilteredNames).

