-module(node).

-export([node/5,
	 loop/5]).

-record(funs, {upd = undefined,
	       spl = undefined,
	       mrg = undefined}).

%% Initializes and spawns a node
node(State, Pred, Children, {UpdateFun, SplitFun, MergeFun}, Output) ->
    Funs = #funs{upd = UpdateFun, spl = SplitFun, mrg = MergeFun},
    Pid = spawn_link(?MODULE, loop, [State, Pred, Children, Funs, Output]),
    Pid.

%% This is the main loop that each node executes.
loop(State, Pred, Children, Funs = #funs{upd=UFun, spl=SFun, mrg=MFun}, Output) ->
    receive
	{msg, Msg} ->
	    case Pred(Msg) of
		false ->
		    SendTo = router:msg(router, Msg),
		    SendTo ! {msg, Msg},
		    loop(State, Pred, Children, Funs, Output);
		true ->
		    case Children of
			[] ->
			    NewState = UFun(Msg, State, Output),
			    loop(NewState, Pred, Children, Funs, Output);
			_ ->
			    %% TODO: There are things missing
			    [State1, State2] = [sync_merge(C) || C <- Children],
			    MergedState = MFun(State1, State2),
			    NewState = UFun(Msg, MergedState, Output),
			    {NewState1, NewState2} = SFun(NewState),
			    [C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
			    loop(NewState, Pred, Children, Funs, Output)
		    end		    
			
	    end;
	{merge, Father} ->
	    Father ! {state, State},
	    receive
		{state, NewState} ->
		    loop(NewState, Pred, Children, Funs, Output)
	    end		    
    end.

sync_merge(C) ->
    C ! {merge, self()},
    receive 
	{state, State} -> 
	    State
    end.
