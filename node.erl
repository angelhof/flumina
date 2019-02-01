-module(node).

-export([node/5,
	 loop/4,
	 mailbox/4]).

-record(funs, {upd = undefined,
	       spl = undefined,
	       mrg = undefined}).

%% Initializes and spawns a node
node(State, Pred, Children, {UpdateFun, SplitFun, MergeFun}, Output) ->
    Funs = #funs{upd = UpdateFun, spl = SplitFun, mrg = MergeFun},
    NodePid = spawn_link(?MODULE, loop, [State, Children, Funs, Output]),
    MailboxPid = spawn_link(?MODULE, mailbox, [[], [], Pred, NodePid]),
    %% We return the mailbox pid because every message should first arrive to the mailbox
    MailboxPid.

%% This is the mailbox process that routes to 
%% their correct nodes and to make sure that
%% dependent messages arrive in order
mailbox(MessageBuffer, Dependencies, Pred, Attachee) ->
    receive
	{msg, Msg} ->
	    case Pred(Msg) of
		false ->
		    %% If this message is not for us then
		    %% route it to its correct node
		    SendTo = router:msg(router, Msg),
		    SendTo ! {msg, Msg},
		    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
		true ->
		    %% Normally here we would add the message in
		    %% the buffer (to make sure that it has arrived after
		    %% all its dependent messages). Then it would release
		    %% all the messages that we know that are ok
		    %% arrives.
		    Attachee ! {msg, Msg},
		    mailbox(MessageBuffer, Dependencies, Pred, Attachee)
	    end;
	{merge, State} ->
	    Attachee ! {merge, State},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
	{state, State} ->
	    Attachee ! {state, State},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee)
    end.

%% This is the main loop that each node executes.
loop(State, Children, Funs = #funs{upd=UFun, spl=SFun, mrg=MFun}, Output) ->
    receive
	{msg, Msg} ->
	    %% The mailbox has cleared this message so we don't need to check for pred
	    case Children of
		[] ->
		    NewState = UFun(Msg, State, Output),
		    loop(NewState, Children, Funs, Output);
		_ ->
		    %% TODO: There are things missing
		    [State1, State2] = [sync_merge(C) || C <- Children],
		    MergedState = MFun(State1, State2),
		    NewState = UFun(Msg, MergedState, Output),
		    {NewState1, NewState2} = SFun(NewState),
		    [C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
		    loop(NewState, Children, Funs, Output)
	    end;
	{merge, Father} ->
	    Father ! {state, State},
	    receive
		{state, NewState} ->
		    loop(NewState, Children, Funs, Output)
	    end		    
    end.

sync_merge(C) ->
    C ! {merge, self()},
    receive 
	{state, State} -> 
	    State
    end.
