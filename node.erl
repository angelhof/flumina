-module(node).

-export([node/6,
	 loop/4,
	 mailbox/4]).

-record(funs, {upd = undefined,
	       spl = undefined,
	       mrg = undefined}).

%% Initializes and spawns a node
node(State, Pred, Children, {UpdateFun, SplitFun, MergeFun}, Dependencies, Output) ->
    Funs = #funs{upd = UpdateFun, spl = SplitFun, mrg = MergeFun},
    NodePid = spawn_link(?MODULE, loop, [State, Children, Funs, Output]),
    Timers = #{a => 0, b => 0},
    MailboxPid = spawn_link(?MODULE, mailbox, [{[], Timers}, Dependencies, Pred, NodePid]),
    %% We return the mailbox pid because every message should first arrive to the mailbox
    MailboxPid.

%%
%% Mailbox
%%


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
		    SendTo = router:or_route(router, Msg),
		    SendTo ! {msg, Msg},
		    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
		true ->
		    %% TODO: Buffer with dependencies
		    %% Normally here we would add the message in
		    %% the buffer (to make sure that it has arrived after
		    %% all its dependent messages). Then it would release
		    %% all the messages that we know that are ok
		    %% arrives.
		    NewMessageBuffer = add_to_buffer_or_send(Msg, MessageBuffer, Dependencies, Attachee), 
		    %% Attachee ! {msg, Msg},
		    io:format("~p -- NewMessagebuffer: ~p~n", [self(), NewMessageBuffer]),
		    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee)
	    end;
	{merge, Father, TagTs} ->
	    %% First clear the message buffer
	    NewMessageBuffer = clear_buffer(TagTs, MessageBuffer, Dependencies, Attachee),
	    %% Then forward the merge
	    Attachee ! {merge, Father, TagTs},
	    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee);
	{state, State} ->
	    Attachee ! {state, State},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
	%% WARNING: There is a problem with the current heartbeat implementation
	%% All messages are released at the same time when a heartbeat arrives
	%% and so for example when {heartbeat, {b, 7}} arrives, all a's
	%% are processed despite not having still processed {b,2}
	%% TODO: Somehow fix this problem
	%% WARNING: A heartbeat here should only be handled by a node that owns it.
	%%          Not exactly though. The a heartbeats should be handled by both 
	%%          the b and the a node because if only the a node gets it, then 
        %%          the b node will never release. So in essence, the parent nodes 
        %%          in the trees need to learn about their childrens' heartbeats
	%%          so that they can ask them, whereas the children (which compute)
	%%          shouldn't learn about the heartbeats of their parents
	{heartbeat, TagTs} ->
	    broadcast_heartbeat(TagTs),
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
	{bheartbeat, TagTs} ->
	    io:format("~p -- TagTs: ~p~n", [self(), TagTs]),
	    NewMessageBuffer = clear_buffer(TagTs, MessageBuffer, Dependencies, Attachee),
	    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee)
    end.

%% It seems that the only way for the buffer to clear messages is
%% after getting a heartbeat/mark, that indicates that all messages
%% of some tag up to that point have been received.
%% Because of that, new messages are just added to the Buffer
add_to_buffer_or_send(Msg, {MsgBuffer, Timers}, Dependencies, Attachee) ->
    {Tag, Ts, _} = Msg,
    TagDeps = maps:get(Tag, Dependencies),
    case lists:all(fun(TD) -> Ts =< maps:get(TD, Timers) end, TagDeps) of
	true ->
	    Attachee ! {msg, Msg},
	    {MsgBuffer, Timers};
	false ->
	    add_to_buffer(Msg, {MsgBuffer, Timers}, Dependencies, [])
    end.
add_to_buffer(Msg, {[], Timers}, Dependencies, NewBuffer) ->
    {lists:reverse([Msg|NewBuffer]), Timers};
add_to_buffer(Msg, {[BMsg|Buf], Timers}, Dependencies, NewBuf) ->
    {Tag, Ts, Payload} = Msg,
    {_, BTs, _} = BMsg,
    case Ts < BTs of
	true ->
	    {lists:reverse([Msg|NewBuf]) ++ [BMsg|Buf], Timers};
	false ->
	    add_to_buffer({Tag, Ts, Payload}, {Buf, Timers}, Dependencies, [BMsg|NewBuf])
    end.

%% This releases all the messages in the buffer that
%% where dependent on this tag. 
%% WARNING: At the moment the implementation is very naive
clear_buffer({HTag, HTs}, {Buffer, Timers}, Dependencies, Attachee) ->
    %% We assume that heartbeats arrive in the correct order
    %% TODO: Assing the maximum of the heartbeats
    NewTimers = maps:put(HTag, HTs, Timers),
    {ToRelease, NewBuffer} = 
	lists:partition(
	  fun({Tag, Ts, _}) ->
		  TagDeps = maps:get(Tag, Dependencies),
		  lists:all(fun(TD) -> Ts =< maps:get(TD, NewTimers) end, TagDeps)
	  end, Buffer),
    io:format("~p -- Timers: ~p~n", [self(), NewTimers]),
    io:format("~p -- Hearbeat: ~p -- Partition: ~p~n", [self(), {HTag, HTs}, {ToRelease, NewBuffer}]),
    [Attachee ! {msg, Msg} || Msg <- ToRelease],
    {NewBuffer, NewTimers}.
    
%% Broadcasts the heartbeat to those who are responsible for it
%% Responsible is the beta-mapping or the predicate (?) are those the same?
broadcast_heartbeat({Tag, Ts}) ->
    AllPids = router:and_route(router, {Tag, Ts, heartbeat}),
    [P ! {bheartbeat, {Tag, Ts}} || P <- AllPids].

%%
%% Main Processing Node
%%


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
		    {Tag, Ts, Payload} = Msg,
		    [State1, State2] = [sync_merge(C, {Tag, Ts}) || C <- Children],
		    MergedState = MFun(State1, State2),
		    NewState = UFun(Msg, MergedState, Output),
		    {NewState1, NewState2} = SFun(NewState),
		    [C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
		    loop(NewState, Children, Funs, Output)
	    end;
	{merge, Father, {Tag, Ts}} ->
	    Father ! {state, State},
	    receive
		{state, NewState} ->
		    loop(NewState, Children, Funs, Output)
	    end		    
    end.

sync_merge(C, TagTs) ->
    C ! {merge, self(), TagTs},
    receive 
	{state, State} -> 
	    State
    end.
