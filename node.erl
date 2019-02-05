-module(node).

-export([node/6,
	 loop/4,
	 mailbox/4]).

-record(funs, {upd = undefined,
	       spl = undefined,
	       mrg = undefined}).

%% Initializes and spawns a node and its mailbox
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
%% their correct nodes and makes sure that
%% dependent messages arrive in order
mailbox(MessageBuffer, Dependencies, Pred, Attachee) ->
    receive
	%% Explanation:
	%% The messages that first enter the system contain an 
	%% imsg tag. Then they are sent to a node that can 
	%% handle them, and they get a msg tag.
	{imsg, Msg} ->
	    %% Explanation:
	    %% Whenever a message arrives to the mailbox of a process
	    %% this process has to decide whether it will process it or
	    %% not. This depends on:
	    %% - If the process can process it. If it doesn't satisfy its
	    %%   predicate then, it cannot really process it.
	    %% - If it has children processes in the tree, it should route
	    %%   the message to a lower node, as only leaf processes process
	    %%   and a message must be handled by (one of) the lowest process 
	    %%   in the tree that can handle it.
	    SendTo = router:or_route(router, Msg),
	    SendTo ! {msg, Msg},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
	{msg, Msg} ->
	    case Pred(Msg) of
		false ->
		    %% This should be unreachable because all the messages 
		    %% are routed to a node that can indeed handle them
		    util:err("The message: ~p doesn't satisfy ~p's predicate~n", [Msg, Attachee]),
		    erlang:halt(1);
		true ->
		    %% Whenever a new message arrives, we add it to the buffer
		    NewMessageBuffer = add_to_buffer_or_send(Msg, MessageBuffer, Dependencies, Attachee), 
		    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee)
	    end;
	{merge, Father, TagTs} ->
	    %% Whenever a merge request arrives, we first clear the message buffer
	    %% so that messages before the merge request are processed
	    NewMessageBuffer = clear_buffer(TagTs, MessageBuffer, Dependencies, Attachee),
	    %% Then we forward the merge to the node
	    Attachee ! {merge, Father, TagTs},
	    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee);
	{state, State} ->
	    %% This is the reply of a child node with its state 
	    Attachee ! {state, State},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
	{iheartbeat, TagTs} ->
	    %% WARNING: I am not sure about that
	    %% Whenever a heartbeat first arrives into the system we have to send it to all nodes
	    %% that this heartbeat satisfies their predicate. Is this correct? Or should we just 
	    %% send it to all the lowest nodes that handle it? In this case how do parent nodes
	    %% in the tree learn about this heartbeat? On the other hand is it bad if they learn
	    %% about a heartbeat before the messages of that type are really processed by their
	    %% children nodes?
	    broadcast_heartbeat(TagTs),
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee);
	{heartbeat, TagTs} ->
	    %% A heartbeat clears the buffer and updates the timers
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
    %% TODO: The new timer should be the maximum of the current timer and the heartbeat
    NewTimers = maps:put(HTag, HTs, Timers),
    {ToRelease, NewBuffer} = 
	lists:partition(
	  fun({Tag, Ts, _}) ->
		  TagDeps = maps:get(Tag, Dependencies),
		  lists:all(fun(TD) -> Ts =< maps:get(TD, NewTimers) end, TagDeps)
	  end, Buffer),
    %% io:format("~p -- Timers: ~p~n", [self(), NewTimers]),
    %% io:format("~p -- Hearbeat: ~p -- Partition: ~p~n", [self(), {HTag, HTs}, {ToRelease, NewBuffer}]),
    [Attachee ! {msg, Msg} || Msg <- ToRelease],
    {NewBuffer, NewTimers}.
    
%% Broadcasts the heartbeat to those who are responsible for it
%% Responsible is the beta-mapping or the predicate (?) are those the same?
broadcast_heartbeat({Tag, Ts}) ->
    AllPids = router:heartbeat_route(router, {Tag, Ts, heartbeat}),
    [P ! {heartbeat, {Tag, Ts}} || P <- AllPids].

%% =================================================================== %%

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
