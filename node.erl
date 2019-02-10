-module(node).

-export([node/5,
	 init_mailbox/4,
	 mailbox/5,
	 init_node/3,
	 loop/4]).

-include("type_definitions.hrl").

-record(funs, {upd = undefined :: update_fun(),
	       spl = undefined :: split_fun(),
	       mrg = undefined :: merge_fun()}).

%% Initializes and spawns a node and its mailbox
-spec node(State::any(), message_predicate(), spec_functions(), dependencies(), pid()) -> {pid(), pid()}.
node(State, Pred, {UpdateFun, SplitFun, MergeFun}, Dependencies, Output) ->
    Funs = #funs{upd = UpdateFun, spl = SplitFun, mrg = MergeFun},
    NodePid = spawn_link(?MODULE, init_node, [State, Funs, Output]),
    Timers = maps:map(fun(_,_) -> 0 end, Dependencies),
    MailboxPid = spawn_link(?MODULE, init_mailbox, [{[], Timers}, Dependencies, Pred, NodePid]),
    %% We return the mailbox pid because every message should first arrive to the mailbox
    {NodePid, MailboxPid}.



%%
%% Mailbox
%%

-spec init_mailbox(message_buffer(), dependencies(), message_predicate(), pid()) -> no_return().
init_mailbox(MessageBuffer, Dependencies, Pred, Attachee) ->
    %% Before executing the main loop receive the
    %% Configuration tree, which can only be received
    %% after all the nodes have already been spawned
    receive
	{configuration, ConfTree} ->
	    Attachee ! {configuration, ConfTree},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee, ConfTree)
    end.


%% This is the mailbox process that routes to 
%% their correct nodes and makes sure that
%% dependent messages arrive in order
-spec mailbox(message_buffer(), dependencies(), message_predicate(), pid(), configuration()) -> no_return().
mailbox(MessageBuffer, Dependencies, Pred, Attachee, ConfTree) ->
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
	    SendTo = router:or_route(Msg, ConfTree),
	    SendTo ! {msg, Msg},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee, ConfTree);
	{msg, Msg} ->
	    case Pred(Msg) of
		false ->
		    %% This should be unreachable because all the messages 
		    %% are routed to a node that can indeed handle them
		    util:err("The message: ~p doesn't satisfy ~p's predicate~n", [Msg, Attachee]),
		    erlang:halt(1);
		true ->
		    %% Whenever a new message arrives, we add it to the buffer
		    NewMessageBuffer = add_to_buffer(Msg, MessageBuffer, Dependencies),
		    %% NewMessageBuffer = add_to_buffer_or_send(Msg, MessageBuffer, Dependencies, Attachee),
		    %% io:format("Message: ~p -- NewMessagebuffer: ~p~n", [Msg, NewMessageBuffer]), 
		    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee, ConfTree)
	    end;
	{merge, {Tag, Ts, Father}} ->
	    %% Whenever a merge request arrives, we first clear the message buffer
	    %% so that messages before the merge request are processed
	    NewMessageBuffer = clear_buffer({Tag, Ts}, MessageBuffer, Dependencies, Attachee),
	    %% Then we forward the merge to the node
	    Attachee ! {merge, {Tag, Ts, Father}},
	    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee, ConfTree);
	{state, State} ->
	    %% This is the reply of a child node with its state 
	    Attachee ! {state, State},
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee, ConfTree);
	{iheartbeat, TagTs} ->
	    %% WARNING: I am not sure about that
	    %% Whenever a heartbeat first arrives into the system we have to send it to all nodes
	    %% that this heartbeat satisfies their predicate. Is this correct? Or should we just 
	    %% send it to all the lowest nodes that handle it? In this case how do parent nodes
	    %% in the tree learn about this heartbeat? On the other hand is it bad if they learn
	    %% about a heartbeat before the messages of that type are really processed by their
	    %% children nodes?
	    broadcast_heartbeat(TagTs, ConfTree),
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee, ConfTree);
	{heartbeat, TagTs} ->
	    %% A heartbeat clears the buffer and updates the timers
	    NewMessageBuffer = clear_buffer(TagTs, MessageBuffer, Dependencies, Attachee),
	    %% io:format("Hearbeat: ~p -- NewMessagebuffer: ~p~n", [TagTs, NewMessageBuffer]),
	    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee, ConfTree)
    end.

%% WARNING: Even if all the dependent timers of a message m1 are higher than it
%%          this doesn't mean that the message m1 should be released, because 
%%          it might be the case that some other messages m2 that depend to
%%          it (and are to be sent before it) are still in the buffer waiting 
%%          for a heartbeat m1 to be cleared. The easiest way to deal with this
%%          is to just add all messages to the buffer and just let heartbeats clear
%%          messages. NOTE however that this implementation decision means that
%%          messages might stay for longer than they really needed in the buffer.
%%          To make sure that this works correctly we must make sure that there
%%          are "infinitely" many heartbeats sent so that everything is eventually
%%          cleared from the mailboxes.
%% 
%% TODO:    Optimize the above procedure, to not let messages wait unnecessarily
%%          in the buffer
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
%% It seems that the only way for the buffer to clear messages is
%% after getting a heartbeat/mark, that indicates that all messages
%% of some tag up to that point have been received.
%% Because of that, new messages are just added to the Buffer
-spec add_to_buffer(message(), message_buffer(), dependencies()) -> message_buffer().
add_to_buffer(Msg, BufferTimers, Dependencies) ->
    add_to_buffer(Msg, BufferTimers, Dependencies, []).

-spec add_to_buffer(message(), message_buffer(), dependencies(), [message()]) -> message_buffer().
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
-spec clear_buffer({tag(), integer()}, message_buffer(), dependencies(), pid()) -> message_buffer().
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
-spec broadcast_heartbeat({tag(), integer()}, configuration()) -> [heartbeat()].
broadcast_heartbeat({Tag, Ts}, ConfTree) ->
    AllPids = router:heartbeat_route({Tag, Ts, heartbeat}, ConfTree),
    [P ! {heartbeat, {Tag, Ts}} || P <- AllPids].

%% =================================================================== %%

%%
%% Main Processing Node
%%
-spec init_node(State::any(), #funs{}, pid()) -> no_return().
init_node(State, Funs, Output) ->
    %% Before executing the main loop receive the
    %% Configuration tree, which can only be received
    %% after all the nodes have already been spawned
    receive
	{configuration, ConfTree} ->
	    loop(State, Funs, Output, ConfTree)
    end.
	

%% This is the main loop that each node executes.
-spec loop(State::any(), #funs{}, pid(), configuration()) -> no_return().
loop(State, Funs = #funs{upd=UFun, spl=SFun, mrg=MFun}, Output, ConfTree) ->
    receive
        MessageMerge ->
	    %% The mailbox has cleared this message so we don't need to check for pred
	    case configuration:find_children_mbox_pids(self(), ConfTree) of
		[] ->
		    NewState = handle_message(MessageMerge, State, Output, UFun),
		    loop(NewState, Funs, Output, ConfTree);
		Children ->
		    %% TODO: There are things missing
		    {_IsMsgMerge, {Tag, Ts, _Payload}} = MessageMerge,
		    [State1, State2] = [sync_merge(C, {Tag, Ts}) || C <- Children],
		    MergedState = MFun(State1, State2),
		    NewState = handle_message(MessageMerge, MergedState, Output, UFun),
		    [Pred1, Pred2] = configuration:find_children_preds(self(), ConfTree),
		    {NewState1, NewState2} = SFun({Pred1, Pred2}, NewState),
		    [C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
		    loop(NewState, Funs, Output, ConfTree)
	    end
    end.

-spec handle_message(message() | merge_request(), State::any(), pid(), update_fun()) -> State::any().
handle_message({msg, Msg}, State, Output, UFun) ->
    update_on_msg(Msg, State, Output, UFun);
handle_message({merge, {_Tag, _Ts, Father}}, State, _Output, _UFun) ->
    respond_to_merge(Father, State).

-spec update_on_msg(message(), State::any(), pid(), update_fun()) -> State::any().
update_on_msg(Msg, State, Output, UFun) ->
    NewState = UFun(Msg, State, Output),    
    NewState.

-spec respond_to_merge(pid(), State::any()) -> State::any().
respond_to_merge(Father, State) ->
    Father ! {state, State},
    receive
	{state, NewState} ->
	    NewState
    end.

-spec sync_merge(pid(), {tag(), integer()}) -> State::any().
sync_merge(C, {Tag, Ts}) ->
    C ! {merge, {Tag, Ts, self()}},
    receive 
	{state, State} ->
	    State
    end.
