-module(node).

-export([node/5,
	 init_mailbox/3,
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
    MailboxPid = spawn_link(?MODULE, init_mailbox, [Dependencies, Pred, NodePid]),
    %% We return the mailbox pid because every message should first arrive to the mailbox
    {NodePid, MailboxPid}.



%%
%% Mailbox
%%

-spec init_mailbox(dependencies(), message_predicate(), pid()) -> no_return().
init_mailbox(Dependencies, Pred, Attachee) ->
    %% Before executing the main loop receive the
    %% Configuration tree, which can only be received
    %% after all the nodes have already been spawned
    receive
	{configuration, ConfTree} ->
	    Attachee ! {configuration, ConfTree},
	    

	    %% The dependencies are used to clear messages from the buffer,
	    %% When we know that we have received all dependent messages to
	    %% after a time t, then we can release all messages {m, t', v}
	    %% where t' \leq t.
	    %% 
	    %% Assumption:
	    %% The parent nodes don't need to get heartbeats from the
	    %% messages that are processed by their children nodes, because
	    %% they will learn about them either way when they ask for a merge.
	    %% Because of that, it is safe to remove the dependencies that have to
	    %% do with messages that our children handle.
	    %%
	    %% Is the above assumption correct?
	    %%	    
	    %% WARNING: Each node's predicate shows which messages this specific
	    %%          node handles, so in we have to remove from the dependencies
	    %%          all the messages that our descendants handle, so the union
	    %%          of all our descendants predicates. 
	    %%
	    %% WARNING: At the moment dependencies are represented with tags, 
	    %%          but we also have predicates. We need to decide and 
	    %%          use one or the other.
	    
	    %% ======== THIS HAS TO BE REPLACED WITH FILTER RELEVANT DEPENDENCIES ======== %%
	    %% DescendantPreds = configuration:find_descendant_preds(Attachee, ConfTree),
	    %% DescendantPred = 
	    %% 	fun(Msg) ->
	    %% 		%% The union of all the descendant predicates
	    %% 		lists:any(fun(Pr) -> Pr(Msg) end, DescendantPreds) 
	    %% 	end,
	    %% OptimizedDependencies = 
	    %% 	remove_unnecassary_dependencies(Pred, DescendantPred, Dependencies),
	    %% =========================================================================== %%
	    RelevantDependencies =
		filter_relevant_dependencies(Dependencies, Attachee, ConfTree),

	    %% All the tags that we depend on
	    AllDependingTags = lists:flatten(maps:values(RelevantDependencies)),
	    Timers = maps:from_list([{T, 0} || T <-  AllDependingTags]),
	    mailbox({[], Timers}, RelevantDependencies, Pred, Attachee, ConfTree)
    end.

%%
%% The mailbox works by releasing messages (and merge requests) when all of their previously
%% received dependencies have been released. However a node never receives heartbeats from
%% messages that their siblings or uncle nodes handle, so they have to disregard those dependencies
%% as they cannot be handled by them. 
%% Because of that, we have to remove dependencies that a node can not handle (because it 
%% doesn't receive those messages and heartbeats), so that progress is ensured.
%%
%% The way we do it, is by only keeping the dependencies that belong to the union 
%% (MyPred - ChildrenPreds), (ParentPred - SiblingPred), (GrandParentPred - UnclePred)
-spec filter_relevant_dependencies(dependencies(), pid(), configuration()) -> dependencies().
filter_relevant_dependencies(Dependencies0, Attachee, ConfTree) ->
    {found, Predicate} = configuration:get_relevant_predicates(Attachee, ConfTree),
    Dependencies1 = 
	maps:map(
	  fun(Tag, DTags) ->
		  %% [io:format("~p -> ~p || ~p : ~p~n", 
		  %% 	    [Tag, DT, self(), not DescendantPred({DT, undef, undef})])
		  %% 	    || DT <- DTags],
		  [DT || DT <- DTags, Predicate({DT, 0, 0})]
	  end, Dependencies0),
    io:format("Clean Deps:~p~n~p~n", [self(), Dependencies1]),
    Dependencies1.
    


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
		    NewMessageBuffer = add_to_buffer({msg, Msg}, MessageBuffer),
		    %% NewMessageBuffer = add_to_buffer_or_send(Msg, MessageBuffer, Dependencies, Attachee),
		    %% io:format("Message: ~p -- NewMessagebuffer: ~p~n", [Msg, NewMessageBuffer]),
		    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee, ConfTree)
	    end;
	{merge, {Tag, Ts, Father}} ->
	    %% A merge requests acts as two different messages in our model.
	    %% - A heartbeat message, because it shows that some ancestor has
	    %%   received all messages with Tag until Ts. Because of that we 
	    %%   need to clear the buffer with it as if it was a heartbeat.
	    %% - A message that will be processed like every other message (after
	    %%   its dependencies are dealt with), so we have to add it to the buffer
	    %%   like we do with every other message
	    NewMessageBuffer = add_to_buffer({merge, {Tag, Ts, Father}}, MessageBuffer),
	    ClearedMessageBuffer = clear_buffer({Tag, Ts}, NewMessageBuffer, Dependencies, Attachee),
	    %% io:format("~p -- After Merge: ~p~n", [self(), NewMessageBuffer]),
	    mailbox(ClearedMessageBuffer, Dependencies, Pred, Attachee, ConfTree);
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
	    %%
	    %% NOTE (Current Implementation):
	    %% Broadcast the heartbeat to all nodes who process tags related to this 
	    %% heartbeat (so if it satisfies their predicates). In order for this to be
	    %% efficient, it assumes that predicates are not too broad in the sense that
	    %% a node processes a message x iff pred(x) = true. 
	    broadcast_heartbeat(TagTs, ConfTree),
	    mailbox(MessageBuffer, Dependencies, Pred, Attachee, ConfTree);
	{heartbeat, TagTs} ->
	    %% A heartbeat clears the buffer and updates the timers
	    NewMessageBuffer = clear_buffer(TagTs, MessageBuffer, Dependencies, Attachee),
	    %% io:format("Hearbeat: ~p -- NewMessagebuffer: ~p~n", [TagTs, NewMessageBuffer]),
	    mailbox(NewMessageBuffer, Dependencies, Pred, Attachee, ConfTree)
    end.

%% It seems that the only way for the buffer to clear messages is
%% after getting a heartbeat/mark, that indicates that all messages
%% of some tag up to that point have been received.
%% Because of that, new messages are just added to the Buffer
-spec add_to_buffer(message_or_merge(), message_buffer()) -> message_buffer().
add_to_buffer(Msg, BufferTimers) ->
    add_to_buffer(Msg, BufferTimers, []).

-spec add_to_buffer(message_or_merge(), message_buffer(), [message_or_merge()]) -> message_buffer().
add_to_buffer(Msg, {[], Timers}, NewBuffer) ->
    {lists:reverse([Msg|NewBuffer]), Timers};
add_to_buffer(Msg, {[BMsg|Buf], Timers}, NewBuf) ->
    {_MsgOrMerge, {Tag, Ts, Payload}} = Msg,
    {_BMsgOrMerge, {BTag, BTs, _}} = BMsg,
    %% Note: I am comparing the tuple {Ts, Tag} to have a total ordering 
    %% between messages with different tags but the same timestamp. 
    case {Ts, Tag} < {BTs, BTag} of
	true ->
	    {lists:reverse([Msg|NewBuf]) ++ [BMsg|Buf], Timers};
	false ->
	    add_to_buffer(Msg, {Buf, Timers}, [BMsg|NewBuf])
    end.

%% This releases all the messages in the buffer that
%% where dependent on this tag. 
%% WARNING: At the moment the implementation is very naive
-spec clear_buffer({tag(), integer()}, message_buffer(), dependencies(), pid()) -> message_buffer().
clear_buffer({HTag, HTs}, {Buffer, Timers}, Dependencies, Attachee) ->
    %% We assume that heartbeats arrive in the correct order
    %% TODO: The new timer should be the maximum of the current timer and the heartbeat
    NewTimers = maps:put(HTag, HTs, Timers),
    {ToRelease, NewBuffer} = release_messages(Buffer, NewTimers, Dependencies, []),
    %% io:format("~p -- Timers: ~p~n", [self(), NewTimers]),
    %% io:format("~p -- Hearbeat: ~p -- Partition: ~p~n", [self(), {HTag, HTs}, {ToRelease, NewBuffer}]),
    [Attachee ! Msg || Msg <- ToRelease],
    {NewBuffer, NewTimers}.

%% This function releases messages in a naive way. It stops releasing on the first message that 
%% it finds that doesn't have its dependencies heartbeat timers higher than itself.
%% TODO: Improve this function to release all the messages which don't have any of
%%       their dependent messages before them in the buffer (Details in notes.org)
-spec release_messages([message_or_merge()], timers(), dependencies(), [message_or_merge()]) 
		      -> {[message_or_merge()], [message_or_merge()]}.
release_messages([], Timers, Dependencies, ToReleaseRev) ->
    {lists:reverse(ToReleaseRev), []};
release_messages([{MsgOrMerge, {Tag, Ts, _}} = Msg|Buffer], Timers, Dependencies, ToReleaseRev) ->
    TagDeps = maps:get(Tag, Dependencies),
    case lists:all(fun(TD) -> Ts =< maps:get(TD, Timers) end, TagDeps) of
	true ->
	    %% If the current messages has a timestamp that is smaller
	    %% than all its dependency heartbeats, then we can release it
	    release_messages(Buffer, Timers, Dependencies, [Msg|ToReleaseRev]);
	false ->
	    %% If not, we stop releasing messages
	    {lists:reverse(ToReleaseRev), [Msg|Buffer]}
    end.

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
		    NewState = handle_message(MessageMerge, State, Output, UFun, ConfTree),
		    loop(NewState, Funs, Output, ConfTree);
		Children ->
		    %% TODO: There are things missing
		    {_IsMsgMerge, {Tag, Ts, _Payload}} = MessageMerge,
		    [State1, State2] = send_merge_requests({Tag, Ts}, Children),
		    MergedState = MFun(State1, State2),
		    NewState = handle_message(MessageMerge, MergedState, Output, UFun, ConfTree),
		    [Pred1, Pred2] = configuration:find_children_preds(self(), ConfTree),
		    {NewState1, NewState2} = SFun({Pred1, Pred2}, NewState),
		    [C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
		    loop(NewState, Funs, Output, ConfTree)
	    end
    end.

-spec handle_message(message() | merge_request(), State::any(), pid(), update_fun(), configuration()) 
		    -> State::any().
handle_message({msg, Msg}, State, Output, UFun, _Conf) ->
    update_on_msg(Msg, State, Output, UFun);
handle_message({merge, {_Tag, _Ts, Father}}, State, _Output, _UFun, Conf) ->
    respond_to_merge(Father, State, Conf).

-spec update_on_msg(message(), State::any(), pid(), update_fun()) -> State::any().
update_on_msg(Msg, State, Output, UFun) ->
    NewState = UFun(Msg, State, Output),    
    NewState.

-spec respond_to_merge(pid(), State::any(), configuration()) -> State::any().
respond_to_merge(Father, State, ConfTree) ->
    %% We have to send our mailbox's pid to our parent
    %% so that they can recognize where does this state message come from
    Self = self(),
    {node, Self, MPid, _P, _Children} = 
	configuration:find_node(Self, ConfTree),
    Father ! {state, {MPid, State}},
    receive
	{state, NewState} ->
	    NewState
    end.

-spec receive_state(pid()) -> State::any().
receive_state(C) ->
    receive
	{state, {C, State}} ->
	    State
    end.

-spec send_merge_requests({tag(), integer()}, [pid()]) -> [State::any()].
send_merge_requests({Tag, Ts}, Children) ->
    [C ! {merge, {Tag, Ts, self()}} || C <- Children],
    [receive_state(C) || C <- Children].

