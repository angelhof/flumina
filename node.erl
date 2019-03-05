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
	    Buffers = maps:from_list([{T, []} || T <-  AllDependingTags]),
	    mailbox({Buffers, Timers}, RelevantDependencies, Pred, Attachee, ConfTree)
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
-spec mailbox(buffers_timers(), dependencies(), message_predicate(), pid(), configuration()) -> no_return().
mailbox(BuffersTimers, Dependencies, Pred, Attachee, ConfTree) ->
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
	    route_message_and_merge_requests(Msg, ConfTree),
	    mailbox(BuffersTimers, Dependencies, Pred, Attachee, ConfTree);
	{msg, Msg} ->
	    case Pred(Msg) of
		false ->
		    %% This should be unreachable because all the messages 
		    %% are routed to a node that can indeed handle them
		    util:err("The message: ~p doesn't satisfy ~p's predicate~n", [Msg, Attachee]),
		    erlang:halt(1);
		true ->
		    %% Whenever a new message arrives, we add it to the buffer
		    NewBuffersTimers = add_to_buffers_timers({msg, Msg}, BuffersTimers),
		    {Tag, Ts, _Payload} = Msg,
		    %% And we then clear the buffer based on it, as messages also act as heartbeats
		    ClearedBuffersTimers = 
			update_timers_clear_buffers({Tag, Ts}, NewBuffersTimers, Dependencies, Attachee),
		    %% NewMessageBuffer = add_to_buffer_or_send(Msg, MessageBuffer, Dependencies, Attachee),
		    %% io:format("Message: ~p -- NewMessagebuffer: ~p~n", [Msg, NewMessageBuffer]),
		    mailbox(ClearedBuffersTimers, Dependencies, Pred, Attachee, ConfTree)
	    end;
	{merge, {Tag, Ts, Father}} ->
	    %% A merge requests acts as two different messages in our model.
	    %% - A heartbeat message, because it shows that some ancestor has
	    %%   received all messages with Tag until Ts. Because of that we 
	    %%   need to clear the buffer with it as if it was a heartbeat.
	    %% - A message that will be processed like every other message (after
	    %%   its dependencies are dealt with), so we have to add it to the buffer
	    %%   like we do with every other message
	    NewBuffersTimers = add_to_buffers_timers({merge, {Tag, Ts, Father}}, BuffersTimers),
	    ClearedBuffersTimers = 
		update_timers_clear_buffers({Tag, Ts}, NewBuffersTimers, Dependencies, Attachee),
	    %% io:format("~p -- After Merge: ~p~n", [self(), ClearedMessageBuffer]),
	    mailbox(ClearedBuffersTimers, Dependencies, Pred, Attachee, ConfTree);
	{state, State} ->
	    %% This is the reply of a child node with its state 
	    Attachee ! {state, State},
	    mailbox(BuffersTimers, Dependencies, Pred, Attachee, ConfTree);
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
	    mailbox(BuffersTimers, Dependencies, Pred, Attachee, ConfTree);
	{heartbeat, TagTs} ->
	    %% A heartbeat clears the buffer and updates the timers
	    NewBuffersTimers = 
		update_timers_clear_buffers(TagTs, BuffersTimers, Dependencies, Attachee),
	    %% io:format("Hearbeat: ~p -- NewMessagebuffer: ~p~n", [TagTs, NewBuffersTimers]),
	    mailbox(NewBuffersTimers, Dependencies, Pred, Attachee, ConfTree)
    end.


%% This function updates the timer for the newly received tag and clears
%% any buffer that depends on this tag
-spec update_timers_clear_buffers({tag(), integer()}, buffers_timers(), dependencies(), pid())
				 -> buffers_timers().
update_timers_clear_buffers({Tag, Ts}, {Buffers, Timers}, Deps, Attachee) ->
    %% A new message always updates the timers (As we assume that 
    %% messages of the same tag all arrive from the same channel,
    %% and that channels are FIFO)
    NewTimers = maps:put(Tag, Ts, Timers),
    %% After updating the timer for Tag, any message that we have 
    %% waiting in the buffers dependent to tag, could be ready for
    %% releasing. Thus we add all those dependencies to the workset.
    %% NOTE: We also add the tag itself to the workset, because
    %%       if the newly received message is of that tag, but
    %%       this tag doesn't depend on itself, then it will stay 
    %%       in the buffer and not be initiated
    TagDeps = maps:get(Tag, Deps),
    clear_buffers([Tag|TagDeps], {Buffers, NewTimers}, Deps, Attachee).
    

%% This function tries to clear the buffer of every tag in 
%% its workset. If a message of a specific tag sigma is indeed released,
%% then all of its dependencies are added in the workset
%% because after this release any dependent message to it
%% could be potentially releasable.
-spec clear_buffers([tag()], buffers_timers(), dependencies(), pid()) -> buffers_timers().
clear_buffers([], BuffersTimers, _Deps, _Attachee) ->
    BuffersTimers;
clear_buffers([WorkTag|WorkSet], BuffersTimers, Deps, Attachee) ->
    {NewWorkTags, NewBuffersTimers} = clear_tag_buffer(WorkTag, BuffersTimers, Deps, Attachee),
    %% WARNING: The following code is a very ugly way 
    %% to implement set union and there are probably 
    %% several other ways to do it (with sorted lists etc)
    NewWorkSet = sets:to_list(sets:union(
				sets:from_list(WorkSet), 
				sets:from_list(NewWorkTags))),
    clear_buffers(NewWorkSet, NewBuffersTimers, Deps, Attachee).

%% This function releases any message that is releasable
%% for the WorkTag, and returns all of its dependent tags (EXCLUDING ITSELF)
%% as the new worktags if any message is released.
-spec clear_tag_buffer(tag(), buffers_timers(), dependencies(), pid()) -> {[tag()], buffers_timers()}.
clear_tag_buffer(WorkTag, {Buffers, Timers}, Deps, Attachee) ->
    Buffer = maps:get(WorkTag, Buffers),
    %% We exclude the worktag from the dependencies, because
    %% each message that we will check will be the earliest of its tag
    %% and it will certainly be smaller than its timestamp.
    %% Also we don't want to return it as a new work tag
    %% because we just released all its messages
    TagDeps = maps:get(WorkTag, Deps) -- [WorkTag],
    case clear_buffer0(Buffer, {Buffers, Timers}, TagDeps, Attachee) of
	{released, NewBuffer} ->
	    NewBuffers = maps:update(WorkTag, NewBuffer, Buffers),
	    {TagDeps, {NewBuffers, Timers}};
	{not_released, _} ->
	    {[], {Buffers, Timers}}
    end.

-spec clear_buffer0([gen_message_or_merge()], buffers_timers(), [tag()], pid()) 
		   -> {'released' | 'not_released', [gen_message_or_merge()]}.
clear_buffer0(Buffer, {Buffers, Timers}, TagDeps, Attachee) ->
    clear_buffer0(Buffer, {Buffers, Timers}, TagDeps, Attachee, not_released).

-spec clear_buffer0([gen_message_or_merge()], buffers_timers(), [tag()], pid(), 'released' | 'not_released') 
		   -> {'released' | 'not_released', [gen_message_or_merge()]}.
clear_buffer0([], {_Buffers, _Timers}, _TagDeps, _Attachee, AnyReleased) ->
    {AnyReleased, []};
clear_buffer0([Msg|Buffer], {Buffers, Timers}, TagDeps, Attachee, AnyReleased) ->
    case maybe_release_message(Msg, {Buffers, Timers}, TagDeps, Attachee) of
	released ->
	    clear_buffer0(Buffer, {Buffers, Timers}, TagDeps, Attachee, released);
	not_released ->
	    {AnyReleased, [Msg|Buffer]}
    end.
	    

%% This function checks whether to release a message
-spec maybe_release_message(gen_message_or_merge(), buffers_timers(), [tag()], pid()) 
			   -> 'released' | 'not_released'.
maybe_release_message(Msg, {Buffers, Timers}, TagDeps, Attachee) ->
    {_MsgOrMerge, {Tag, Ts, _Payload}} = Msg,
    %% 1. All its dependent timers must be higher than the
    %%    the timestamp of the message
    Cond1 = lists:all(fun(TD) -> Ts =< maps:get(TD, Timers) end, TagDeps),
    %% 2. All the messages that are dependent to it in their buffers
    %%    should have a later timestamp than it (if there are any at all).
    Cond2 = lists:all(fun(TD) -> empty_or_later({Tag, Ts}, maps:get(TD, Buffers)) end, TagDeps),
    case Cond1 andalso Cond2 of
	true ->
	    Attachee ! Msg,
	    released;
	false ->
	    not_released
    end.

 
-spec empty_or_later({tag(), integer()}, [gen_message_or_merge()]) -> boolean().
empty_or_later(_TagTs, []) ->
    true;
empty_or_later({Tag, Ts}, [{_MsgOrMerge, {BTag, BTs, _Payload}}|_Rest]) ->
    %% Note: I am comparing the tuple {Ts, Tag} to have a total ordering 
    %% between messages with different tags but the same timestamp. 
    %% Regarding correctness, we should be allowed to reorder concurrent
    %% messages, any way we want.
    {Ts, Tag} =< {BTs, BTag}.

%% This function inserts a newly arrived message to the buffers
-spec add_to_buffers_timers(gen_message_or_merge(), buffers_timers()) -> buffers_timers().
add_to_buffers_timers(Msg, {Buffers, Timers}) ->
    {_MsgOrMerge, {Tag, _Ts, _Payload}} = Msg,
    Buffer = maps:get(Tag, Buffers),
    NewBuffer = add_to_buffer0(Msg, Buffer),
    NewBuffers = maps:update(Tag, NewBuffer, Buffers),
    {NewBuffers, Timers}.

-spec add_to_buffer0(gen_message_or_merge(), [gen_message_or_merge()]) -> [gen_message_or_merge()].
add_to_buffer0(Msg, Buffer) ->
    add_to_buffer0(Msg, Buffer, []).

-spec add_to_buffer0(gen_message_or_merge(), [gen_message_or_merge()], [gen_message_or_merge()]) 
		    -> [gen_message_or_merge()].
add_to_buffer0(Msg, [], NewBuffer) ->
    lists:reverse([Msg|NewBuffer]);
add_to_buffer0(Msg, [BMsg|Buf], NewBuf) ->
    {_MsgOrMerge, {Tag, Ts, Payload}} = Msg,
    {_BMsgOrMerge, {BTag, BTs, _}} = BMsg,
    %% Note: I am comparing the tuple {Ts, Tag} to have a total ordering 
    %% between messages with different tags but the same timestamp. 
    %% NOTE: This is left over from before, all messages in the same
    %%       buffer should have the same tag.
    case {Ts, Tag} < {BTs, BTag} of
	true ->
	    lists:reverse([Msg|NewBuf]) ++ [BMsg|Buf];
	false ->
	    add_to_buffer0(Msg, Buf, [BMsg|NewBuf])
    end.
    

%% This function sends the message to the head node of the subtree,
%% and the merge request to all its children
-spec route_message_and_merge_requests(gen_message(), configuration()) -> 'ok'.
route_message_and_merge_requests(Msg, ConfTree) ->
    %% This finds the head node of the subtree
    [{SendTo, undef}|Rest] = router:find_responsible_subtree_pids(ConfTree, Msg),
    SendTo ! {msg, Msg},
    {Tag, Ts, _Payload} = Msg,
    [To ! {merge, {Tag, Ts, ToFather}} || {To, ToFather} <- Rest],
    ok.


%% Broadcasts the heartbeat to those who are responsible for it
%% Responsible is the beta-mapping or the predicate (?) are those the same?
-spec broadcast_heartbeat({tag(), integer()}, configuration()) -> [gen_heartbeat()].
broadcast_heartbeat({Tag, Ts}, ConfTree) ->    
    %% WARNING: WE HAVE MADE THE ASSUMPTION THAT EACH ROOT NODE PROCESSES A DIFFERENT
    %%          SET OF TAGS. SO THE OR-SPLIT NEVER REALLY CHOOSES BETWEEN TWO AT THE MOMENT
    [{SendTo, undef}|Rest] = router:find_responsible_subtree_pids(ConfTree, {Tag, Ts, heartbeat}),
    SendTo ! {heartbeat, {Tag, Ts}},
    [To ! {heartbeat, {Tag, Ts}} || {To, _ToFather} <- Rest].
    %% Old implementation of broadcast heartbeat
    %% AllPids = router:heartbeat_route({Tag, Ts, heartbeat}, ConfTree),
    %% [P ! {heartbeat, {Tag, Ts}} || P <- AllPids].

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
        {MsgOrMerge, _} = MessageMerge when MsgOrMerge =:= msg orelse MsgOrMerge =:= merge ->
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

-spec handle_message(gen_message() | gen_merge_request(), State::any(), pid(), update_fun(), configuration()) 
		    -> State::any().
handle_message({msg, Msg}, State, Output, UFun, _Conf) ->
    update_on_msg(Msg, State, Output, UFun);
handle_message({merge, {_Tag, _Ts, Father}}, State, _Output, _UFun, Conf) ->
    respond_to_merge(Father, State, Conf).

-spec update_on_msg(gen_message(), State::any(), pid(), update_fun()) -> State::any().
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
    %% [C ! {merge, {Tag, Ts, self()}} || C <- Children],
    [receive_state(C) || C <- Children].

-spec id(any()) -> any().
id(X) ->
    X.
