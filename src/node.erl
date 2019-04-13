-module(node).

-export([node/8,
	 init_mailbox/4,
	 mailbox/5,
	 init_node/5,
	 loop/6]).

-include("type_definitions.hrl").

-record(funs, {upd = undefined :: update_fun(),
	       spl = undefined :: split_fun(),
	       mrg = undefined :: merge_fun()}).

%% Initializes and spawns a node and its mailbox
-spec node(State::any(), mailbox(), message_predicate(), spec_functions(), 
	   conf_gen_options_rec(), dependencies(), mailbox(), integer()) 
	  -> {pid(), mailbox()}.
node(State, {Name, Node}, Pred, {UpdateFun, SplitFun, MergeFun}, 
     #options{log_triple = LogTriple, checkpoint = CheckFun}, Dependencies, Output, Depth) ->
    Funs = #funs{upd = UpdateFun, spl = SplitFun, mrg = MergeFun},

    %% Only give the checkpoint function as argument if it is the
    %% root node of the tree (with depth =:= 0).
    NodeCheckpointFun =
	case Depth of
	    0 -> CheckFun;
	    _ -> fun conf_gen:no_checkpoint/2
	end,
    NodePid = spawn_link(Node, ?MODULE, init_node, 
			 [State, Funs, LogTriple, NodeCheckpointFun, Output]),
    _MailboxPid = spawn_link(Node, ?MODULE, init_mailbox, [Name, Dependencies, Pred, NodePid]),
    %% We return the mailbox pid because every message should first arrive to the mailbox
    {NodePid, {Name, Node}}.



%%
%% Mailbox
%%

-spec init_mailbox(atom(), dependencies(), message_predicate(), pid()) -> no_return().
init_mailbox(Name, Dependencies, Pred, Attachee) ->

    %% Register the mailbox to have a name
    true = register(Name, self()),

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
	    Buffers = maps:from_list([{T, queue:new()} || T <-  AllDependingTags]),
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
    {ok, Predicate} = configuration:get_relevant_predicates(Attachee, ConfTree),
    Dependencies1 = 
	maps:map(
	  fun(_Tag, DTags) ->
		  %% [io:format("~p -> ~p || ~p : ~p~n", 
		  %% 	    [Tag, DT, self(), not DescendantPred({DT, undef, undef})])
		  %% 	    || DT <- DTags],
		  [DT || DT <- DTags, Predicate({DT, 0, 0})]
	  end, Dependencies0),
    %% io:format("Clean Deps:~p~n~p~n", [self(), Dependencies1]),
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
	    %% io:format("~p -- After Merge: ~p~n", [self(), ClearedBuffersTimers]),
	    %% io:format("~p -- ~p~n", [self(), erlang:process_info(self(), message_queue_len)]),
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
	    mailbox(NewBuffersTimers, Dependencies, Pred, Attachee, ConfTree);
	{get_message_log, ReplyTo} ->
	    Attachee ! {get_message_log, ReplyTo},
	    mailbox(BuffersTimers, Dependencies, Pred, Attachee, ConfTree)
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
    case clear_buffer(Buffer, {Buffers, Timers}, TagDeps, Attachee) of
	{released, NewBuffer} ->
	    NewBuffers = maps:update(WorkTag, NewBuffer, Buffers),
	    {TagDeps, {NewBuffers, Timers}};
	{not_released, _} ->
	    {[], {Buffers, Timers}}
    end.

-spec clear_buffer(buffer(), buffers_timers(), [tag()], pid()) 
		   -> {'released' | 'not_released', buffer()}.
clear_buffer(Buffer, {Buffers, Timers}, TagDeps, Attachee) ->
    clear_buffer(Buffer, {Buffers, Timers}, TagDeps, Attachee, not_released).

-spec clear_buffer(buffer(), buffers_timers(), [tag()], pid(), 'released' | 'not_released') 
		   -> {'released' | 'not_released', buffer()}.
clear_buffer(Buffer, {Buffers, Timers}, TagDeps, Attachee, AnyReleased) ->
    case queue:out(Buffer) of
	{empty, Buffer} ->
	    {AnyReleased, Buffer};
	{{value, Msg}, Rest} ->
	    case maybe_release_message(Msg, {Buffers, Timers}, TagDeps, Attachee) of
		released ->
		    clear_buffer(Rest, {Buffers, Timers}, TagDeps, Attachee, released);
		not_released ->
		    {AnyReleased, Buffer}
	    end
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

 
-spec empty_or_later({tag(), integer()}, buffer()) -> boolean().
empty_or_later({Tag, Ts}, Buffer) ->
    case queue:peek(Buffer) of
	empty -> 
	    true;
	{value, {_MsgOrMerge, {BTag, BTs, _Payload}}} ->
	    %% Note: I am comparing the tuple {Ts, Tag} to have a total ordering 
	    %% between messages with different tags but the same timestamp. 
	    %% Regarding correctness, we should be allowed to reorder concurrent
	    %% messages, any way we want.
	    {Ts, Tag} =< {BTs, BTag}
    end.

%% This function inserts a newly arrived message to the buffers
-spec add_to_buffers_timers(gen_message_or_merge(), buffers_timers()) -> buffers_timers().
add_to_buffers_timers(Msg, {Buffers, Timers}) ->
    {_MsgOrMerge, {Tag, _Ts, _Payload}} = Msg,
    Buffer = maps:get(Tag, Buffers),
    NewBuffer = add_to_buffer(Msg, Buffer),
    NewBuffers = maps:update(Tag, NewBuffer, Buffers),
    {NewBuffers, Timers}.

%% This function adds a newly arrived message to its buffer.
%% As messages arrive from the same channel, we can be certain that 
%% any message will be the last one on its buffer
-spec add_to_buffer(gen_message_or_merge(), buffer()) -> buffer().
add_to_buffer(Msg, Buffer) ->
    queue:in(Msg, Buffer).    

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
-spec init_node(State::any(), #funs{}, num_log_triple(), 
		checkpoint_predicate(), mailbox()) -> no_return().
init_node(State, Funs, LogTriple, CheckPred, Output) ->
    %% Before executing the main loop receive the
    %% Configuration tree, which can only be received
    %% after all the nodes have already been spawned
    receive
	{configuration, ConfTree} ->
	    loop(State, Funs, LogTriple, CheckPred, Output, ConfTree)
    end.
	

%% This is the main loop that each node executes.
-spec loop(State::any(), #funs{}, num_log_triple(), checkpoint_predicate(), 
	   mailbox(), configuration()) -> no_return().
loop(State, Funs = #funs{upd=UFun, spl=SFun, mrg=MFun}, 
     {LogFun, ResetFun, LogState} = LogTriple, CheckPred, Output, ConfTree) ->
    receive
        {MsgOrMerge, _} = MessageMerge when MsgOrMerge =:= msg orelse MsgOrMerge =:= merge ->
	    %% The mailbox has cleared this message so we don't need to check for pred
	    {NewLogState, NewState} = 
		case configuration:find_children_mbox_pids(self(), ConfTree) of
		    [] ->
			handle_message(MessageMerge, State, Output, UFun, LogTriple, ConfTree);
		    Children ->
			%% TODO: There are things missing
			{_IsMsgMerge, {Tag, Ts, _Payload}} = MessageMerge,
			{LogState1, [State1, State2]} = 
			    receive_states({Tag, Ts}, Children, LogTriple),
			MergedState = MFun(State1, State2),
			{LogState2, NewState0} = 
			    handle_message(MessageMerge, MergedState, Output, UFun, 
					   {LogFun, ResetFun, LogState1}, ConfTree),
			[Pred1, Pred2] = configuration:find_children_preds(self(), ConfTree),
			{NewState1, NewState2} = SFun({Pred1, Pred2}, NewState0),
			[C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
			{LogState2, NewState0}
		end,
	    %% Check whether to create a checkpoint 
	    NewCheckPred = CheckPred(MessageMerge, NewState),
	    %% Maybe log some information about the message
	    FinalLogState = LogFun(MsgOrMerge, NewLogState),
	    loop(NewState, Funs, {LogFun, ResetFun, FinalLogState}, NewCheckPred, Output, ConfTree);
	{get_message_log, ReplyTo} = GetLogMsg ->
	    NewLogState = handle_get_message_log(GetLogMsg, {LogFun, ResetFun, LogState}),
	    loop(State, Funs, {LogFun, ResetFun, NewLogState}, CheckPred, Output, ConfTree)
    end.

-spec handle_message(gen_message() | gen_merge_request(), State::any(), mailbox(), 
		     update_fun(), num_log_triple(), configuration()) 
		    -> {num_log_state(), State::any()}.
handle_message({msg, Msg}, State, Output, UFun, {_, _, LogState}, _Conf) ->
    {LogState, update_on_msg(Msg, State, Output, UFun)};
handle_message({merge, {_Tag, _Ts, Father}}, State, _Output, _UFun, LogTriple, Conf) ->
    respond_to_merge(Father, State, LogTriple, Conf).

-spec update_on_msg(gen_message(), State::any(), mailbox(), update_fun()) -> State::any().
update_on_msg(Msg, State, Output, UFun) ->
    NewState = UFun(Msg, State, Output),    
    NewState.

-spec respond_to_merge(pid(), State::any(), num_log_triple(), configuration())
		      -> {num_log_state(), State::any()}.
respond_to_merge(Father, State, LogTriple, ConfTree) ->
    %% We have to send our mailbox's pid to our parent
    %% so that they can recognize where does this state message come from
    Self = self(),
    {node, Self, MboxNameNode, _P, _Children} = 
	configuration:find_node(Self, ConfTree),
    Father ! {state, {MboxNameNode, State}},
    receive_new_state_or_get_message_log(LogTriple).


%%
%% The following functions have become dirty because of logging.
%% TODO: Clean up, and disentangle
%%

%% This function waits for the new state, but can also handle
%% get_message_log messages so that they are not stuck in the
%% mailbox
-spec receive_new_state_or_get_message_log(num_log_triple()) -> {num_log_state(), State::any()}.
receive_new_state_or_get_message_log({LogFun, ResetFun, LogState}) ->
    %% WARNING: There is an issue with that here.
    %%          It searches through the whole mailbox for a state
    %%          message. If the load is unhandleable, then it might
    %%          create overheads.
    receive
	{state, NewState} ->
	    {LogState, NewState};
	{get_message_log, ReplyTo} = GetLogMsg ->
	    NewLogState = handle_get_message_log(GetLogMsg, {LogFun, ResetFun, LogState}),
	    receive_new_state_or_get_message_log({LogFun, ResetFun, NewLogState})
    end.

-spec handle_get_message_log({'get_message_log', mailbox()}, num_log_triple()) -> num_log_state().
handle_get_message_log({get_message_log, ReplyTo}, {_LogFun, ResetFun, LogState}) ->
    %% Reply to ReplyTo with the number of processed messages
    ReplyTo ! {message_log, {self(), node()}, LogState},
    %% Reset the num_state
    ResetFun(LogState).

-spec receive_state_or_get_message_log(mailbox(), {[State::any()], num_log_triple()}) 
				      -> {[State::any()], num_log_triple()}.
receive_state_or_get_message_log(C, {States, {LogFun, ResetFun, LogState}}) ->
    %% WARNING: There is an issue with that here.
    %%          It searches through the whole mailbox for a state
    %%          message. If the load is unhandleable, then it might
    %%          create overheads.
    receive
	{state, {C, State}} ->
	    {[State|States], {LogFun, ResetFun, LogState}};
	{get_message_log, ReplyTo} = GetLogMsg ->
	    NewLogState = handle_get_message_log(GetLogMsg, {LogFun, ResetFun, LogState}),
	    receive_state_or_get_message_log(C, {States, {LogFun, ResetFun, NewLogState}})
    end.

-spec receive_states({tag(), integer()}, [mailbox()], num_log_triple()) 
		    -> {num_log_state(), [State::any()]}.
receive_states({_Tag, _Ts}, Children, LogTriple) ->
    {States, {_, _, NewLogState}} =
	lists:foldr(fun receive_state_or_get_message_log/2, {[], LogTriple}, Children),
    %% [receive_state(C) || C <- Children].
    {NewLogState, States}.
