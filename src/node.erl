-module(node).

-export([node/9,
	 init_mailbox/6,
	 mailbox/1,
	 init_node/5,
	 loop/7]).

-include("type_definitions.hrl").

-record(funs, {upd = undefined :: update_fun(),
	       spl = undefined :: split_fun(),
	       mrg = undefined :: merge_fun()}).

%% Initializes and spawns a node and its mailbox
-spec node(State::any(), mailbox(), impl_message_predicate(), spec_functions(), 
	   conf_gen_options_rec(), dependencies(), mailbox(), integer(),
	   impl_tags()) 
	  -> {pid(), mailbox()}.
node(State, {Name, Node}, Pred, {UpdateFun, SplitFun, MergeFun}, 
     #options{log_triple = LogTriple, checkpoint = CheckFun}, Dependencies, 
     Output, Depth, ImplTags) ->
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
    _MailboxPid = spawn_link(Node, ?MODULE, init_mailbox, 
			    [Name, Dependencies, Pred, NodePid, {master, node()}, ImplTags]),
    %% Make sure that the mailbox has registered its name
    receive
	{registered, Name} ->
	    {NodePid, {Name, Node}}
    end.



%%
%% Mailbox
%%

-spec init_mailbox(atom(), dependencies(), impl_message_predicate(), pid(), 
		   mailbox(), impl_tags()) -> no_return().
init_mailbox(Name, Dependencies, Pred, Attachee, Master, ImplTags) ->
    log_mod:init_debug_log(),
    %% Register the mailbox to have a name
    true = register(Name, self()),
    Master ! {registered, Name},
    
    %% Set the priority of the mailboxes to high, so that it can handle tis messages.
    %% erlang:process_flag(priority, high),

    %% Before executing the main loop receive the
    %% Configuration tree, which can only be received
    %% after all the nodes have already been spawned
    receive
	{configuration, ConfTree} ->
	    Attachee ! {configuration, ConfTree},
	    
	    log_mod:debug_log("Ts: ~s -- Mailbox ~p in ~p received configuration~n", 
			      [util:local_timestamp(),self(), node()]),
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
	    
	    RelevantDependencies =
		filter_relevant_dependencies(Dependencies, Attachee, ConfTree, ImplTags),

	    %% All the tags that we depend on
	    AllDependingImplTags = lists:flatten(maps:values(RelevantDependencies)),
	    Timers = maps:from_list([{T, 0} || T <-  AllDependingImplTags]),
	    Buffers = maps:from_list([{T, queue:new()} || T <-  AllDependingImplTags]),
            MboxState = #mb_st{buffers = {Buffers, Timers},
                               deps = RelevantDependencies,
                               pred = Pred,
                               attachee = Attachee,
                               conf = ConfTree},
	    mailbox(MboxState)
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
-spec filter_relevant_dependencies(dependencies(), pid(), configuration(), impl_tags()) -> impl_dependencies().
filter_relevant_dependencies(Dependencies, Attachee, ConfTree, ImplTags) ->
    {ok, Predicate} = configuration:get_relevant_predicates(Attachee, ConfTree),
    %% Find all the implementation tags that are in this mailbox's predicate

    ImplDependencies =
	lists:map(
	  fun({Tag, _Node} = ImplTag) ->
		  %% For each implementation tag
		  %% find the tags that depend on its
		  %% specification tag
		  DepTags = sets:from_list(maps:get(Tag, Dependencies)),
		  %% Then find all implementation tags that have these
		  %% specification tags, and only keep the relevant ones
		  %% as dependencies
		  RelevantImplTags = 
		      [{Tag1, Node1} || {Tag1, Node1} <- ImplTags,
				  Predicate({{Tag1, undef}, Node1, 0}) 
				      andalso sets:is_element(Tag1, DepTags)],
		  %% io:format("The implementation tag: ~p~n" 
		  %% 	    " has specification deps: ~p~n"
		  %% 	    " and implementation deps: ~p~n",
		  %% 	    [ImplTag, DepTags, RelevantImplTags]),
		  {ImplTag, RelevantImplTags}
	  end, ImplTags),
    %% io:format("Implementation Tag Dependencies for: ~p~n~p~n", [self(), ImplDependencies]),
    maps:from_list(ImplDependencies).
    %% Dependencies1 = 
    %% 	maps:map(
    %% 	  fun(_Tag, DTags) ->
    %% 		  %% [io:format("~p -> ~p || ~p : ~p~n", 
    %% 		  %% 	    [Tag, DT, self(), not DescendantPred({DT, undef, undef})])
    %% 		  %% 	    || DT <- DTags],
    %% 		  [DT || DT <- DTags, Predicate({{DT, 0}, undefined, 0})]
    %% 	  end, Dependencies0),
				   
    %% %% io:format("Clean Deps:~p~n~p~n", [self(), Dependencies1]),
    %% Dependencies1.
   

%% This is the mailbox process that routes to 
%% their correct nodes and makes sure that
%% dependent messages arrive in order
-spec mailbox(mailbox_state()) -> no_return().
mailbox(MboxState) ->
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
            ConfTree = MboxState#mb_st.conf,
	    route_message_and_merge_requests(Msg, ConfTree),
	    mailbox(MboxState);
	{msg, Msg} ->
            Pred = MboxState#mb_st.pred,
	    case Pred(Msg) of
		false ->
		    %% This should be unreachable because all the messages 
		    %% are routed to a node that can indeed handle them
		    log_mod:debug_log("Ts: ~s -- Mailbox ~p in ~p was sent msg: ~p ~n"
				      "  that doesn't satisfy its predicate.~n",
				      [util:local_timestamp(),self(), node(), Msg]),
                    Attachee = MboxState#mb_st.attachee,
		    util:err("The message: ~p doesn't satisfy ~p's predicate~n", [Msg, Attachee]),
		    erlang:halt(1);
		true ->
		    %% Whenever a new message arrives, we add it to its buffer
                    BuffersTimers = MboxState#mb_st.buffers,
		    NewBuffersTimers = add_to_buffers_timers({msg, Msg}, BuffersTimers),
		    {{Tag, _Payload}, Node, Ts} = Msg,
		    ImplTag = {Tag, Node},
		    %% And we then clear the buffer based on it, as messages also act as heartbeats
                    Attachee = MboxState#mb_st.attachee,
                    Dependencies = MboxState#mb_st.deps,
		    ClearedBuffersTimers = 
			update_timers_clear_buffers({ImplTag, Ts}, NewBuffersTimers, Dependencies, Attachee),
		    %% NewMessageBuffer = add_to_buffer_or_send(Msg, MessageBuffer, Dependencies, Attachee),
		    %% io:format("Message: ~p -- NewMessagebuffer: ~p~n", [Msg, NewMessageBuffer]),
                    NewMboxState = MboxState#mb_st{buffers = ClearedBuffersTimers},
		    mailbox(NewMboxState)
	    end;
	{merge, {{Tag, Father}, Node, Ts}} ->
	    %% A merge requests acts as two different messages in our model.
	    %% - A heartbeat message, because it shows that some ancestor has
	    %%   received all messages with Tag until Ts. Because of that we 
	    %%   need to clear the buffer with it as if it was a heartbeat.
	    %% - A message that will be processed like every other message (after
	    %%   its dependencies are dealt with), so we have to add it to the buffer
	    %%   like we do with every other message
	    ImplTag = {Tag, Node},
            BuffersTimers = MboxState#mb_st.buffers,
            Attachee = MboxState#mb_st.attachee,
            Dependencies = MboxState#mb_st.deps,
	    NewBuffersTimers = add_to_buffers_timers({merge, {{Tag, Father}, Node, Ts}}, BuffersTimers),
	    ClearedBuffersTimers = 
		update_timers_clear_buffers({ImplTag, Ts}, NewBuffersTimers, Dependencies, Attachee),
	    %% io:format("~p -- After Merge: ~p~n", [self(), ClearedBuffersTimers]),
	    %% io:format("~p -- ~p~n", [self(), erlang:process_info(self(), message_queue_len)]),
            NewMboxState = MboxState#mb_st{buffers = ClearedBuffersTimers},
            mailbox(NewMboxState);
	{state, State} ->
	    %% This is the reply of a child node with its state
            Attachee = MboxState#mb_st.attachee, 
	    Attachee ! {state, State},
	    mailbox(MboxState);
	{iheartbeat, ImplTagTs} ->
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
            ConfTree = MboxState#mb_st.conf, 
	    broadcast_heartbeat(ImplTagTs, ConfTree),
	    mailbox(MboxState);
	{heartbeat, ImplTagTs} ->
	    %% A heartbeat clears the buffer and updates the timers
            Attachee = MboxState#mb_st.attachee,
            Dependencies = MboxState#mb_st.deps,
            BuffersTimers = MboxState#mb_st.buffers,
	    NewBuffersTimers = 
		update_timers_clear_buffers(ImplTagTs, BuffersTimers, Dependencies, Attachee),
	    %% io:format("Hearbeat: ~p -- NewMessagebuffer: ~p~n", [TagTs, NewBuffersTimers]),
            NewMboxState = MboxState#mb_st{buffers = NewBuffersTimers},
            mailbox(NewMboxState);
	{get_message_log, ReplyTo} ->
            Attachee = MboxState#mb_st.attachee,
            BuffersTimers = MboxState#mb_st.buffers,
	    log_mod:debug_log("Ts: ~s -- Mailbox ~p in ~p was asked for throughput.~n" 
			      " -- Its erl_mailbox_size is: ~p~n"
			      " -- Its buffer mailbox size is: ~p~n",
			      [util:local_timestamp(),self(), node(), 
			       erlang:process_info(self(), message_queue_len),
			       buffers_length(BuffersTimers)]),
	    Attachee ! {get_message_log, ReplyTo},
	    mailbox(MboxState)
    end.


%% This function updates the timer for the newly received tag and clears
%% any buffer that depends on this tag
-spec update_timers_clear_buffers({impl_tag(), integer()}, buffers_timers(), impl_dependencies(), pid())
				 -> buffers_timers().
update_timers_clear_buffers({ImplTag, Ts}, {Buffers, Timers}, ImplDeps, Attachee) ->
    %% A new message always updates the timers (As we assume that 
    %% messages of the same tag all arrive from the same channel,
    %% and that channels are FIFO)
    NewTimers = maps:put(ImplTag, Ts, Timers),
    %% After updating the timer for Tag, any message that we have 
    %% waiting in the buffers dependent to tag, could be ready for
    %% releasing. Thus we add all those dependencies to the workset.
    %% NOTE: We also add the tag itself to the workset, because
    %%       if the newly received message is of that tag, but
    %%       this tag doesn't depend on itself, then it will stay 
    %%       in the buffer and not be initiated
    ImplTagDeps = maps:get(ImplTag, ImplDeps),
    clear_buffers([ImplTag|ImplTagDeps], {Buffers, NewTimers}, ImplDeps, Attachee).
    

%% This function tries to clear the buffer of every tag in 
%% its workset. If a message of a specific tag sigma is indeed released,
%% then all of its dependencies are added in the workset
%% because after this release any dependent message to it
%% could be potentially releasable.
-spec clear_buffers([impl_tag()], buffers_timers(), impl_dependencies(), pid()) -> buffers_timers().
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
-spec clear_tag_buffer(impl_tag(), buffers_timers(), impl_dependencies(), pid()) 
		      -> {[impl_tag()], buffers_timers()}.
clear_tag_buffer(ImplWorkTag, {Buffers, Timers}, ImplDeps, Attachee) ->
    Buffer = maps:get(ImplWorkTag, Buffers),
    %% We exclude the worktag from the dependencies, because
    %% each message that we will check will be the earliest of its tag
    %% and it will certainly be smaller than its timestamp.
    %% Also we don't want to return it as a new work tag
    %% because we just released all its messages
    ImplTagDeps = maps:get(ImplWorkTag, ImplDeps) -- [ImplWorkTag],
    case clear_buffer(Buffer, {Buffers, Timers}, ImplTagDeps, Attachee) of
	{released, NewBuffer} ->
	    NewBuffers = maps:update(ImplWorkTag, NewBuffer, Buffers),
	    {ImplTagDeps, {NewBuffers, Timers}};
	{not_released, _} ->
	    {[], {Buffers, Timers}}
    end.

-spec clear_buffer(buffer(), buffers_timers(), [impl_tag()], pid()) 
		   -> {'released' | 'not_released', buffer()}.
clear_buffer(Buffer, {Buffers, Timers}, ImplTagDeps, Attachee) ->
    clear_buffer(Buffer, {Buffers, Timers}, ImplTagDeps, Attachee, not_released).

-spec clear_buffer(buffer(), buffers_timers(), [impl_tag()], pid(), 'released' | 'not_released') 
		   -> {'released' | 'not_released', buffer()}.
clear_buffer(Buffer, {Buffers, Timers}, ImplTagDeps, Attachee, AnyReleased) ->
    case queue:out(Buffer) of
	{empty, Buffer} ->
	    {AnyReleased, Buffer};
	{{value, Msg}, Rest} ->
	    case maybe_release_message(Msg, {Buffers, Timers}, ImplTagDeps, Attachee) of
		released ->
		    clear_buffer(Rest, {Buffers, Timers}, ImplTagDeps, Attachee, released);
		not_released ->
		    {AnyReleased, Buffer}
	    end
    end.
	    

%% This function checks whether to release a message
-spec maybe_release_message(gen_message_or_merge(), buffers_timers(), [impl_tag()], pid()) 
			   -> 'released' | 'not_released'.
maybe_release_message(Msg, {Buffers, Timers}, ImplTagDeps, Attachee) ->
    {_MsgOrMerge, {{Tag, _Payload}, Node, Ts}} = Msg,
    ImplTag = {Tag, Node},
    %% 1. All its dependent timers must be higher than the
    %%    the timestamp of the message
    Cond1 = lists:all(fun(TD) -> Ts =< maps:get(TD, Timers) end, ImplTagDeps),
    %% 2. All the messages that are dependent to it in their buffers
    %%    should have a later timestamp than it (if there are any at all).
    Cond2 = lists:all(fun(TD) -> empty_or_later({ImplTag, Ts}, maps:get(TD, Buffers)) end, ImplTagDeps),
    case Cond1 andalso Cond2 of
	true ->
	    Attachee ! Msg,
	    released;
	false ->
	    not_released
    end.

 
-spec empty_or_later({impl_tag(), timestamp()}, buffer()) -> boolean().
empty_or_later({ImplTag, Ts}, Buffer) ->
    case queue:peek(Buffer) of
	empty -> 
	    true;
	{value, {_MsgOrMerge, {{BTag, _Payload}, BNode, BTs}}} ->
	    %% Note: I am comparing the tuple {Ts, Tag} to have a total ordering 
	    %% between messages with different tags but the same timestamp. 
	    %% Regarding correctness, we should be allowed to reorder concurrent
	    %% messages, any way we want.
	    BImplTag = {BTag, BNode},
	    {Ts, ImplTag} =< {BTs, BImplTag}
    end.

%% This function inserts a newly arrived message to the buffers
-spec add_to_buffers_timers(gen_message_or_merge(), buffers_timers()) -> buffers_timers().
add_to_buffers_timers(Msg, {Buffers, Timers}) ->
    {_MsgOrMerge, {{Tag, _Payload}, Node, _Ts}} = Msg,
    ImplTag = {Tag, Node},
    Buffer = maps:get(ImplTag, Buffers),
    NewBuffer = add_to_buffer(Msg, Buffer),
    NewBuffers = maps:update(ImplTag, NewBuffer, Buffers),
    {NewBuffers, Timers}.

-spec buffers_length(buffers_timers()) -> #{impl_tag() := integer()}.
buffers_length({Buffers, _Timers}) ->
    maps:map(
      fun(_ImplTag, Buffer) ->
	      queue:len(Buffer)
      end, Buffers).


%% This function adds a newly arrived message to its buffer.
%% As messages arrive from the same channel, we can be certain that 
%% any message will be the last one on its buffer
-spec add_to_buffer(gen_message_or_merge(), buffer()) -> buffer().
add_to_buffer(Msg, Buffer) ->
    queue:in(Msg, Buffer).    

%% This function sends the message to the head node of the subtree,
%% and the merge request to all its children
-spec route_message_and_merge_requests(gen_impl_message(), configuration()) -> 'ok'.
route_message_and_merge_requests(Msg, ConfTree) ->
    %% This finds the head node of the subtree
    [{SendTo, undef}|Rest] = router:find_responsible_subtree_pids(ConfTree, Msg),
    SendTo ! {msg, Msg},
    {{Tag,  _Payload}, Node, Ts} = Msg,
    [To ! {merge, {{Tag, ToFather}, Node, Ts}} || {To, ToFather} <- Rest],
    ok.


%% Broadcasts the heartbeat to those who are responsible for it
%% Responsible is the beta-mapping or the predicate (?) are those the same?
-spec broadcast_heartbeat({impl_tag(), integer()}, configuration()) -> [gen_heartbeat()].
broadcast_heartbeat({ImplTag, Ts}, ConfTree) ->
    {Tag, Node} = ImplTag,
    %% WARNING: WE HAVE MADE THE ASSUMPTION THAT EACH ROOT NODE PROCESSES A DIFFERENT
    %%          SET OF TAGS. SO THE OR-SPLIT NEVER REALLY CHOOSES BETWEEN TWO AT THE MOMENT
    [{SendTo, undef}|Rest] = router:find_responsible_subtree_pids(ConfTree, {{Tag, heartbeat}, Node, Ts}),
    SendTo ! {heartbeat, {ImplTag, Ts}},
    [To ! {heartbeat, {ImplTag, Ts}} || {To, _ToFather} <- Rest].
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
    log_mod:init_debug_log(),
    %% Before executing the main loop receive the
    %% Configuration tree, which can only be received
    %% after all the nodes have already been spawned
    receive
	{configuration, ConfTree} ->
	    log_mod:debug_log("Ts: ~s -- Node ~p in ~p received configuration~n", 
			      [util:local_timestamp(),self(), node()]),
	    %% Setup the children predicates so that they don't have to be searched 
	    %% on the configuration tree every time
	    Preds = configuration:find_children_preds(self(), ConfTree),
	    SpecPreds = configuration:find_children_spec_preds(self(), ConfTree),
	    ChildrenPredicates = {SpecPreds, Preds},
	    loop(State, Funs, LogTriple, CheckPred, Output, ChildrenPredicates, ConfTree)
    end.


%% This is the main loop that each node executes.
-spec loop(State::any(), #funs{}, num_log_triple(), checkpoint_predicate(), 
	   mailbox(), children_predicates(), configuration()) -> no_return().
loop(State, Funs = #funs{upd=UFun, spl=SFun, mrg=MFun}, 
     {LogFun, ResetFun, LogState} = LogTriple, CheckPred, Output, ChildrenPredicates, ConfTree) ->
    receive
        {MsgOrMerge, _} = MessageMerge when MsgOrMerge =:= msg orelse MsgOrMerge =:= merge ->
	    %% The mailbox has cleared this message so we don't need to check for pred
	    {NewLogState, NewState} = 
		case configuration:find_children_mbox_pids(self(), ConfTree) of
		    [] ->
			handle_message(MessageMerge, State, Output, UFun, LogTriple, ConfTree);
		    Children ->
			{_IsMsgMerge, {{Tag, _Payload}, Node, Ts}} = MessageMerge,
			ImplTag = {Tag, Node},
			{LogState1, [State1, State2]} = 
			    receive_states({ImplTag, Ts}, Children, LogTriple),
			MergedState = MFun(State1, State2),
			{LogState2, NewState0} = 
			    handle_message(MessageMerge, MergedState, Output, UFun, 
					   {LogFun, ResetFun, LogState1}, ConfTree),
			{[SpecPred1, SpecPred2], _} = ChildrenPredicates, 
			%% [Pred1, Pred2] = configuration:find_children_preds(self(), ConfTree),

			{NewState1, NewState2} = SFun({SpecPred1, SpecPred2}, NewState0),
			[C ! {state, NS} || {C, NS} <- lists:zip(Children, [NewState1, NewState2])],
			{LogState2, NewState0}
		end,
	    %% Check whether to create a checkpoint 
	    NewCheckPred = CheckPred(MessageMerge, NewState),
	    %% Maybe log some information about the message
	    FinalLogState = LogFun(MsgOrMerge, NewLogState),
	    loop(NewState, Funs, {LogFun, ResetFun, FinalLogState}, 
		 NewCheckPred, Output, ChildrenPredicates, ConfTree);
	{get_message_log, _ReplyTo} = GetLogMsg ->
	    log_mod:debug_log("Ts: ~s -- Node ~p in ~p was asked for throughput.~n" 
			      " -- Its erl_mailbox_size is: ~p~n", 
			      [util:local_timestamp(),self(), node(), 
			       erlang:process_info(self(), message_queue_len)]),
	    NewLogState = handle_get_message_log(GetLogMsg, {LogFun, ResetFun, LogState}),
	    loop(State, Funs, {LogFun, ResetFun, NewLogState}, CheckPred, 
		 Output, ChildrenPredicates, ConfTree)
    end.

-spec handle_message(gen_impl_message() | merge_request(), State::any(), mailbox(), 
		     update_fun(), num_log_triple(), configuration()) 
		    -> {num_log_state(), State::any()}.
handle_message({msg, Msg}, State, Output, UFun, {_, _, LogState}, _Conf) ->
    {LogState, update_on_msg(Msg, State, Output, UFun)};
handle_message({merge, {{_Tag, Father}, _Node, _Ts}}, State, _Output, _UFun, LogTriple, Conf) ->
    respond_to_merge(Father, State, LogTriple, Conf).

-spec update_on_msg(gen_impl_message(), State::any(), mailbox(), update_fun()) -> State::any().
update_on_msg({Msg, _Node, _Ts} = _ImplMsg, State, Output, UFun) ->
    %% The update function is called with the
    %% specification message
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
	{get_message_log, _ReplyTo} = GetLogMsg ->
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
	{get_message_log, _ReplyTo} = GetLogMsg ->
	    log_mod:debug_log("Ts: ~s -- Node ~p in ~p was asked for throughput.~n" 
			      " -- Its erl_mailbox_size is: ~p~n", 
			      [util:local_timestamp(),self(), node(), 
			       erlang:process_info(self(), message_queue_len)]),
	    NewLogState = handle_get_message_log(GetLogMsg, {LogFun, ResetFun, LogState}),
	    receive_state_or_get_message_log(C, {States, {LogFun, ResetFun, NewLogState}})
    end.

-spec receive_states({impl_tag(), integer()}, [mailbox()], num_log_triple()) 
		    -> {num_log_state(), [State::any()]}.
receive_states({_ImplTag, _Ts}, Children, LogTriple) ->
    {States, {_, _, NewLogState}} =
	lists:foldr(fun receive_state_or_get_message_log/2, {[], LogTriple}, Children),
    %% [receive_state(C) || C <- Children].
    {NewLogState, States}.
