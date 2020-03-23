-module(mailbox).

-export([init_mailbox/6]).

-include("type_definitions.hrl").
-include("config.hrl").

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
%% The mailbox works by releasing messages (and merge requests) when
%% all of their previously received dependencies have been
%% released. However a node never receives heartbeats from messages
%% that their siblings or uncle nodes handle, so they have to
%% disregard those dependencies as they cannot be handled by them.
%% Because of that, we have to remove dependencies that a node can not
%% handle (because it doesn't receive those messages and heartbeats),
%% so that progress is ensured.
%%
%% The way we do it, is by only keeping the dependencies that belong
%% to the union (MyPred - ChildrenPreds), (ParentPred - SiblingPred),
%% (GrandParentPred - UnclePred)
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
    io:format("Implementation Tag Dependencies for: ~p~n~p~n", [self(), ImplDependencies]),
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
                    NewMboxState = handle_message({msg, Msg}, MboxState),
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
	    mailbox(MboxState);
        What ->
            %% Mailboxes should never receive anything else. If they
            %% do they should report it and crash immediatelly.
            log_mod:debug_log("Ts: ~s -- ERROR! Mailbox ~p in ~p received unknown message format: ~p~n",
			      [util:local_timestamp(),self(), node(), What]),
            error({unknown_message_format, What})
    end.

%% This function handles a message, by updating the relative buffers
%% and timers, and by clearing buffers.
-spec handle_message(gen_message(), mailbox_state()) -> mailbox_state().
handle_message({msg, Msg}, MboxState) ->
    BuffersTimers = MboxState#mb_st.buffers,
    case is_completely_independent(Msg, BuffersTimers) of
        true ->
            %% If the message is completely independent, just forward it
            Attachee = MboxState#mb_st.attachee,
            Attachee ! {msg, Msg},
            MboxState;
        false ->
            %% Whenever a new message arrives, we add it to its buffer
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
            MboxState#mb_st{buffers = ClearedBuffersTimers}
    end.

%% If this specific message is completely independent, then it
%% shouldn't have any associated buffer or timer, since those would
%% have been cleared in the filter_relevant dependencies function.
-spec is_completely_independent(gen_impl_message(), buffers_timers()) -> boolean().
is_completely_independent({{Tag, _Pld}, Node, _Ts}, {Buffers, _Timers}) ->
    ImplTag = {Tag, Node},
    not maps:is_key(ImplTag, Buffers).

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
    [{SendTo, undef}|Rest] = router:find_responsible_subtree_child_father_pids(ConfTree, Msg),
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
    Pids = router:find_responsible_subtree_pids(ConfTree, {{Tag, heartbeat}, Node, Ts}),
    [To ! {heartbeat, {ImplTag, Ts}} || To <- Pids].
    %% Old implementation of broadcast heartbeat
    %% AllPids = router:heartbeat_route({Tag, Ts, heartbeat}, ConfTree),
    %% [P ! {heartbeat, {Tag, Ts}} || P <- AllPids].
