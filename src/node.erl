-module(node).

-export([node/9,
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
    _MailboxPid = spawn_link(Node, mailbox, init_mailbox, 
			    [Name, Dependencies, Pred, NodePid, {master, node()}, ImplTags]),
    %% Make sure that the mailbox has registered its name
    receive
	{registered, Name} ->
	    {NodePid, {Name, Node}}
    end.


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
