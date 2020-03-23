-module(node).

-export([init/9,
	 init_worker/7]).

-include("type_definitions.hrl").
-include("config.hrl").

%% Initializes and spawns a node and its mailbox
-spec init(State::any(), node_and_mailbox(), impl_message_predicate(), spec_functions(),
           conf_gen_options_rec(), dependencies(), mailbox(), integer(),
           impl_tags())
          -> {pid(), node_and_mailbox()}.
init(State, {NodeName, MailboxName, Node}, Pred, {UpdateFun, SplitFun, MergeFun},
     #options{log_triple = LogTriple, checkpoint = CheckFun}, Dependencies,
     Output, Depth, ImplTags) ->
    Funs = #wr_funs{upd = UpdateFun, spl = SplitFun, mrg = MergeFun},

    %% Only give the checkpoint function as argument if it is the
    %% root node of the tree (with depth =:= 0).
    NodeCheckpointFun =
	case Depth of
	    0 -> CheckFun;
	    _ -> fun conf_gen:no_checkpoint/2
	end,
    NodePid = spawn_link(Node, ?MODULE, init_worker,
			 [State, Funs, LogTriple, NodeCheckpointFun, Output, NodeName, {master, node()}]),
    _MailboxPid = spawn_link(Node, mailbox, init_mailbox,
                             [MailboxName, Dependencies, Pred, NodePid, {master, node()}, ImplTags]),
    %% Make sure that the mailbox has registered its name
    receive
	{registered, MailboxName} ->
            %% Make sure that the node is also registered
            receive
                {registered, NodeName} ->
                    {NodePid, {NodeName, MailboxName, Node}}
            end
    end.


%%
%% Main Processing Node
%%
-spec init_worker(State::any(), #wr_funs{}, num_log_triple(),
                  checkpoint_predicate(), mailbox(),
                  Name::atom(), mailbox()) -> no_return().
init_worker(State, Funs, LogTriple, CheckPred, Output, Name, Master) ->

    %% Register the node and inform the master
    true = register(Name, self()),
    Master ! {registered, Name},

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
            WorkerState = #wr_st{state = State,
                                 funs = Funs,
                                 log = LogTriple,
                                 cp_pred = CheckPred,
                                 output = Output,
                                 child_preds = ChildrenPredicates,
                                 conf = ConfTree},
	    worker(WorkerState)
    end.


%% This is the main loop that each node executes.
-spec worker(worker_state()) -> no_return().
worker(WorkerState = #wr_st{log={LogFun, ResetFun, LogState}}) ->
    receive
        {MsgOrMerge, _} = MessageMerge when MsgOrMerge =:= msg orelse MsgOrMerge =:= merge ->
	    %% Handle the message, depending on whether his worker has
	    %% children or not.
	    {NewLogState, NewState} = handle_message(MessageMerge, WorkerState),
	    %% Check whether to create a checkpoint 
            CheckPred = WorkerState#wr_st.cp_pred,
	    NewCheckPred = CheckPred(MessageMerge, NewState),
	    %% Maybe log some information about the message
	    FinalLogState = LogFun(MessageMerge, NewLogState),
            NewWorkerState =
                WorkerState#wr_st{state=NewState,
                                  log={LogFun, ResetFun, FinalLogState},
                                  cp_pred=NewCheckPred},
	    worker(NewWorkerState);
	{get_message_log, _ReplyTo} = GetLogMsg ->
	    log_mod:debug_log("Ts: ~s -- Node ~p in ~p was asked for throughput.~n" 
			      " -- Its erl_mailbox_size is: ~p~n", 
			      [util:local_timestamp(),self(), node(), 
			       erlang:process_info(self(), message_queue_len)]),            
	    NewLogState = handle_get_message_log(GetLogMsg, {LogFun, ResetFun, LogState}),
            NewWorkerState =
                WorkerState#wr_st{log={LogFun, ResetFun, NewLogState}},
	    worker(NewWorkerState)
    end.

%% This is the function that handles a message. If the worker node has
%% children, then it receives their states, then acts on the message,
%% and then splits back the state. If it has no children, then it just
%% acts on the message.
-spec handle_message(gen_impl_message() | merge_request(), worker_state())
                    -> {num_log_state(), State::any()}.
handle_message(MessageMerge, WorkerState = #wr_st{log={LogFun, ResetFun, _} = Log}) ->
    ConfTree = WorkerState#wr_st.conf,
    %% The mailbox has cleared this message so we don't need to check for pred
    ConfNode = configuration:find_node(self(), ConfTree),
    case configuration:get_children_mbox_pids(ConfNode) of
        [] ->
            act_on_message(MessageMerge, WorkerState);
        Children ->
            {_IsMsgMerge, {{Tag, _Payload}, Node, Ts}} = MessageMerge,
            ImplTag = {Tag, Node},
            #wr_funs{mrg=MFun, spl=SFun} = WorkerState#wr_st.funs,
            {LogState1, [State1, State2]} =
                receive_states({ImplTag, Ts}, Children, Log),
            MergedState = MFun(State1, State2),
            MergedWorkerState =
                WorkerState#wr_st{state=MergedState,
                                  log={LogFun, ResetFun, LogState1}},
            {LogState2, NewState0} =
                act_on_message(MessageMerge, MergedWorkerState),
            {[SpecPred1, SpecPred2], _} = WorkerState#wr_st.child_preds,
            {NewState1, NewState2} = SFun({SpecPred1, SpecPred2}, NewState0),
            %% Instead of sending the state to the children mailboxes,
            %% we send it straight to the node, because it is a
            %% blocking message.
            ChildrenNameNodes = configuration:get_children_node_names(ConfNode),
            [C ! {state, NS} || {C, NS} <- lists:zip(ChildrenNameNodes, [NewState1, NewState2])],
            {LogState2, NewState0}
    end.

%% This function acts on a message. If it is a simple message, then it
%% just runs the update, otherwise if it is a merge, it responds to
%% the parent.
-spec act_on_message(gen_impl_message() | merge_request(), worker_state())
		    -> {num_log_state(), State::any()}.
act_on_message({msg, Msg}, #wr_st{state=State, output=Output, funs=Funs, log={_, _, LogState}}) ->
    UFun = Funs#wr_funs.upd,
    {LogState, update_on_msg(Msg, State, Output, UFun)};
act_on_message({merge, {{_Tag, Father}, _Node, _Ts}}, #wr_st{state=State, log=Log, conf=Conf}) ->
    respond_to_merge(Father, State, Log, Conf).

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
    Node = configuration:find_node(Self, ConfTree),
    MboxNameNode = configuration:get_mailbox_name_node(Node),
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
    log_mod:debug_log("Ts: ~s -- Node ~p in ~p has ~p unread messages~n",
                      [util:local_timestamp(),self(), node(), process_info(self(), message_queue_len)]),
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
    log_mod:debug_log("Ts: ~s -- Node ~p in ~p has ~p unread messages~n",
                      [util:local_timestamp(),self(), node(), process_info(self(), message_queue_len)]),
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
