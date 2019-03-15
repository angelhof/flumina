-module(tracer).

-export([msg_trace/3]).

%% TODO:
%% - Register the nodes with names

%% NOTE: This module only works when we have local execution

-include("type_definitions.hrl").

-type trace_msg() :: {'send', pid(), any(), pid()}.
-type message_map() :: #{{pid(), pid()} := integer()}.
-type vertex_ids() :: #{pid() := digraph:vertex()}.
-type dot_label() :: {'label', string()} | 
		     {'color', string()}.
-type dot_labels() :: [dot_label()].

%% This function starts the system with a specific 
%% MFA and then traces all messages that are sent during its execution.
-spec msg_trace(module(), atom(), [any()]) -> ok. 
msg_trace(Mod, Function, Args) ->
    erlang:trace(processes, true, [send]),
    MatchSpecs = 
	[{['_', {merge, '_'}],[],[]},
	 {['_', {state, '_'}],[],[]},
	 {['_', {msg, '_'}],[],[]},
	 {['_', {heartbeat, '_'}],[],[]},
	 {['_', {configuration, '_'}],[],[]},
	 {['_', {imsg, '_'}],[],[]},
	 {['_', {iheartbeat, '_'}],[],[]}],
    erlang:trace_pattern(send, MatchSpecs, []),
    Pid = spawn_link(Mod, Function, Args),
    msg_trace_loop(Pid, [], undef).

-spec msg_trace_loop(pid(), [trace_msg()], configuration() | 'undef') -> ok.
msg_trace_loop(Pid, Results, ConfTree) ->
    receive
	{trace, _PidPort, send, {configuration, NewConfTree}, _To} ->
	    %% io:format(" ~p -> ( ~p ) -> ~p~n", [PidPort, msg_pp(Msg), To]),
	    msg_trace_loop(Pid, Results, NewConfTree);
	{trace, PidPort, send, Msg, To} ->
	    %% io:format(" ~p -> ( ~p ) -> ~p~n", [PidPort, msg_pp(Msg), To]),
	    msg_trace_loop(Pid, [{send, PidPort, Msg, To}|Results], ConfTree);
	{trace, PidPort, send_to_non_existing_process, Msg, To} ->
	    %% io:format(" ~p -> ( ~p ) -> [NON-EXISTING]~p~n", [PidPort, msg_pp(Msg), To]),
	    msg_trace_loop(Pid, [{send, PidPort, Msg, To}|Results], ConfTree);
	{'EXIT', Pid, normal} ->
	    print_results(Results, ConfTree);
	What ->
	    util:err("What is that?~n~p~n", [What]),
	    msg_trace_loop(Pid, Results, ConfTree)
    end.

%% -spec msg_pp(trace_msg()) -> string().
%% msg_pp(Msg) -> 
%%     "msg".
    
-spec print_results([trace_msg()], configuration()) -> ok.
print_results(RevResults, ConfTree) ->
    Results = lists:reverse(RevResults),
    CleanResults = preprocess_messages(Results, ConfTree),
    EdgeMessages = messages_per_edge(CleanResults),
    %% io:format("~p~n", [EdgeMessages]),
    {ok, DotGraph} = make_message_digraph(EdgeMessages),
    %% io:format("~p~n", [DotGraph]),
    {ok, DotString} = dot:to_string(DotGraph),
    %% io:format("~p~n", [DotString]),
    ok = file:write_file("messages.dot", lists:flatten(DotString)),
    ok.

-spec preprocess_messages([trace_msg()], configuration()) -> [trace_msg()].
preprocess_messages(EdgeMessages, ConfTree) ->
    io:format("~p~n", [ConfTree]),
    %% Merge the mailbox and the node Pids
    PidPairs = configuration:find_node_mailbox_pid_pairs(ConfTree),
    PidMapsTo = 
	lists:foldl(
	  fun({NPid, MboxNameNode}, Acc) ->
		  Acc1 = maps:put(NPid, MboxNameNode, Acc),
		  Acc2 = maps:put(MboxNameNode, MboxNameNode, Acc1),
		  {Name, _Node} = MboxNameNode,
		  maps:put(whereis(Name), MboxNameNode, Acc2)
	  end, #{}, PidPairs),
    MergedPidsMessages = 
	[{send, maps:get(From, PidMapsTo, From), Msg, maps:get(To, PidMapsTo, To)}
	 || {send, From, Msg, To} <- EdgeMessages],

    %% Remove the messages to self
    NoReflMessages = 
	[{send, From, Msg, To} 
	 || {send, From, Msg, To} <- MergedPidsMessages, From =/= To],
    %% TODO: Give better names by making them up by the tree structure 
    NoReflMessages.

-spec messages_per_edge([trace_msg()]) -> message_map().
messages_per_edge(Results) ->
    lists:foldl(
     fun({send, From, {MsgTag, _}, To}, Acc) ->
	     maps:update_with({From, To, MsgTag}, fun incr/1, 1, Acc)
     end, #{}, Results).

-spec make_message_digraph(message_map()) -> dot:dot().
make_message_digraph(EdgesMap) ->
    Graph = digraph:new(),
    VertexIds = 
	maps:fold(
	 fun({K1,K2,_Tag}, _, Acc) ->
		 Acc1 = add_vertex_if_not_exists(K1, Graph, Acc),
		 add_vertex_if_not_exists(K2, Graph, Acc1)
	 end, #{}, EdgesMap),
    %% io:format("Vertices: ~p~n", [digraph:vertices(Graph)]),
    maps:fold(
      fun({From, To, Tag}, Number, G) ->
	      V1 = maps:get(From, VertexIds),
	      V2 = maps:get(To, VertexIds),
	      digraph:add_edge(G, V1, V2, mk_labels(Tag, Number)),
	      G
      end, Graph, EdgesMap),
    dot:export_graph(Graph).

-spec add_vertex_if_not_exists(pid(), digraph:digraph(), vertex_ids()) -> vertex_ids().
add_vertex_if_not_exists(Pid, Graph, VertexIds) ->
    case maps:is_key(Pid, VertexIds) of
	true ->
	    VertexIds;
	false ->
	    V0 = digraph:add_vertex(Graph),
	    Label = lists:flatten(io_lib:format("~w",[Pid])),
	    V = digraph:add_vertex(Graph, V0, [{label, Label}]),
	    maps:put(Pid, V, VertexIds)
    end.

-spec mk_labels(atom(), integer()) -> dot_labels().
mk_labels(Tag, Number) ->
    [{label, atom_to_list(Tag) ++ ":" ++ integer_to_list(Number)},
     {color,
      case Tag of
	  msg ->
	      "black";
	  merge ->
	      "red";
	  heartbeat ->
	      "green";
	  state ->
	      "blue";
	  imsg ->
	      "blueviolet";
	  iheartbeat ->
	      "blueviolet"
      end}].

incr(X) ->
    X + 1.

