-module(tracer).

-export([msg_trace/3]).

%% TODO:
%% - Filter out the messages to itself (which are unecessary routing)
%% - Find a way to merge the mailbox with the node
%% - Show the input messages too
%% - Register the nodes with names

%% This function starts the system with a specific 
%% MFA and then traces all messages that are sent during its execution.
msg_trace(Mod, Function, Args) ->
    erlang:trace(processes, true, [send]),
    MatchSpecs = 
	[{['_', {merge, '_'}],[],[]},
	 {['_', {state, '_'}],[],[]},
	 {['_', {msg, '_'}],[],[]},
	 {['_', {heartbeat, '_'}],[],[]}],
    erlang:trace_pattern(send, MatchSpecs, []),
    Pid = spawn_link(Mod, Function, Args),
    msg_trace_loop(Pid, []).

msg_trace_loop(Pid, Results) ->
    receive
	{trace, PidPort, send, Msg, To} ->
	    %% io:format(" ~p -> ( ~p ) -> ~p~n", [PidPort, msg_pp(Msg), To]),
	    msg_trace_loop(Pid, [{send, PidPort, Msg, To}|Results]);
	{trace, PidPort, send_to_non_existing_process, Msg, To} ->
	    %% io:format(" ~p -> ( ~p ) -> [NON-EXISTING]~p~n", [PidPort, msg_pp(Msg), To]),
	    msg_trace_loop(Pid, [{send, PidPort, Msg, To}|Results]);
	{'EXIT', Pid, normal} ->
	    print_results(Results);
	What ->
	    util:err("What is that?~n~p~n", [What]),
	    msg_trace_loop(Pid, Results)
    end.

msg_pp(Msg) -> 
    "msg".
    
print_results(RevResults) ->
    Results = lists:reverse(RevResults),
    EdgeMessages = messages_per_edge(Results),
    %% io:format("~p~n", [EdgeMessages]),
    {ok, DotGraph} = make_message_digraph(EdgeMessages),
    %% io:format("~p~n", [DotGraph]),
    {ok, DotString} = dot:to_string(DotGraph),
    %% io:format("~p~n", [DotString]),
    ok = file:write_file("messages.dot", lists:flatten(DotString)),
    ok.

messages_per_edge(Results) ->
    lists:foldl(
     fun({send, From, {MsgTag, _}, To}, Acc) ->
	     maps:update_with({From, To, MsgTag}, fun incr/1, 1, Acc)
     end, #{}, Results).

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

add_vertex_if_not_exists(Pid, Graph, VertexIds) ->
    case maps:is_key(Pid, VertexIds) of
	true ->
	    VertexIds;
	false ->
	    V0 = digraph:add_vertex(Graph),
	    V = digraph:add_vertex(Graph, V0, [{label, pid_to_list(Pid)}]),
	    maps:put(Pid, V, VertexIds)
    end.

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
	      "blue"
      end}].
incr(X) ->
    X + 1.

