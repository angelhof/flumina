-module(optimizer_greedy).

-export([generate_setup_tree/2]).

-include("type_definitions.hrl").

%%
%% This is a very simple greedy optimizer. It sorts tags based on increasing
%% order of rates, and tries to remove tags in this order, until the dependency graph
%% becomes disconnected. When it becomes disconnected, it tries to find a sequence of splits
%% that will lead it to the separate components in the dependency graph.
%%

-type tag_vertices() :: #{tag() := digraph:vertex()}.
-type tag_root_tree() :: {tags(), [tag_root_tree()]}.

-spec generate_setup_tree(specification(), topology()) -> temp_setup_tree().
generate_setup_tree(Specification, Topology) ->
    Dependencies = conf_gen:get_dependencies(Specification),
    
    %% Make the dependency graph
    {DepGraph, TagsVertices} = make_dependency_graph(Dependencies),
    print_graph(DepGraph),

    %% Get the nodes-tags-rates association list
    NodesRates = conf_gen:get_nodes_rates(Topology),
    SortedTags = sort_tags_by_rate_ascending(NodesRates),
    io:format("Sorted Tags: ~p~n", [SortedTags]),
    
    TagsRootTree = iterative_greedy_split(SortedTags, NodesRates, TagsVertices, DepGraph),
    io:format("Tags root tree: ~n~p~n", [TagsRootTree]),
    

    %% Now that we have the root tree we only need to 
    %% find a sequence of splits to reach this root tree

    %% NodesRates = conf_gen:get_nodes_rates(Topology),
    %% MaxRateNode = max_rate_node(NodesRates),
    %% AllTags = conf_gen:get_implementation_tags(Topology),
    %% {InitStateType, InitState} = conf_gen:get_init_state(Specification),
    %% %% Assert that the initial state type can handle all tags
    %% true = can_state_type_handle_tags(InitStateType, AllTags, Specification),
    %% {_, UpdateFun} = conf_gen:get_state_type_tags_upd(InitStateType, Specification),
    
    %% %% Make the setup tree
    %% Funs = {UpdateFun, fun util:crash/2, fun util:crash/2},
    %% AllTagsPredicate = tags_to_predicate(AllTags),
    %% Node = {InitState, MaxRateNode, AllTagsPredicate, Funs, []},
    %% Node. 
    ok.

%%
%% This greedy algorithm, greedily chooses the tags with the lowest rates
%% that disconnect the dependency graph and then removes them, iterating.
%%
%% WARNING: It doesn't care whether a split function exists or not,
%%          but it returns the "optimal" greedy tree.
%% MORE IMPORTANT WARNING: It deletes the original graph
%% 
-spec iterative_greedy_split(tags(), nodes_rates(), tag_vertices(), digraph:graph()) -> tag_root_tree().
iterative_greedy_split(Tags, NodesRates, TagsVertices, Graph) ->
    {TopTags, TagsCCs} = best_greedy_split(Tags, TagsVertices, Graph),
    %% WARNING: Naive sorting of Tags based on rates. A better way would be
    %%          to keep the rates and use them to sort here. Or keep the rates
    %%          for each tag in a map
    SortedTagsCCs =
	lists:map(
	  fun(TagsCC) ->
		  FilteredTags = filter_tags_in_nodes_rates(TagsCC, NodesRates),
		  sort_tags_by_rate_ascending(FilteredTags)
	  end, TagsCCs),
    ChildrenRootTrees = 
	lists:map(
	  fun(SortedTagsCC) ->
		  Vertices = [maps:get(Tag, TagsVertices) || Tag <- SortedTagsCC],
		  Subgraph = digraph_utils:subgraph(Graph, Vertices),
		  iterative_greedy_split(SortedTagsCC, NodesRates, TagsVertices, Subgraph)
	  end, SortedTagsCCs),

    %% WARNING: Delete the graph because the ETS is not garbage collected
    true = digraph:delete(Graph),
    {TopTags, ChildrenRootTrees}.
	

%% This function returns the minimal set of tags that disconnects
%% the dependency graph.
-spec best_greedy_split(tags(), tag_vertices(), digraph:graph()) -> {tags(), [tags()]}.
best_greedy_split(Tags, TagsVertices, Graph) ->
    best_greedy_split(Tags, TagsVertices, Graph, []).

-spec best_greedy_split(tags(), tag_vertices(), digraph:graph(), tags()) -> {tags(), [tags()]}.
best_greedy_split([], TagsVertices, Graph, Acc) ->
    {Acc, []};
best_greedy_split([Tag|Tags], TagsVertices, Graph, Acc) ->
    Vertex = maps:get(Tag, TagsVertices),
    case does_disconnect(Vertex, Graph) of
	{disconnected, Components} ->
	    TagCCs = 
		[[get_label(V, Graph) || V <- Component] 
		 || Component <- Components],
	    {[Tag|Acc], TagCCs};
	still_connected ->
	    best_greedy_split(Tags, TagsVertices, Graph, [Tag|Acc])
    end.

-spec does_disconnect(digraph:vertex(), digraph:graph()) -> 
			     {'disconnected', [[digraph:vertex()]]} |
			     'still_connected'.
does_disconnect(Vertex, Graph) ->
    %% Delete the vertex from the graph
    true = digraph:del_vertex(Graph, Vertex),
    %% Check the number of the connected components
    %% after the removal of Vertex
    case digraph_utils:components(Graph) of
	[] ->
	    still_connected;
	[_Component] ->
	    still_connected;
	Components ->
	    {disconnected, Components}
    end.


-spec filter_tags_in_nodes_rates(tags(), nodes_rates()) -> nodes_rates().
filter_tags_in_nodes_rates(Tags, NodesRates) ->
    TagsSet = sets:from_list(Tags),
    [{Node, Tag, Rate} || {Node, Tag, Rate} <- NodesRates, sets:is_element(Tag, TagsSet)].

-spec make_dependency_graph(dependencies()) -> {digraph:graph(), tag_vertices()}.
make_dependency_graph(Dependencies) ->
    Graph = digraph:new(),
    Tags = maps:keys(Dependencies),
    TagsVertices = add_tags_in_dependency_graph(Tags, Graph),
    ok = add_edges_in_dependency_graph(Dependencies, Graph, TagsVertices),
    {Graph, TagsVertices}.
    
-spec add_tags_in_dependency_graph(tags(), digraph:graph()) -> tag_vertices().
add_tags_in_dependency_graph(Tags, Graph) ->
    TagsVerticesList = 
	lists:map(
	  fun(Tag) ->
		  V = digraph:add_vertex(Graph),
		  V = digraph:add_vertex(Graph, V, Tag),
		  {Tag, V}
	  end, Tags),
    maps:from_list(TagsVerticesList).
    
-spec add_edges_in_dependency_graph(dependencies(), digraph:graph(), tag_vertices()) -> ok.
add_edges_in_dependency_graph(Dependencies, Graph, TagsVerts) ->
    lists:foreach(
      fun({Tag, DTags}) ->
	      lists:foreach(
		fun(DTag) ->
			V1 = maps:get(Tag, TagsVerts),
			V2 = maps:get(DTag, TagsVerts),
		        digraph:add_edge(Graph, V1, V2)
		end, DTags)
      end, maps:to_list(Dependencies)).

-spec sort_tags_by_rate_ascending(nodes_rates()) -> tags().
sort_tags_by_rate_ascending(NodesRates) ->
    SortedTagsRate =
	lists:sort(
	  fun({_Node1, _Tag1, Rate1}, {_Node2, _Tag2, Rate2}) ->
		  Rate1 =< Rate2
	  end, NodesRates),
    [Tag || {_Node, Tag, _Rate} <- SortedTagsRate].

-spec print_graph(digraph:graph()) -> ok.
print_graph(Graph) ->
    Edges = digraph:edges(Graph),
    FullEdges = [digraph:edge(Graph, E) || E <- Edges],
    LabelEdges = [{get_label(V1, Graph), get_label(V2, Graph)} || {_, V1, V2, _} <- FullEdges],
    io:format("Dependency Graph: ~n~p~n", [LabelEdges]),
    ok.
    
-spec get_label(digraph:vertex(), digraph:graph()) -> tag().
get_label(V, G) ->
    {V, Label} = digraph:vertex(G, V),
    Label.
