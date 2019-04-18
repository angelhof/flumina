-module(setup).

-export([
    generate_implementation_tags/1,
    distributed_setup/5
]).

-include("type_definitions.hrl").

% generate_implementation_tags
% Given a set of specification tags and a number of tags for each ID, generate a set of implementation tags.
-spec generate_implementation_tags(node_setup_info()) -> [tag()].
generate_implementation_tags(NodeSetupList) ->
    lists:flatmap(
        fun ({Tag,NumTag,_Rate,_HBRate}) ->
            [{Tag,Id} || Id <- lists:seq(1,NumTag)] end,
        NodeSetupList
    ).

-spec expand_node_setup_info(node_setup_info()) -> expanded_node_setup_info().
expand_node_setup_info(NodeSetupList) ->
    lists:flatmap(
        fun ({Tag,NumTag,Rate,HBRate}) ->
            [{{Tag,Id},Rate,HBRate} || Id <- lists:seq(1,NumTag)] end,
        NodeSetupList
    ).

% -spec generate_node_names(node_setup_info(),atom()) -> [atom()].
generate_node_names(NodeSetupList,NodeNamePrefix) ->
    lists:flatmap(
        fun({Tag,NumTag,_Rate,_HBRate}) ->
            [atom_to_list(Tag) ++ integer_to_list(Id)
                || Id <- lists:seq(1,NumTag)] end,
        NodeSetupList
    ).

% -spec generate_synthetic_input_streams(expanded_node_setup_info(), [string()], non_neg_integer()) -> 
% generate_synthetic_input_streams(ExpandedNodeSetupList,NodeNames,NumTimeUnits) ->
    
% parametrized_input_distr_example(NumberAs, [BNodeName|ANodeNames], RatioAB, HeartbeatBRatio) ->
%     LengthAStream = 1000000,
%     As = [make_as(Id, ANode, LengthAStream, 1) || {Id, ANode} <- lists:zip(lists:seq(1, NumberAs), ANodeNames)],
% 
%     LengthBStream = LengthAStream div RatioAB,
%     %% Bs = [{b, RatioAB + (RatioAB * BT), empty} 
%     %% 	  || BT <- lists:seq(0,LengthBStream)]
%     %% 	++ [{heartbeat, {b,LengthAStream + 1}}],
%     Bs = lists:flatten(
% 	   [[{heartbeat, {{b, BNodeName}, (T * RatioAB div HeartbeatBRatio) + (RatioAB * BT)}} 
% 	    || T <- lists:seq(0, HeartbeatBRatio - 1)] 
% 	   ++ [{{b, RatioAB + (RatioAB * BT)}, BNodeName, RatioAB + (RatioAB * BT)}]
% 	   || BT <- lists:seq(0,LengthBStream)])
% 	++ [{heartbeat, {{b, BNodeName}, LengthAStream + 1}}],
%     {As, Bs}.


% distributed_setup
%   - Specification: specification of the computation
%   - NodeSetupList: A list where each element specifies a number of nodes to set up, with a particular tag, rate, and heartbeat rate.
%   - Optimizer: which optimizer to use
%   - RateMultiplier: optionally, process the input faster or slower than the timestamps specify. (1 should be the default)
%   - RepeatUpdates: optionally, repeat each update a nonnegative integer number of times, to increase the computation cost of updates. (1 should be the default)
-spec distributed_setup(specification(), node_setup_info(), optimizer_type(), float(), non_neg_integer()) -> ok.
distributed_setup(Specification, NodeSetupList, Optimizer, RateMultiplier, RepeatUpdates) ->
    %% Print arguments to IO
    io:format("Setting up edge cluster:~n  Architecture: ~p~n  Other args: ~p~n", [NodeSetupList, [Optimizer, RateMultiplier, RepeatUpdates]]),
    
    %% Nodes and Implementation Tags
    Tags = generate_implementation_tags(NodeSetupList),
    TagsWithRates = expand_node_setup_info(NodeSetupList),
    NodeNames = generate_node_names(NodeSetupList,'TODO'),
    NumNodes = length(TagsWithRates),
    % Sink node (to send output)
    true = register('sink', self()),
    SinkName = {sink, node()},
    
    %% Topology
    Topology = conf_gen:make_topology(
        [{NodeName, Tag, Rate} || 
            {NodeName, {Tag, Rate, _HBRate}} <-
                lists:zip(NodeNames,TagsWithRates)
        ]
    ),

    %% Logging and configuration tree
    LogTriple = log_mod:make_num_log_triple(),    
    ConfTree = conf_gen:generate(Specification, Topology, 
				 [{optimizer,Optimizer}, 
				  {checkpoint, fun conf_gen:always_checkpoint/2},
				  {log_triple, LogTriple}]),
    
    % %% Input Streams
    % {Is, Ds} = parametrized_input_distr_example(NumberAs, RatioAB, HeartbeatBRatio),
    % %% InputStreams = [{A1input, {a,1}, 30}, {A2input, {a,2}, 30}, {BsInput, b, 30}],
    % AInputStreams = [{AIn, ATag, RateMultiplier} || {AIn, ATag} <- lists:zip(As, ATags)],
    % BInputStream = {Bs, b, RateMultiplier},
    % InputStreams = [BInputStream|AInputStreams],
    % 
    % %% Log the input times of b messages
    % _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
    % LoggerInitFun = 
	% fun() ->
	%         log_mod:initialize_message_logger_state("producer", sets:from_list([b]))
	% end,
    % producer:make_producers(InputStreams, ConfTree, Topology, steady_timestamp, LoggerInitFun),

    % SinkPid ! finished,
    ok.
    
    % ExecPid = spawn_link(?MODULE, distributed_experiment_conf, 
	% 		 [SinkName, NodeNames, RateMultiplier, RatioAB, HeartbeatBRatio, Optimizer]),
    % LoggerInitFun =
    %     fun() ->
	%         log_mod:initialize_message_logger_state("sink", sets:from_list([sum]))
    %     end,
    % util:sink(LoggerInitFun).

