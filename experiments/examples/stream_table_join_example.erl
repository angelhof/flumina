-module(stream_table_join_example).

-export([greedy_big/0,
	 greedy_big_conf/1,
         experiment/1,
         experiment_conf/1,
	 make_page_view_events/4,
	 make_get_user_address_events/4,
         make_update_user_address_events/5,
         specification/1
	]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("flumina/include/type_definitions.hrl").

greedy_big() ->
    Uids = [1, 2],
    Options =
        %% TODO: Add logging tags for latency measurement
        [{experiment_args, Uids},
         {sink_options,
          [{log_tags, update_user_address_tags(Uids)},
           {sink_wait_time, 5000}]},
         {producer_options,
          [{producer_type, steady_sync_timestamp},
           {global_start_sync_wait_ms, 3000}]}],
    util:run_experiment(?MODULE, greedy_big_conf, Options).

-spec greedy_big_conf(experiment_opts()) -> 'finished'.
greedy_big_conf(Options) ->
    %% Get arguments from options
    {sink_name, SinkPid} = lists:keyfind(sink_name, 1, Options),
    {producer_options, ProducerOptions} = lists:keyfind(producer_options, 1, Options),
    {experiment_args, Uids} = lists:keyfind(experiment_args, 1, Options),

    %% Architecture
    Rates =
        lists:flatten([[{node(), {update_user_address, Uid}, 1},
                        {node(), {get_user_address, Uid}, 10},
                        {node(), {page_view, Uid}, 1000}] || Uid <- Uids]),
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    io:format("Tags: ~p~n", [tags(Uids)]),
    io:format("Dependencies: ~p~n", [dependencies(Uids)]),

    ConfTree = conf_gen:generate_for_module(?MODULE, Topology, [{optimizer,optimizer_greedy},
                                                                {specification_arg, Uids}]),

    configuration:pretty_print_configuration(tags(Uids), ConfTree),

    %% Make producers
    make_big_input_seq_producers(Uids, ConfTree, Topology, ProducerOptions),
    SinkPid ! finished.

-type uid_node_list() :: [{uid(), [{atom(), [node()]}]}].

-spec experiment({uid_node_list(), Rate::integer()}) -> 'ok'.
experiment(Args) ->
    {UidNodeList, _Rate} = Args,
    {Uids, _Nodes} = lists:unzip(UidNodeList),

    Options =
        %% TODO: Add logging tags for latency measurement
        [{producer_options,
          [{producer_type, steady_sync_timestamp},
           {global_start_sync_wait_ms, 3000}]},
         {optimizer_type, optimizer_greedy},
         {sink_options,
          [{log_tags, update_user_address_tags(Uids)},
           {sink_wait_time, 15000}]},
         {experiment_args, Args}],
    util:run_experiment(?MODULE, experiment_conf, Options).


-spec experiment_conf(experiment_opts()) -> 'finished'.
experiment_conf(Options) ->
    %% Get arguments from options
    {sink_name, SinkPid} = lists:keyfind(sink_name, 1, Options),
    {producer_options, ProducerOptions} = lists:keyfind(producer_options, 1, Options),
    {optimizer_type, OptimizerType} = lists:keyfind(optimizer_type, 1, Options),
    {experiment_args, {UidNodeList, Rate}} = lists:keyfind(experiment_args, 1, Options),

    %% Keys
    {Uids, _Nodes} = lists:unzip(UidNodeList),

    %% Architecture
    Rates =
        lists:flatten(
          [[{Node, {page_view, Uid}, 1000}
            || Node <- element(2, (lists:keyfind(page_view, 1, TagNodes)))] ++
               [{Node, {get_user_address, Uid}, 10}
                || Node <- element(2, (lists:keyfind(get_user_address, 1, TagNodes)))] ++
               [{Node, {update_user_address, Uid}, 1}
                || Node <- element(2, (lists:keyfind(update_user_address, 1, TagNodes)))]
           || {Uid, TagNodes} <- UidNodeList]),

    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    io:format("Tags: ~p~n", [tags(Uids)]),
    io:format("Dependencies: ~p~n", [dependencies(Uids)]),

    ConfTree = conf_gen:generate_for_module(?MODULE, Topology, [{optimizer,OptimizerType},
                                                                {specification_arg, Uids}]),

    configuration:pretty_print_configuration(tags(Uids), ConfTree),

    %% Make producers
    make_big_input_distr_producers(UidNodeList, ConfTree, Topology, ProducerOptions, Rate),
    SinkPid ! finished.


%%
%% This example is taken from the Apache Samza repository. There are
%% two input streams. A stream containing profile update events
%% (containing information about the user zipcode), and a stream
%% containing page views. The goal of the query is to keep a table of
%% users in its state and join the page view events to enrich them
%% with zipcode.
%%
%% We can separate the events of the profile update stream in the
%% following tags:
%%
%% - new_user
%% - remove_user
%% - update_user_address
%% - get_user_address
%% - update_user_email
%% - ...
%%
%% And the events of the other stream only contain events with tag:
%%
%% - page_view

-type uid() :: integer().
-type uids() :: list(uid()).
-type zipcode() :: integer().

-type update_user_address_tag() :: {'update_user_address', uid()}.
-type get_user_address_tag() :: {'get_user_address', uid()}.
-type page_view_tag() :: {'page_view', uid()}.
-type event_tag() :: update_user_address_tag()
                   | get_user_address_tag()
                   | page_view_tag().

-type update_user_address() :: {update_user_address_tag(), {zipcode(), integer()}}.
-type get_user_address() :: {get_user_address_tag(), integer()}.
-type page_view() :: {page_view_tag(), integer()}.

-type event() :: update_user_address()
               | get_user_address()
               | page_view().

-spec tags(uids()) -> [event_tag()].
tags(Uids) ->
    update_user_address_tags(Uids)
        ++ get_user_address_tags(Uids)
        ++ page_view_tags(Uids).

-spec page_view_tags(uids()) -> [page_view_tag()].
page_view_tags(Uids) ->
    [{page_view, Uid} || Uid <- Uids].

-spec get_user_address_tags(uids()) -> [get_user_address_tag()].
get_user_address_tags(Uids) ->
    [{get_user_address, Uid} || Uid <- Uids].

-spec update_user_address_tags(uids()) -> [update_user_address_tag()].
update_user_address_tags(Uids) ->
    [{update_user_address, Uid} || Uid <- Uids].


%% All of the above events are keyed with the user_id. We will only
%% focus on update_user_address, get_user_address, page_view events.
%%
%% Dependencies:
%%
%% D({page_view, uid1}, {update_user_address, uid2}) if uid1 == uid2
%% D({get_user_address, uid1}, {update_user_address, uid2}) if uid1 == uid2
%% D({update_user_address, uid1}, {update_user_address, uid2}) if uid1 == uid2
%%
-spec dependencies(uids()) -> dependencies().
dependencies(Keys) ->
    PageViewDeps = [{{page_view, Key}, [{update_user_address, Key}]} || Key <- Keys],
    GetAddressDeps = [{{get_user_address, Key}, [{update_user_address, Key}]} || Key <- Keys],
    UpdateAddressDeps = [{{update_user_address, Key},
                          [{update_user_address, Key},
                           {get_user_address, Key},
                           {page_view, Key}]}
                         || Key <- Keys],
    maps:from_list(PageViewDeps ++ GetAddressDeps ++ UpdateAddressDeps).

%% State contains a map from keys to zipcodes.
-type state() :: #{uid() := zipcode()}.

-spec init_state() -> state().
init_state() ->
    #{}.

-spec init_state_pair() -> state_type_pair().
init_state_pair() ->
    {'state0', init_state()}.

-spec state_types_map(uids()) -> state_types_map().
state_types_map(Uids) ->
    AllUidStates =
        #{'state0' => {sets:from_list(tags(Uids)), fun update/3},
          'state_get'  => {sets:from_list(get_user_address_tags(Uids)), fun update_get/3},
          'state_page_view'  => {sets:from_list(page_view_tags(Uids)), fun update_page_view/3}},
    lists:foldl(
      fun(Uid, StateMap) ->
              UidStates =
                  #{state_name("0", Uid) =>
                        {sets:from_list(tags([Uid])), fun update/3},
                    state_name("_get", Uid) =>
                        {sets:from_list(get_user_address_tags([Uid])), fun update_get/3},
                    state_name("_page_view", Uid) =>
                        {sets:from_list(page_view_tags([Uid])), fun update_page_view/3}},
              maps:merge(UidStates, StateMap)
      end, AllUidStates, Uids).

-spec state_name(string(), uid()) -> atom().
state_name(Prefix, Uid) ->
    list_to_atom("state" ++ Prefix ++ "_" ++ integer_to_list(Uid)).

%% TODO: This is slightly wrong and needs to be fixed. It constraints
%% the forks to first happen for keys.
-spec splits_merges(uids()) -> split_merge_funs().
splits_merges(Uids) ->
    UidStateTransitions =
        [[{'state0', 'state0', state_name("0", Uid)},
          {state_name("0", Uid), state_name("_get", Uid), state_name("_page_view", Uid)},
          {state_name("_page_view", Uid), state_name("_page_view", Uid), state_name("_page_view", Uid)}]
         || Uid <- Uids],
    [{Trans, {fun fork/2, fun join/2}} || Trans <- lists:flatten(UidStateTransitions)].

-spec specification(uids()) -> specification().
specification(Uids) ->
    conf_gen:make_specification(state_types_map(Uids), splits_merges(Uids),
                                dependencies(Uids), init_state_pair()).


%% Update functions

-spec update_get(get_user_address(), state(), mailbox()) -> state().
update_get({{get_user_address, Uid}, Ts}, State, SendTo) ->
    update({{get_user_address, Uid}, Ts}, State, SendTo).

-spec update_page_view(page_view(), state(), mailbox()) -> state().
update_page_view({{page_view, Uid}, Ts}, State, SendTo) ->
    update({{page_view, Uid}, Ts}, State, SendTo).

-spec update(event(), state(), mailbox()) -> state().
update({{update_user_address, Uid}, {ZipCode, Ts}}, State, SendTo) ->
    SendTo ! {{update_user_address, Uid}, {ZipCode, Ts}},
    maps:put(Uid, ZipCode, State);
update({{page_view, Uid}, _Ts}, State, _SendTo) ->
    _ZipCode = maps:get(Uid, State, 'no_zipcode'),
    %% SendTo ! {page_view, {Uid, ZipCode, Ts}},
    State;
update({{get_user_address, Uid}, _Ts}, State, _SendTo) ->
    _ZipCode = maps:get(Uid, State, 'no_zipcode'),
    %% SendTo ! {"Zipcode for", Uid, ZipCode, Ts},
    State.


-spec fork({tag_predicate(), tag_predicate()}, state()) -> {state(), state()}.
fork({Pred1, Pred2}, State) ->
    %% The map contains Uids as keys, while the predicates refer to
    %% tags, so we have to give a fake tag to the key.
    State1 =
        maps:filter(fun(K,_) -> is_uid_in_pred(K, Pred1) end, State),
    State2 =
        maps:filter(fun(K,_) -> is_uid_in_pred(K, Pred2) end, State),
    %% io:format("State1: ~p~nState2: ~p~n", [maps:to_list(State1), maps:to_list(State2)]),
    {State1, State2}.

-spec join(state(), state()) -> state().
join(State1, State2) ->
    util:merge_with(
      fun(K, V1, V2) ->
              case V1 =:= V2 of
                  true ->
                      V1;
                  false ->
                      %% This should never be called
                      util:err("Key: ~p shouldn't exist in both maps~n", [K]),
                      erlang:halt()
              end
      end, State1, State2).

%% Since predicates are on tags, we have to check if any of the tags
%% with the same key satisfies the predicate.
-spec is_uid_in_pred(uid(), tag_predicate()) -> boolean().
is_uid_in_pred(Uid, Pred) ->
    UidTags = tags([Uid]),
    lists:any(Pred, UidTags).

%%
%% Input generation
%%

-spec make_big_input_seq_producers(uids(), configuration(), topology(), producer_options()) -> [pid()].
make_big_input_seq_producers(Uids, ConfTree, Topology, ProducerOptions) ->
    {Streams, Lengths} =
        lists:unzip([make_big_input_uid_producers(Uid, [node()], node(), node(), 100) || Uid <- Uids]),
    AllStreams =
        lists:flatten(Streams),
    NumberOfMessages = lists:sum(Lengths),
    util:log_time_and_number_of_messages_before_producers_spawn("stream-table-join-experiment",
                                                                NumberOfMessages),

    LogTags = lists:flatten([[ {update_user_address, Uid}
                             , {get_user_address, Uid}
                             ] || Uid <- Uids]),
    producer:make_producers(AllStreams, ConfTree, Topology, [{log_tags, LogTags}|ProducerOptions]).

-spec make_big_input_distr_producers([{uid(), [{atom(), [node()]}]}], configuration(),
                                     topology(), producer_options(), RateMultiplier::integer()) -> [pid()].
make_big_input_distr_producers(UidTagNodeList, ConfTree, Topology, ProducerOptions, RateMultiplier) ->
    %% Extract the nodes for each uid. Only page view events can be
    %% sent to many different nodes.
    UidNodeList =
        [{Uid,
          element(2, (lists:keyfind(page_view, 1, TagNodes))),
          hd(element(2, (lists:keyfind(get_user_address, 1, TagNodes)))),
          hd(element(2, (lists:keyfind(update_user_address, 1, TagNodes))))}
         || {Uid, TagNodes} <- UidTagNodeList],
    {Streams, Lengths} =
        lists:unzip([make_big_input_uid_producers(Uid, NodesPV, NodeGUA, NodeUUA, RateMultiplier)
                     || {Uid, NodesPV, NodeGUA, NodeUUA} <- UidNodeList]),
    AllStreams =
        lists:flatten(Streams),
    NumberOfMessages = lists:sum(Lengths),
    util:log_time_and_number_of_messages_before_producers_spawn("stream-table-join-experiment",
                                                                NumberOfMessages),
    {Uids, _} = lists:unzip(UidTagNodeList),
    LogTags = lists:flatten([[ {update_user_address, Uid}
                             %% , {get_user_address, Uid}
                             ] || Uid <- Uids]),
    producer:make_producers(AllStreams, ConfTree, Topology, [{log_tags, LogTags}|ProducerOptions]).


-spec make_big_input_uid_producers(uid(), [node()], node(), node(), RateMultiplier::integer())
                                  -> {gen_producer_init(), integer()}.
make_big_input_uid_producers(Uid, NodesPV, NodeGUA, NodeUUA, RateMultiplier) ->
    LengthPV = 500000,
    PageViewStreams = [{{fun ?MODULE:make_page_view_events/4, [Uid, NodePV, LengthPV, 1]},
                        {{page_view, Uid}, NodePV}, RateMultiplier}
                       || NodePV <- NodesPV],
    GUAevents = {fun ?MODULE:make_get_user_address_events/4, [Uid, NodeGUA, LengthPV, 1000]},
    UUAevents = {fun ?MODULE:make_update_user_address_events/5, [Uid, NodeUUA, LengthPV, 10000, 100]},
    UserAddressStreams =
        [{GUAevents, {{get_user_address, Uid}, NodeGUA}, RateMultiplier},
         {UUAevents, {{update_user_address, Uid}, NodeUUA}, RateMultiplier}],
    {PageViewStreams ++ UserAddressStreams,
     LengthPV * length(NodesPV) +
         (LengthPV div 1000) +
         (LengthPV div 10000)}.


-spec make_events_no_heartbeats(page_view_tag() | get_user_address_tag(),
                                node(), integer(), integer()) -> msg_generator().
make_events_no_heartbeats(Tag, Node, Number, Step) ->
    Events =
        [{{Tag, T}, Node, T} || T <- lists:seq(1, Number, Step)]
        ++ [{heartbeat, {{Tag, Node}, Number + 1}}],
    %% io:format("PV:~n~p~n", [Events]),
    producer:list_generator(Events).

-spec make_update_events_heartbeats(update_user_address_tag(), node(), integer(),
                                    integer(), integer()) -> msg_generator().
make_update_events_heartbeats(Tag, Node, LengthFastStream, Ratio, HeartbeatRatio) ->
    LengthStream = LengthFastStream div Ratio,
    Events =
        lists:flatten(
          [[{heartbeat, {{Tag, Node}, (T * Ratio div HeartbeatRatio) + (Ratio * BT)}}
            || T <- lists:seq(1, HeartbeatRatio - 1)]
           ++ [{{Tag, {Ratio + (Ratio * BT), Ratio + (Ratio * BT)}}, Node, Ratio + (Ratio * BT)}]
           || BT <- lists:seq(0,LengthStream - 1)])
	++ [{heartbeat, {{Tag, Node}, LengthFastStream + 1}}],
    %% io:format("UUA:~n~p~n", [Events]),
    producer:list_generator(Events).


-spec make_page_view_events(uid(), node(), integer(), integer()) -> msg_generator().
make_page_view_events(Uid, Node, N, Step) ->
    make_events_no_heartbeats({page_view, Uid}, Node, N, Step).

-spec make_get_user_address_events(uid(), node(), integer(), integer()) -> msg_generator().
make_get_user_address_events(Uid, Node, N, Step) ->
    make_events_no_heartbeats({get_user_address, Uid}, Node, N, Step).

-spec make_update_user_address_events(uid(), node(), integer(),
                                      integer(), integer()) -> msg_generator().
make_update_user_address_events(Uid, Node, LengthFastStream, Ratio, HeartbeatRatio) ->
    make_update_events_heartbeats({update_user_address, Uid}, Node, LengthFastStream, Ratio, HeartbeatRatio).


%% WARNING: The hearbeat ratio needs to be a divisor of RatioAB (Maybe not necessarily)
%% -spec parametrized_input_distr_example(integer(), [node()], integer(), integer())
%% 				      -> {[msg_generator_init()], msg_generator_init(), integer()}.
%% parametrized_input_distr_example(NumberAs, [BNodeName|ANodeNames], RatioAB, HeartbeatBRatio) ->
%%     LengthAStream = 1000000,
%%     %% Return a triple that makes the results
%%     As = [{fun abexample:make_as/4, [Id, ANode, LengthAStream, 1]}
%% 	  || {Id, ANode} <- lists:zip(lists:seq(1, NumberAs), ANodeNames)],

%%     Bs = {fun abexample:make_bs_heartbeats/4, [BNodeName, LengthAStream, RatioAB, HeartbeatBRatio]},

%%     %% Return the streams and the total number of messages
%%     {As, Bs, (LengthAStream * NumberAs) + (LengthAStream div RatioAB)}.
