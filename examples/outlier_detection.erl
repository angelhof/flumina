-module(outlier_detection).

-export([make_kddcup_generator/0,
         sample_seq/0,
         sample_seq_conf/1]).

-include_lib("eunit/include/eunit.hrl").
-include("type_definitions.hrl").

%% TODO: What should this value be?
-define(PARAM_S, 1).

%%%
%%% Data types
%%%

%% Features

%% TODO: Add all the features here, as well as all the possible
%% values. Extend the parse_function.
-type duration() :: integer().
-type protocol_type() :: 'tcp' | 'udp'.
-type service() :: 'http'.
-type flag() :: 'SF'.
-type src_bytes() :: integer().
-type dst_bytes() :: integer().

%% All categorical features
-type cat_feature() :: protocol_type() | service() | flag().

-type connection_tag() :: 'connection'.
-type connection_payload() :: {duration(), protocol_type(), service(),
                          flag(), src_bytes(), dst_bytes()}.
-type connection() :: {connection_tag(), connection_payload()}.
-type event() :: connection().

%% Dependencies

-spec dependencies() -> dependencies().
dependencies() ->
    #{connection => []}.

%% State

%% The itemset hash value
-record(hval, {sup = 0 :: integer()}).
-type hval() :: #hval{}.

%% Itemset is a tuple of categorical features
-type itemset() :: {[cat_feature()]} | tuple().
-type ihash() :: #{ itemset() := hval()}.

-type state() :: ihash().

%% TODO: Construct itemset for a given set of features.

%% TODO: Extend the map to have all the necessary values to compute
%%       the score.

-spec all_itemsets() -> [itemset()].
all_itemsets() ->
    [{tcp}, {udp}].

-spec init_itemset_hash() -> ihash().
init_itemset_hash() ->
    Itemsets = all_itemsets(),
    ItemsetList = lists:zip(Itemsets, lists:duplicate(length(Itemsets), #hval{})),
    maps:from_list(ItemsetList).

-spec init_state() -> state().
init_state() ->
    init_itemset_hash().

-spec update_sup(connection_payload(), itemset(), ihash()) -> ihash().
update_sup(Payload, G, ItemsetHash) ->
    case util:is_subset(G, Payload) of
        true ->
            maps:update_with(G, fun(HVal = #hval{sup=Sup}) ->
                                        HVal#hval{sup = Sup + 1}
                                end, ItemsetHash);
        false ->
            ItemsetHash
    end.

-spec compute_score(connection_payload(), itemset(), {state(), float()}) -> {state(), float()}.
compute_score(Payload, G, {State, Score}) ->
    ItemsetHash = State, % This hints that state will probably be extended.
    NewItemsetHash = update_sup(Payload, G, ItemsetHash),
    %% TODO: Update the rest of the state (Covariance matrices, etc)
    NewState = NewItemsetHash,

    HVal = maps:get(G, NewItemsetHash),
    SupG = HVal#hval.sup,
    NewScore =
        case SupG < ?PARAM_S of
            true ->
                Score + 1.0 / tuple_size(G);
            false ->
                Score
        end,
    {NewState, NewScore}.


-spec update(event(), state(), pid()) -> state().
update({connection, Payload}, State, _SinkPid) ->
    %% TODO: Enumerate only the relevant itemsets. Fow now we can try
    %%       all, or use the MAXLEVEL optimization as they propose.
    Itemsets = all_itemsets(),
    {NewState, Score} =
        lists:foldl(
         fun(G, Acc) ->
                 compute_score(Payload, G, Acc)
         end, {State, 0}, Itemsets),
    NewState.

-spec update_id(event(), state(), pid()) -> state().
update_id({connection, _Payload} = Msg, State, SinkPid) ->
    SinkPid ! Msg,
    State.

%%
%% Generation
%%

%% Make a generator initializer, used to initialize the computation
-spec make_connection_generator_init() -> producer_init(connection_tag()).
make_connection_generator_init() ->
    [{{fun ?MODULE:make_kddcup_generator/0, []}, {connection, node()}, 10}].

%% Makes a generator for a kddcup data file, that doesn't add heartbeats
-spec make_kddcup_generator() -> msg_generator().
make_kddcup_generator() ->
    Filename = io_lib:format("data/outlier_detection/sample_kddcup_data", []),
    producer:file_generator(Filename, fun parse_kddcup_csv_line/1).

%% TODO: Implement a generator that adds heartbeats


%% Parses a line of the kddcup dataset csv file
-spec parse_kddcup_csv_line(string()) -> impl_message(connection_tag(), connection_payload()).
parse_kddcup_csv_line(Line) ->
    TrimmedLine = string:trim(Line),
    [STimestamp, SDuration, SProtocol, SService, SFlag, SSrcBytes, SDstBytes|_] =
        string:split(TrimmedLine, ",", all),
    Timestamp = list_to_integer(STimestamp),
    Duration = list_to_integer(SDuration),
    Protocol = list_to_atom(SProtocol),
    Service = list_to_atom(SService),
    Flag = list_to_atom(SFlag),
    SrcBytes = list_to_integer(SSrcBytes),
    DstBytes = list_to_integer(SDstBytes),
    %% WARNING: This should return the producer node
    Node = node(),
    %% TODO: Add node name (or some number) in the connection.
    {{connection, {Duration, Protocol, Service, Flag, SrcBytes, DstBytes}}, Node, Timestamp}.



%%
%% Experiments
%%

sample_seq() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sample_seq_conf, [SinkName]),
    util:sink().

sample_seq_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), connection, 1000}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Tags = [connection],
    StateTypesMap =
	#{'state0' => {sets:from_list(Tags), fun update_id/3}},
    SplitsMerges = [],
    Dependencies = dependencies(),
    InitState = {'state0', init_state()},
    Specification =
	conf_gen:make_specification(StateTypesMap, SplitsMerges, Dependencies, InitState),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer,optimizer_sequential}]),

    InputStream = make_connection_generator_init(),
    producer:make_producers(InputStream, ConfTree, Topology),

    SinkPid ! finished.

%%
%% Tests
%%

sample_test_output() ->
    [{connection,{0,tcp,http,'SF',215,45076}},
     {connection,{0,tcp,http,'SF',162,4528}},
     {connection,{0,tcp,http,'SF',236,1228}},
     {connection,{0,tcp,http,'SF',233,2032}},
     {connection,{0,tcp,http,'SF',239,486}},
     {connection,{0,tcp,http,'SF',238,1282}},
     {connection,{0,tcp,http,'SF',235,1337}},
     {connection,{0,tcp,http,'SF',234,1364}},
     {connection,{0,tcp,http,'SF',239,1295}},
     {connection,{0,tcp,http,'SF',181,5450}}].


sample_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, sample_seq_conf}, sample_test_output()))
      end} || _ <- Rounds]}.
