-module(outlier_detection).

-export([make_kddcup_generator/0,
         sample_id_seq/0,
         sample_id_seq_conf/1,
         sample_seq/0,
         sample_seq_conf/1]).

-include_lib("eunit/include/eunit.hrl").
-include("type_definitions.hrl").

%% TODO: What should this value be?
-define(PARAM_S, 10).
-define(INPUT_FILE, "data/outlier_detection/sample_kddcup_data").
-define(ITEMSETS_FILE, "data/outlier_detection/kddcup_itemsets.csv").

%%%
%%% Data types
%%%

%% Features

%% TODO: Add all the features here, as well as all the possible
%% values. Extend the parse_function.
-type duration() :: integer().
-type protocol_type() :: 'tcp' | 'udp' | 'icmp'.
-type service() :: 'http'.
-type flag() :: 'SF'.
-type src_bytes() :: integer().
-type dst_bytes() :: integer().

%% All categorical features
-type cat_feature() :: protocol_type() | service() | flag().

-type categorical_features() :: {protocol_type(), service(), flag()}.
-type continuous_features() :: {duration(), src_bytes(), dst_bytes()}.

-type connection_tag() :: 'connection'.
-type connection_payload() :: {categorical_features(), continuous_features()}.
-type connection() :: {connection_tag(), connection_payload()}.
-type event() :: connection().

%% Specification

-spec tags() -> [connection_tag()].
tags() ->
    [connection].

-spec state_types_map() -> state_types_map().
state_types_map() ->
    #{'state0' => {sets:from_list(tags()), fun update/3}}.

-spec splits_merges() -> split_merge_funs().
splits_merges() ->
    [].

-spec dependencies() -> dependencies().
dependencies() ->
    #{connection => []}.

-spec init_state_pair() -> state_type_pair().
init_state_pair() ->
    {'state0', init_state()}.


%% Array API

%% TODO: Move this to another file (or at the end)

-type array(X) :: list(X).

-spec new_array(integer(), X) -> array(X).
new_array(N, Default) ->
    [Default || _ <- lists:seq(1, N)].

-spec a_from_list(list(X)) -> array(X).
a_from_list(List) ->
    List.

-spec a_get(integer(), array(X)) -> X.
a_get(I, Array) ->
    lists:nth(I, Array).

-spec a_size(array(_)) -> integer().
a_size(Array) ->
    length(Array).

%% Matrix API

-type matrix() :: array(array(float())).

%% This matrix returns a new 0 filled matrix with size n
new_matrix(N) ->
    new_array(N, new_array(N, 0.0)).

%% TODO: Use a_from_list
-spec m_from_list(list(list(float()))) -> matrix().
m_from_list(ListMatrix) ->
    ListMatrix.

-spec m_get(integer(), integer(), matrix()) -> float().
m_get(I, J, Matrix) ->
    Row = a_get(I, Matrix),
    a_get(J, Row).

-spec m_size(matrix()) -> integer().
m_size(Matrix) ->
    a_size(Matrix).

%% State

-type support() :: integer().
-type l_array() :: array(float()).
-type s_matrix() :: matrix().
-type c_matrix() :: matrix().

-spec new_l_array() -> l_array().
new_l_array() ->
    new_array(3, 0.0).

%% This returnes a new 0 filled matrix for S. Its size should be equal
%% to the number of continuous features.
-spec new_s_matrix() -> s_matrix().
new_s_matrix() ->
    new_matrix(3).

%% The itemset hash value
-record(hval, {sup = 0 :: support(),
               s = new_s_matrix()  :: s_matrix(),
               l = new_l_array()   :: l_array(),
               vs = new_s_matrix() :: s_matrix(),
               vl = new_s_matrix() :: s_matrix()}).
-type hval() :: #hval{}.

%% Itemset is a tuple of categorical features
-type itemset() :: {[cat_feature()]} | tuple().
-type ihash() :: #{ itemset() := hval()}.

-type state() :: ihash().

%% Generates all itemsets.

-spec all_itemsets() -> [itemset()].
all_itemsets() ->
    Items = parse_items(?ITEMSETS_FILE),
    Itemsets = generate_itemsets(Items),
    Itemsets.

-spec parse_items(file:filename()) -> [[cat_feature()]].
parse_items(Filename) ->
    {ok, Data} = file:read_file(Filename),
    StringData = binary:bin_to_list(Data),
    TrimmedStringData = string:trim(StringData, trailing),
    Lines = string:split(TrimmedStringData, "\n", all),
    Items = [[list_to_atom(Item) || Item <- string:split(Line, ",", all)]
             || Line <- Lines],
    Items.

-spec generate_itemsets([[cat_feature()]]) -> [itemset()].
generate_itemsets(Items) ->
    PreparedItems = [[[]] ++ [[Item] || Item <- Feature]
                     || Feature <- Items],
    UnflattenedItemsets = util:cartesian(PreparedItems),
    Itemsets = [list_to_tuple(lists:flatten(UI))
                || UI <- UnflattenedItemsets],
    io:format("Number of Itemsets: ~p~n", [length(Itemsets)]),
    lists:delete({}, Itemsets).


-spec init_itemset_hash() -> ihash().
init_itemset_hash() ->
    Itemsets = all_itemsets(),
    ItemsetList = lists:zip(Itemsets, lists:duplicate(length(Itemsets), #hval{})),
    maps:from_list(ItemsetList).

-spec init_state() -> state().
init_state() ->
    init_itemset_hash().

%% Updates the support for a point in the itemset
-spec update_sup(itemset(), ihash()) -> ihash().
update_sup(G, ItemsetHash) ->
    maps:update_with(G, fun(HVal = #hval{sup=Sup}) ->
                                HVal#hval{sup = Sup + 1}
                        end, ItemsetHash).

%% Updates the S matrix for a point in the dataset
-spec update_s(continuous_features(), itemset(), ihash()) -> ihash().
update_s(Continuous, G, ItemsetHash) ->
    maps:update_with(G, fun(HVal = #hval{s = S}) ->
                                HVal#hval{s = extend_s(Continuous, S)}
                        end, ItemsetHash).

-spec extend_s(continuous_features(), s_matrix()) -> s_matrix().
extend_s(Cont, S) ->
    N = tuple_size(Cont),
    ListMatrix = [[(element(I, Cont) * element(J, Cont)) + m_get(I, J, S)
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).

%% Updates the L array for a point in the dataset
-spec update_l(continuous_features(), itemset(), ihash()) -> ihash().
update_l(Continuous, G, ItemsetHash) ->
    maps:update_with(G, fun(HVal = #hval{l = L}) ->
                                HVal#hval{l = extend_l(Continuous, L)}
                        end, ItemsetHash).

-spec extend_l(continuous_features(), l_array()) -> l_array().
extend_l(Cont, L) ->
    N = tuple_size(Cont),
    ListArray = [element(I, Cont) + a_get(I, L)
                 || I <- lists:seq(1,N)],
    a_from_list(ListArray).

%% Updates the S matrix that is needed for the violation score
-spec update_vs(c_matrix(), itemset(), ihash()) -> ihash().
update_vs(CovP, G, ItemsetHash) ->
    maps:update_with(G, fun(HVal = #hval{vs = VS}) ->
                                HVal#hval{vs = extend_vs(CovP, VS)}
                        end, ItemsetHash).

-spec extend_vs(c_matrix(), s_matrix()) -> s_matrix().
extend_vs(CovP, VS) ->
    N = m_size(VS),
    ListMatrix = [[math:pow(m_get(I,J,CovP),2) + m_get(I, J, VS)
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).

%% Updates the L array needed for the violation score
-spec update_vl(c_matrix(), itemset(), ihash()) -> ihash().
update_vl(CovP, G, ItemsetHash) ->
    maps:update_with(G, fun(HVal = #hval{vl = L}) ->
                                HVal#hval{vl = extend_vl(CovP, L)}
                        end, ItemsetHash).

-spec extend_vl(c_matrix(), s_matrix()) -> s_matrix().
extend_vl(CovP, L) ->
    N = m_size(L),
    ListMatrix = [[m_get(I,J,CovP) + m_get(I, J, L)
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).


-spec update_ihash(connection_payload(), itemset(), ihash()) -> ihash().
update_ihash({Categorical, Continuous}, G, ItemsetHash) ->
    case util:is_subset(G, Categorical) of
        true ->
            %% io:format("~p is subset of: ~p~n", [G, Categorical]),
            ItemsetHash1 = update_sup(G, ItemsetHash),
            ItemsetHash2 = update_s(Continuous, G, ItemsetHash1),
            ItemsetHash3 = update_l(Continuous, G, ItemsetHash2),
            %% Compute the covariance matrix for G the point in the
            %% itemset, as well as for the input event.
            CovG = compute_c(G, ItemsetHash3),
            CovP = compute_c_score(Continuous, G, ItemsetHash3),

            ItemsetHash4 = update_vs(CovP, G, ItemsetHash3),
            ItemsetHash5 = update_vl(CovP, G, ItemsetHash4),
            %% TODO: Update the rest of the state (Covariance matrices, etc)
            %% io:format("New ItemsetHash for: ~p~n~p~n", [G, maps:get(G, ItemsetHash2)]),
            ItemsetHash5;
        false ->
            %% io:format("~p is not a subset of ~p~n", [G, Categorical]),
            ItemsetHash
    end.

%% Computes the covariance matrix for a point in the itemset
-spec compute_c(itemset(), ihash()) -> c_matrix().
compute_c(G, ItemsetHash) ->
    #hval{sup = Sup, s = S, l = L} =
        maps:get(G, ItemsetHash),
    N = a_size(L),
    ListMatrix = [[compute_c_cell(Sup, m_get(I, J, S), a_get(I, L), a_get(J, L))
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).

-spec compute_c_cell(support(), float(), float(), float()) -> float().
compute_c_cell(Sup, Sij, Li, Lj) when Sup > 1 ->
    (Sij / (Sup - 1)) + ((Li * Lj) / (Sup * (Sup - 1)));
compute_c_cell(Sup, Sij, Li, Lj) ->
    %% This only happens on the first item that covers each d, so it
    %% shouldn't matter that much.
    Sij + (Li * Lj).

%% Computes the covariance score between an input and a point in the
%% itemset.
-spec compute_c_score(continuous_features(), itemset(), ihash()) -> c_matrix().
compute_c_score(Continuous, G, ItemsetHash) ->
    #hval{sup = Sup, l = L} =
        maps:get(G, ItemsetHash),
    N = a_size(L),
    ListMatrix = [[compute_c_score_cell(element(I, Continuous), element(J, Continuous),
                                        Sup, a_get(I, L), a_get(J, L))
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).

-spec compute_c_score_cell(number(), number(), support(),
                           float(), float()) -> float().
compute_c_score_cell(Pi, Pj, Sup, Li, Lj) ->
    (Pi - (Li / Sup)) * (Pj - (Lj / Sup)).

-spec compute_score(connection_payload(), itemset(), {state(), float()}) -> {state(), float()}.
compute_score(Payload, G, {State, Score}) ->
    %% io:format("Msg: ~p - g: ~p~nState: ~p - Score: ~p~n", [Payload, G, State, Score]),
    ItemsetHash = State, % This hints that state will probably be extended.
    NewItemsetHash = update_ihash(Payload, G, ItemsetHash),
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
update({connection, Payload}, State, SinkPid) ->
    %% TODO: Do the optimization of itemsets.

    Itemsets = maps:keys(State),
    {NewState, Score} =
        lists:foldl(
         fun(G, Acc) ->
                 compute_score(Payload, G, Acc)
         end, {State, 0}, Itemsets),
    %% TODO: Use the score to flag as local outlier
    SinkPid ! Score,
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
    Filename = io_lib:format(?INPUT_FILE, []),
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

    %% Separate features
    Categorical = {Protocol, Service, Flag},
    Continuous = {Duration, SrcBytes, DstBytes},

    %% WARNING: This should return the producer node
    Node = node(),
    %% TODO: Add node name (or some number) in the connection.
    {{connection, {Categorical, Continuous}}, Node, Timestamp}.



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
    Specification =
	conf_gen:make_specification(state_types_map(), splits_merges(),
                                    dependencies(), init_state_pair()),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer,optimizer_sequential}]),

    InputStream = make_connection_generator_init(),
    producer:make_producers(InputStream, ConfTree, Topology),

    SinkPid ! finished.


sample_id_seq() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, sample_id_seq_conf, [SinkName]),
    util:sink().

sample_id_seq_conf(SinkPid) ->
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

sample_id_test_output() ->
    [{connection,{{tcp,http,'SF'},{0,215,45076}}},
     {connection,{{tcp,http,'SF'},{0,162,4528}}},
     {connection,{{tcp,http,'SF'},{0,236,1228}}},
     {connection,{{tcp,http,'SF'},{0,233,2032}}},
     {connection,{{tcp,http,'SF'},{0,239,486}}},
     {connection,{{tcp,http,'SF'},{0,238,1282}}},
     {connection,{{tcp,http,'SF'},{0,235,1337}}},
     {connection,{{tcp,http,'SF'},{0,234,1364}}},
     {connection,{{tcp,http,'SF'},{0,239,1295}}},
     {connection,{{tcp,http,'SF'},{0,181,5450}}}].


sample_test_() ->
    Rounds = lists:seq(1,100),
    {"Input example test",
     [{setup,
      fun util:nothing/0,
      fun(ok) -> testing:unregister_names() end,
      fun(ok) ->
	      ?_assertEqual(ok, testing:test_mfa({?MODULE, sample_id_seq_conf}, sample_id_test_output()))
      end} || _ <- Rounds]}.
