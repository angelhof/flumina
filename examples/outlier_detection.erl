-module(outlier_detection).

-export([make_kddcup_generator/2,
         check_outliers_input/5,
         seq/0,
         seq_conf/1,
         distr/0,
         distr_conf/1,
         experiment_sequential/0,
         experiment_greedy/0,
         setup_experiment/6,
         run_experiment/7]).

-include_lib("eunit/include/eunit.hrl").
-include("type_definitions.hrl").

%% These values are copied from the initial paper.
-define(PARAM_S, 10).
-define(PARAM_TAU, 1.96).
-define(PARAM_DELTA, 0.3).
-define(DELTA_SCORE, 10).
-define(SCORE_WINDOW_SIZE, 40).
-define(MAX_LEVEL, 2).


-define(INPUT_FILE_10K, "data/outlier_detection/sample_kddcup_data_10k").
-define(INPUT_FILE_5K0, "data/outlier_detection/sample_kddcup_data_5k_0").
-define(INPUT_FILE_5K1, "data/outlier_detection/sample_kddcup_data_5k_1").
-define(INPUT_FILE_FMT, "data/outlier_detection/sample_kddcup_data_2.5k_~B").
-define(ITEMSETS_FILE, "data/outlier_detection/kddcup_itemsets.csv").

%%%
%%% Data types
%%%

%% Features

%% TODO: Add all the features here, as well as all the possible
%% values. Extend the parse_function.
-type duration() :: integer().
-type protocol_type() :: 'tcp' | 'udp' | 'icmp'.
-type service() :: 'http' | atom().
-type flag() :: 'SF' | atom().
-type src_bytes() :: integer().
-type dst_bytes() :: integer().
-type land() :: '0' | '1'.
-type wrong_fragment() :: integer().
-type urgent() :: integer().
-type hot() :: integer().
-type num_failed_logins() :: integer().
-type logged_in() :: '0' | '1'.
-type num_compromised() :: integer().
-type root_shell() :: integer().
-type su_attempted() :: integer().
-type num_root() :: integer().
-type num_file_creations() :: integer().
-type num_shells() :: integer().
-type num_access_files() :: integer().
-type num_outbound_cmds() :: integer().
-type is_host_login() :: '0' | '1'.
-type is_guest_login() :: '0' | '1'.
-type count() :: integer().
-type srv_count() :: integer().
-type serror_rate() :: float().
-type srv_serror_rate() :: float().
-type rerror_rate() :: float().
-type srv_rerror_rate() :: float().
-type same_srv_rate() :: float().
-type diff_srv_rate() :: float().
-type srv_diff_host_rate() :: float().
-type dst_host_count() :: integer().
-type dst_host_srv_count() :: integer().
-type dst_host_same_srv_rate() :: float().
-type dst_host_diff_srv_rate() :: float().
-type dst_host_same_src_port_rate() :: float().
-type dst_host_srv_diff_host_rate() :: float().
-type dst_host_serror_rate() :: float().
-type dst_host_srv_serror_rate() :: float().
-type dst_host_rerror_rate() :: float().
-type dst_host_srv_rerror_rate() :: float().
-type label() :: atom().

%% All categorical features
-type cat_feature() :: protocol_type() | service() | flag() | land()
                     | logged_in() | is_host_login() | is_guest_login().

-type categorical_features() :: {protocol_type(), service(), flag(), land(), logged_in(),
                                 is_host_login(), is_guest_login()}.
-type continuous_features() :: {duration(), src_bytes(), dst_bytes(), wrong_fragment(),
                                urgent(), hot(), num_failed_logins(), num_compromised(),
                                root_shell(), su_attempted(), num_root(), num_file_creations(),
                                num_shells(), num_access_files(), num_outbound_cmds(),
                                count(), srv_count(), serror_rate(), srv_serror_rate(),
                                rerror_rate(), srv_rerror_rate(), same_srv_rate(), diff_srv_rate(),
                                srv_diff_host_rate(), dst_host_count(), dst_host_srv_count(),
                                dst_host_same_srv_rate(), dst_host_diff_srv_rate(),
                                dst_host_same_src_port_rate(), dst_host_srv_diff_host_rate(),
                                dst_host_serror_rate(), dst_host_srv_serror_rate(), dst_host_rerror_rate(),
                                dst_host_srv_rerror_rate()}.

-define(NUM_CONT_FEATURES, 34).

-type connection_tag() :: 'connection'.
-type connection_features() :: {categorical_features(), continuous_features()}.
-type connection_payload() :: {integer(), connection_features(), label()}.
-type connection() :: {connection_tag(), connection_payload()}.

-type check_event_tag() :: 'check_local_outliers'.
-type check_event() :: {check_event_tag(), integer()}.
-type event() :: connection() | check_event().

%% Specification

-spec tags() -> [connection_tag() | check_event_tag()].
tags() ->
    [connection, check_local_outliers].

-spec state_types_map() -> state_types_map().
state_types_map() ->
    #{'state0' => {sets:from_list(tags()), fun update/3},
      'state'  => {sets:from_list([connection]), fun update_local/3}}.

-spec splits_merges() -> split_merge_funs().
splits_merges() ->
    [{{'state0', 'state', 'state'}, {fun split/2, fun merge/2}},
     {{'state', 'state', 'state'}, {fun split/2, fun merge/2}}].

-spec dependencies() -> dependencies().
dependencies() ->
    #{connection => [check_local_outliers],
      check_local_outliers => [connection, check_local_outliers]}.

-spec init_state_pair() -> state_type_pair().
init_state_pair() ->
    {'state0', init_state()}.


%% Array API

%% TODO: Move this to another file (or at the end)

-type array(X) :: array:array(X).

-spec new_array(integer(), X) -> array(X).
new_array(N, Default) ->
    array:new(N, [{default, Default}]).

-spec a_from_list(list(X)) -> array(X).
a_from_list(List) ->
    Arr = array:from_list(List),
    array:fix(Arr).

-spec a_get(integer(), array(X)) -> X.
a_get(I, Array) ->
    array:get(I-1, Array).

-spec a_size(array(_)) -> integer().
a_size(Array) ->
    array:size(Array).

-spec a_add(array(X), array(X)) -> array(X).
a_add(Array1, Array2) ->
    N = a_size(Array1),
    N = a_size(Array2),
    ListArray = [a_get(I, Array1) + a_get(I, Array2)
                 || I <- lists:seq(1,N)],
    a_from_list(ListArray).

%% Matrix API

-type matrix() :: array(array(float())).

%% This matrix returns a new 0 filled matrix with size n
new_matrix(N) ->
    new_array(N, new_array(N, 0.0)).

%% TODO: Use a_from_list
-spec m_from_list(list(list(float()))) -> matrix().
m_from_list(ListMatrix) ->
    Rows = [array:fix(array:from_list(Row))
            || Row <- ListMatrix],
    array:fix(array:from_list(Rows)).

-spec m_get(integer(), integer(), matrix()) -> float().
m_get(I, J, Matrix) ->
    Row = a_get(I, Matrix),
    a_get(J, Row).

-spec m_size(matrix()) -> integer().
m_size(Matrix) ->
    a_size(Matrix).

-type m_map_fun() :: fun((integer(), integer(), float()) -> float()).

-spec m_map(m_map_fun(), matrix()) -> matrix().
m_map(Fun, Matrix) ->
    N = m_size(Matrix),
    ListMatrix = [[Fun(I, J, m_get(I, J, Matrix))
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).

-spec m_add(matrix(), matrix()) -> matrix().
m_add(Matrix1, Matrix2) ->
    m_map(fun(I, J, Mcell1) ->
                  (Mcell1 + m_get(I, J, Matrix2))
          end, Matrix1).

%% State

-type support() :: integer().
-type l_array() :: array(float()).
-type s_matrix() :: matrix().
-type c_matrix() :: matrix().
-type v_score() :: integer().

-spec new_l_array() -> l_array().
new_l_array() ->
    new_array(?NUM_CONT_FEATURES, 0.0).

%% This returnes a new 0 filled matrix for S. Its size should be equal
%% to the number of continuous features.
-spec new_s_matrix() -> s_matrix().
new_s_matrix() ->
    new_matrix(?NUM_CONT_FEATURES).

%% The itemset hash value
-record(hval, {sup = 0 :: support(),
               s = new_s_matrix()  :: s_matrix(),
               l = new_l_array()   :: l_array(),
               vs = new_s_matrix() :: s_matrix(),
               vl = new_s_matrix() :: s_matrix(),
               vscore = 0 :: v_score()}).
-type hval() :: #hval{}.

%% Itemset is a tuple of categorical features
-type itemset() :: {[cat_feature()]} | tuple().
-type ihash() :: #{ itemset() := hval()}.
-type window_scores() :: [float()].
-type local_outliers() :: [connection_payload()].

-type state() :: {ihash(), window_scores(), local_outliers()}.

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
    MaxLevelItemsets = [I || I <- Itemsets, tuple_size(I) =< ?MAX_LEVEL],
    io:format("Number of Itemsets: ~p~n", [length(MaxLevelItemsets)]),
    lists:delete({}, MaxLevelItemsets).


-spec init_itemset_hash() -> ihash().
init_itemset_hash() ->
    Itemsets = all_itemsets(),
    ItemsetList = lists:zip(Itemsets, lists:duplicate(length(Itemsets), #hval{})),
    maps:from_list(ItemsetList).

-spec init_window_scores() -> window_scores().
init_window_scores() ->
    [].

-spec update_window_scores(float(), window_scores()) -> window_scores().
update_window_scores(Score, WindowScores) when length(WindowScores) == 40 ->
    tl(WindowScores) ++ [Score];
update_window_scores(Score, WindowScores) when length(WindowScores) =< 40 ->
    WindowScores ++ [Score].

-spec get_window_avg_score(window_scores()) -> float().
get_window_avg_score(WindowScores = [_|_])->
    lists:sum(WindowScores) / length(WindowScores);
get_window_avg_score([]) ->
    0.

-spec init_local_outliers() -> local_outliers().
init_local_outliers() ->
    [].

-spec init_state() -> state().
init_state() ->
    {init_itemset_hash(), init_window_scores(), init_local_outliers()}.

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
    m_map(fun(I, J, Mcell) ->
                  (element(I, Cont) * element(J, Cont)) + Mcell
          end, S).

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
    m_map(fun(I, J, Mcell) ->
                  math:pow(m_get(I,J,CovP),2) + Mcell
          end, VS).

%% Updates the L array needed for the violation score
-spec update_vl(c_matrix(), itemset(), ihash()) -> ihash().
update_vl(CovP, G, ItemsetHash) ->
    maps:update_with(G, fun(HVal = #hval{vl = L}) ->
                                HVal#hval{vl = extend_vl(CovP, L)}
                        end, ItemsetHash).

-spec extend_vl(c_matrix(), s_matrix()) -> s_matrix().
extend_vl(CovP, VL) ->
    m_map(fun(I, J, Mcell) ->
                  m_get(I,J,CovP) + Mcell
          end, VL).

%% This function updates the hash and computes the score
-spec update_ihash(connection_features(), itemset(), ihash()) -> {ihash(), float()}.
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

            %% Compute the violation score for P and G
            Sigma = compute_sigma(CovG, G, ItemsetHash5),

            %% Compute the violation score
            ViolationScore = compute_v_score(CovG, CovP, Sigma),

            %% Return the score
            NewScore = compute_new_score(ViolationScore, G, ItemsetHash5),

            %% io:format("New ItemsetHash for: ~p~n~p~n", [G, maps:get(G, ItemsetHash2)]),
            {ItemsetHash5, NewScore};
        false ->
            %% io:format("~p is not a subset of ~p~n", [G, Categorical]),
            {ItemsetHash, 0}
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
compute_c_cell(_Sup, Sij, Li, Lj) ->
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

%% Computes the sigma needed for the violation score for an element and point in itemset
-spec compute_sigma(c_matrix(), itemset(), ihash()) -> c_matrix().
compute_sigma(CovG, G, ItemsetHash) ->
    #hval{sup = Sup, vs = VS, vl = VL} =
        maps:get(G, ItemsetHash),
    N = m_size(VL),
    ListMatrix = [[compute_sigma_cell(Sup, m_get(I, J, CovG),
                                      m_get(I, J, VS), m_get(I, J, VL))
                   || J <- lists:seq(1,N)]
                  || I <- lists:seq(1,N)],
    m_from_list(ListMatrix).

-spec compute_sigma_cell(support(), float(), float(), float()) -> float().
compute_sigma_cell(Sup, Cij, VSij, VLij) when Sup > 1 ->
    math:sqrt((VSij + 2 * Cij * VLij + Sup * math:pow(Cij, 2)) / (Sup - 1));
compute_sigma_cell(Sup, Cij, VSij, VLij) ->
    %% This only happens on the first item that covers each d, so it
    %% shouldn't matter that much.
    math:sqrt(VSij + 2 * Cij * VLij + Sup * math:pow(Cij, 2)).

-spec compute_v_score(c_matrix(), c_matrix(), c_matrix()) -> integer().
compute_v_score(CovG, CovP, Sigma) ->
    N = m_size(CovG),
    List = [compute_v_score0(m_get(I,J,CovG), m_get(I,J,CovP), m_get(I,J,Sigma))
            || I <- lists:seq(1,N), J <- lists:seq(1,N)],
    lists:sum(List).

-spec compute_v_score0(float(), float(), float()) -> integer().
compute_v_score0(_Cij, _CPij, Sigmaij) when Sigmaij == 0 ->
    %% WARNING: I am not sure if that is the correct behaviour when
    %% Sigma is 0
    0;
compute_v_score0(Cij, CPij, Sigmaij) ->
    P = abs((CPij - Cij) / Sigmaij),
    case P =< ?PARAM_TAU of
        true -> 0;
        false -> 1
    end.

-spec compute_new_score(v_score(), itemset(), ihash()) -> float().
compute_new_score(V, G, ItemsetHash) ->
    HVal = maps:get(G, ItemsetHash),
    SupG = HVal#hval.sup,
    case (SupG < ?PARAM_S)
        orelse (V > (?PARAM_DELTA * ?NUM_CONT_FEATURES * ?NUM_CONT_FEATURES)) of
        true ->
            %% io:format("G: ~p~nSup: ~p~nV: ~p~n", [G, SupG, V]),
            1.0 / tuple_size(G);
        false ->
            0
    end.

-spec compute_score_item(connection_features(), itemset(), {ihash(), float()}) -> {ihash(), float()}.
compute_score_item(Features, G, {ItemsetHash, Score}) ->
    %% io:format("Msg: ~p - g: ~p~nState: ~p - Score: ~p~n", [Payload, G, State, Score]),
    {NewItemsetHash, ItemScore} = update_ihash(Features, G, ItemsetHash),

    NewScore = Score + ItemScore,
    {NewItemsetHash, NewScore}.

-spec compute_score_update_ihash(connection_features(), ihash()) -> {ihash(), float()}.
compute_score_update_ihash(Features, ItemsetHash) ->
    Itemsets = maps:keys(ItemsetHash),

    lists:foldl(
      fun(G, Acc) ->
              compute_score_item(Features, G, Acc)
      end, {ItemsetHash, 0.0}, Itemsets).

-spec compute_outlier(connection_features(), state())
                     -> {{'outlier', float()} | 'normal', state()}.
compute_outlier(Features, State) ->
    {ItemsetHash, WindowScores, LocalOutliers} = State,

    {NewItemsetHash, Score} =
        compute_score_update_ihash(Features, ItemsetHash),

    {IsOutlier, NewWindowScores} =
        case Score > ?DELTA_SCORE * get_window_avg_score(WindowScores) of
            true ->
                {{outlier, Score}, WindowScores};
            false ->
                {normal, update_window_scores(Score, WindowScores)}
        end,
    {IsOutlier, {NewItemsetHash, NewWindowScores, LocalOutliers}}.

-spec update(event(), state(), pid()) -> state().
update({connection, {Timestamp, Features, Label}}, State, SinkPid) ->
    FinalState =
        case compute_outlier(Features, State) of
            {{outlier, Score}, NewState} ->
                SinkPid ! {Label, Timestamp, Score},
                NewState;
            {normal, NewState} ->
                NewState
        end,

    case Timestamp rem (100 * 1000) == 0 of
        true ->
            {Timestamp div 1000, "seconds"};
        false ->
            ok
    end,
    FinalState;
update({check_local_outliers, CheckTimestamp}, State, SinkPid) ->
    {ItemsetHash, WindowScores, LocalOutliers} = State,

    SinkPid ! {"init_global_check", CheckTimestamp},

    %% Check if all local outliers are also global
    lists:foreach(
      fun({Timestamp, Features, Label}) ->
              {_NewItemsetHash, Score} =
                  compute_score_update_ihash(Features, ItemsetHash),

              case Score > ?DELTA_SCORE * get_window_avg_score(WindowScores) of
                  true ->
                      SinkPid ! {Label, Timestamp, Score};
                  false ->
                      ok
              end
      end, LocalOutliers),

    %% This is for latency logging purposes
    SinkPid ! {check_local_outliers, CheckTimestamp},

    SinkPid ! {"end_global_check", CheckTimestamp},

    {ItemsetHash, WindowScores, []}.

%%
%% Distributed Specification
%%

-spec split(split_preds(), state()) -> {state(), state()}.
split({_Pred1, _Pred2}, State) ->
    {State, State}.

-spec merge(state(), state()) -> state().
merge(State1, State2) ->
    {ItemsetHash1, WindowScores1, LocalOutliers1} = State1,
    {ItemsetHash2, WindowScores2, LocalOutliers2} = State2,

    MergedItemsetHash =
        merge_itemsethashes(ItemsetHash1, ItemsetHash2),

    %% Keep one of the two window scores arbitrarily
    MergedWindowScores = WindowScores1,
    MergedLocalOutliers = LocalOutliers1 ++ LocalOutliers2,
    {MergedItemsetHash, MergedWindowScores, MergedLocalOutliers}.

-spec merge_itemsets(hval(), hval()) -> hval().
merge_itemsets(HVal1, HVal2) ->
    #hval{sup = Sup1, s = S1, l = L1, vs = VS1, vl = VL1} = HVal1,
    #hval{sup = Sup2, s = S2, l = L2, vs = VS2, vl = VL2} = HVal2,

    #hval{sup = Sup1 + Sup2,
          s = m_add(S1, S2),
          l = a_add(L1, L2),
          vs = m_add(VS1, VS2),
          vl = m_add(VL1, VL2)}.

-spec merge_itemsethashes(ihash(), ihash()) -> ihash().
merge_itemsethashes(IHash1, IHash2) ->
    maps:map(
      fun(Itemset, HVal1) ->
              HVal2 = maps:get(Itemset, IHash2),
              merge_itemsets(HVal1, HVal2)
      end, IHash1).

-spec update_local(connection(), state(), pid()) -> state().
update_local({connection, {Timestamp, Features, Label}}, State, SinkPid) ->
    FinalState =
        case compute_outlier(Features, State) of
            {{outlier, _Score}, {IHash, WScores, LocalOutliers}} ->
                {IHash, WScores, [{Timestamp, Features, Label}|LocalOutliers]};
            {normal, NewState} ->
                NewState
        end,

    case Timestamp rem (100 * 1000) =< 1000 of
        true ->
            SinkPid ! {Timestamp div 1000, "seconds"};
        false ->
            ok
    end,
    FinalState.


%%
%% Generation
%%

%% Make a generator initializer, used to initialize the computation
-spec make_check_outliers_generator_init(node(), integer(), integer(), integer(),
                                         integer(), integer()) -> producer_init(check_event_tag()).
make_check_outliers_generator_init(Node, CheckOutliersPeriod, CheckOutliersHeartbeatPeriod,
                                   BeginTime, EndTime, Rate) ->
    [{{fun ?MODULE:check_outliers_input/5,
       [Node, BeginTime, EndTime, CheckOutliersPeriod, CheckOutliersHeartbeatPeriod]},
      {check_local_outliers, Node}, Rate}].

-spec check_outliers_input(node(), integer(), integer(), integer(), integer()) -> msg_generator().
check_outliers_input(Node, From, To, Step, HeartbeatPeriod) ->
    Input = [{{check_local_outliers, T}, Node, T} || T <- lists:seq(From, To, Step)],
    Msgs = producer:interleave_heartbeats(Input, {{check_local_outliers, Node}, HeartbeatPeriod}, To + 2),
    producer:list_generator(Msgs).

%% Make a generator initializer, used to initialize the computation
-spec make_connection_generator_init(node(), string(), integer()) -> producer_init(connection_tag()).
make_connection_generator_init(Node, Filename, Rate) ->
    [{{fun ?MODULE:make_kddcup_generator/2, [Node, Filename]}, {connection, Node}, Rate}].

%% Makes a generator for a kddcup data file, that doesn't add heartbeats
-spec make_kddcup_generator(node(), string()) -> msg_generator().
make_kddcup_generator(Node, Filename) ->
    producer:file_generator(Filename,
                            fun(Line) ->
                                    parse_kddcup_csv_line(Node, Line)
                            end).

%% TODO: Implement a generator that adds heartbeats

%% Parses a line of the kddcup dataset csv file
-spec parse_kddcup_csv_line(node(), string()) -> impl_message(connection_tag(), connection_payload()).
parse_kddcup_csv_line(Node, Line) ->
    TrimmedLine = string:trim(Line),
    [STimestamp, SDuration, SProtocol, SService, SFlag, SSrcBytes, SDstBytes,
     SLand, SWrongFragment, SUrgent, SHot, SNumFailedLogins, SLoggedIn,
     SNumCompromised, SRootShell, SSuAttempted, SNumRoot, SNumFileCreations,
     SNumShells, SNumAccessFiles, SNumOutboundCmds, SIsHostLogin, SIsGuestLogin,
     SCount, SSrvCount, SSErrorRate, SSrvSErrorRate, SRErrorRate, SSrvRErrorRate,
     SSameSrvRate, SDiffSrvRate, SSrvDiffHostRate, SDstHostCount, SDstHostSrvCount,
     SDstHostSameSrvRate, SDstHostDiffSrvRate, SDstHostSameSrcPortRate,
     SDstHostSrvDiffHostRate, SDstHostSErrorRate, SDstHostSrvSErrorRate,
     SDstHostRErrorRate, SDstHostSrvRErrorRate, SLabel] =
        string:split(TrimmedLine, ",", all),

    Timestamp = list_to_integer(STimestamp),
    Duration = list_to_integer(SDuration),
    Protocol = list_to_atom(SProtocol),
    Service = list_to_atom(SService),
    Flag = list_to_atom(SFlag),
    SrcBytes = list_to_integer(SSrcBytes),
    DstBytes = list_to_integer(SDstBytes),
    Land = list_to_atom(SLand),
    WrongFragment = list_to_integer(SWrongFragment),
    Urgent = list_to_integer(SUrgent),
    Hot = list_to_integer(SHot),
    NumFailedLogins = list_to_integer(SNumFailedLogins),
    LoggedIn = list_to_atom(SLoggedIn),
    NumCompromised = list_to_integer(SNumCompromised),
    RootShell = list_to_integer(SRootShell),
    SuAttempted = list_to_integer(SSuAttempted),
    NumRoot = list_to_integer(SNumRoot),
    NumFileCreations = list_to_integer(SNumFileCreations),
    NumShells = list_to_integer(SNumShells),
    NumAccessFiles = list_to_integer(SNumAccessFiles),
    NumOutboundCmds = list_to_integer(SNumOutboundCmds),
    IsHostLogin = list_to_atom(SIsHostLogin),
    IsGuestLogin = list_to_atom(SIsGuestLogin),
    Count = list_to_integer(SCount),
    SrvCount = list_to_integer(SSrvCount),
    %% The next are floats
    SErrorRate = list_to_float(SSErrorRate),
    SrvSErrorRate = list_to_float(SSrvSErrorRate),
    SrvSErrorRate = list_to_float(SSrvSErrorRate),
    RErrorRate = list_to_float(SRErrorRate),
    SrvRErrorRate = list_to_float(SSrvRErrorRate),
    SameSrvRate = list_to_float(SSameSrvRate),
    DiffSrvRate = list_to_float(SDiffSrvRate),
    SrvDiffHostRate = list_to_float(SSrvDiffHostRate),
    DstHostCount = list_to_integer(SDstHostCount),
    DstHostSrvCount = list_to_integer(SDstHostSrvCount),
    DstHostSameSrvRate = list_to_float(SDstHostSameSrvRate),
    DstHostDiffSrvRate = list_to_float(SDstHostDiffSrvRate),
    DstHostSameSrcPortRate = list_to_float(SDstHostSameSrcPortRate),
    DstHostSrvDiffHostRate = list_to_float(SDstHostSrvDiffHostRate),
    DstHostSErrorRate = list_to_float(SDstHostSErrorRate),
    DstHostSrvSErrorRate = list_to_float(SDstHostSrvSErrorRate),
    DstHostRErrorRate = list_to_float(SDstHostRErrorRate),
    DstHostSrvRErrorRate = list_to_float(SDstHostSrvRErrorRate),

    Label = list_to_atom(string:trim(SLabel, trailing, ".")),

    %% Separate features
    Categorical = {Protocol, Service, Flag, Land, LoggedIn,
                   IsHostLogin, IsGuestLogin},
    Continuous = {Duration, SrcBytes, DstBytes, WrongFragment,
                  Urgent, Hot, NumFailedLogins, NumCompromised,
                  RootShell, SuAttempted, NumRoot, NumFileCreations,
                  NumShells, NumAccessFiles, NumOutboundCmds,
                  Count, SrvCount, SErrorRate, SrvSErrorRate, RErrorRate, SrvRErrorRate,
                  SameSrvRate, DiffSrvRate, SrvDiffHostRate, DstHostCount, DstHostSrvCount,
                  DstHostSameSrvRate, DstHostDiffSrvRate, DstHostSameSrcPortRate,
                  DstHostSrvDiffHostRate, DstHostSErrorRate, DstHostSrvSErrorRate,
                  DstHostRErrorRate, DstHostSrvRErrorRate},

    %% Maybe all continuous have to be floats?
    %% It doesn't make any difference
    %% FloatContinuous = list_to_tuple([float(C) || C <- tuple_to_list(Continuous)]),

    %% TODO: Add node name (or some number) in the connection.
    {{connection, {Timestamp, {Categorical, Continuous}, Label}}, Node, Timestamp}.

%%
%% Experiments
%%

seq() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, seq_conf, [SinkName]),
    util:sink_no_log(30000).

seq_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), connection, 1000},
             {node(), check_local_outliers, 1}],
    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Specification =
	conf_gen:make_specification(state_types_map(), splits_merges(),
                                    dependencies(), init_state_pair()),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer,optimizer_sequential}]),

    InputStream = make_connection_generator_init(node(), ?INPUT_FILE_10K, 1),
    CheckOutliersPeriodMs = 1000 * 1000,
    CheckOutliersHeartbeatPeriodMs = 10 * 1000,
    StartTimeMs = 1 * 1000,
    EndTimeMs = 11000 * 1000,
    CheckInputStream =
        make_check_outliers_generator_init(node(), CheckOutliersPeriodMs,
                                           CheckOutliersHeartbeatPeriodMs,
                                           StartTimeMs,
                                           EndTimeMs, 1),
    producer:make_producers(InputStream ++ CheckInputStream, ConfTree, Topology),

    SinkPid ! finished.

%% TODO: Make a distributed scenario
distr() ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, distr_conf, [SinkName]),
    util:sink_no_log(30000).

distr_conf(SinkPid) ->
    %% Architecture
    Rates = [{node(), connection, 1000},
             {node(), connection, 1000},
             {node(), check_local_outliers, 1}],

    Topology =
	conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Specification =
	conf_gen:make_specification(state_types_map(), splits_merges(),
                                    dependencies(), init_state_pair()),

    ConfTree = conf_gen:generate(Specification, Topology, [{optimizer,optimizer_greedy}]),

    InputStream1 = make_connection_generator_init(node(), ?INPUT_FILE_5K0, 1),
    InputStream2 = make_connection_generator_init(node(), ?INPUT_FILE_5K1, 1),
    CheckOutliersPeriodMs = 1000 * 1000,
    CheckOutliersHeartbeatPeriodMs = 10 * 1000,
    StartTimeMs = 1 * 1000,
    EndTimeMs = 21000 * 1000,
    CheckInputStream =
        make_check_outliers_generator_init(node(), CheckOutliersPeriodMs,
                                           CheckOutliersHeartbeatPeriodMs,
                                           StartTimeMs,
                                           EndTimeMs, 1),
    producer:make_producers(InputStream1 ++ InputStream2 ++ CheckInputStream, ConfTree, Topology),

    SinkPid ! finished.

%% Notes:
%%
%% On my machine it can process a bit less than 10 items per second
%% per worker. Since the timestamps are in round robin, the rate has
%% to be as much as the throughput of all the processes together.
%% 1 process: rate ~7-8
%% 2 process: rate ~15
%% 4 process: rate ~30
%% 8 process: rate ~60
%% 16 process: rate ~120

experiment_sequential() ->
    InputStreams = 4,
    NodeNames = [node() || _ <- lists:seq(0, InputStreams)],
    CheckOutliersPeriodMs = 1000 * 1000,
    CheckOutliersHeartbeatPeriodMs = 10 * 1000,
    Rate = 30,
    setup_experiment(optimizer_sequential, NodeNames, CheckOutliersPeriodMs,
                     CheckOutliersHeartbeatPeriodMs, Rate, log_latency_throughput).

experiment_greedy() ->
    NumHouses = 4,
    NodeNames = [node() || _ <- lists:seq(0, NumHouses)],
    CheckOutliersPeriodMs = 1000 * 1000,
    CheckOutliersHeartbeatPeriodMs = 10 * 1000,
    Rate = 30,
    setup_experiment(optimizer_greedy, NodeNames, CheckOutliersPeriodMs,
                     CheckOutliersHeartbeatPeriodMs, Rate, log_latency_throughput).

setup_experiment(Optimizer, NodeNames, CheckOutliersPeriod,
                 CheckOutliersHeartbeatPeriod, RateMultiplier, DoLog) ->
    true = register('sink', self()),
    SinkName = {sink, node()},
    _ExecPid = spawn_link(?MODULE, run_experiment,
                          [SinkName, Optimizer, NodeNames, CheckOutliersPeriod,
                           CheckOutliersHeartbeatPeriod, RateMultiplier, DoLog]),
    case DoLog of
        log_latency_throughput ->
            LoggerInitFun =
                fun() ->
                        log_mod:initialize_message_logger_state("sink", sets:from_list([check_local_outliers]))
                end,
            util:sink(LoggerInitFun, 30000);
        no_log ->
            util:sink_no_log(30000)
    end.


run_experiment(SinkPid, Optimizer, NodeNames, CheckOutliersPeriod,
               CheckOutliersHeartbeatPeriod, RateMultiplier, DoLog) ->

    io:format("Args:~n~p~n", [[SinkPid, Optimizer, NodeNames, CheckOutliersPeriod,
                               CheckOutliersHeartbeatPeriod, RateMultiplier, DoLog]]),

    %% We assume that the first node name is the main node
    [MainNodeName|OtherNodeNames] = NodeNames,

    %% We assume that there is one node for each input stream and one for main
    NumInputStreams = length(OtherNodeNames),

    %% ConnectionTags = [connection || _ <- lists:seq(0, NumInputStreams - 1)],
    Tags = [check_local_outliers, connection],

    %% Some Garbage
    BeginSimulationTime = 1 * 1000,
    EndSimulationTime   = 21000 * 1000,

    %% Architecture
    OtherNodeRates = [{OtherNodeName, connection, 1000}
                      || OtherNodeName <- OtherNodeNames],
    Rates = [{MainNodeName, check_local_outliers, 1}|OtherNodeRates],
    Topology =
        conf_gen:make_topology(Rates, SinkPid),

    %% Computation
    Specification =
	conf_gen:make_specification(state_types_map(), splits_merges(),
                                    dependencies(), init_state_pair()),

    %% Maybe setup logging
    LogOptions =
        case DoLog of
            log_latency_throughput ->
                LogTriple = log_mod:make_num_log_triple(),
                [{log_triple, LogTriple}];
            no_log ->
                []
        end,
    ConfTree = conf_gen:generate(Specification, Topology,
                                 LogOptions ++ [{optimizer, Optimizer}]),

    %% Prepare the producers input
    StreamIds = [{Id, HouseNodeName}
                 || {Id, HouseNodeName} <- lists:zip(lists:seq(0, NumInputStreams - 1), OtherNodeNames)],
    InputStreams = lists:flatten([make_connection_generator_init(
                                    Node, io_lib:format(?INPUT_FILE_FMT, [Id]), RateMultiplier)
                                  || {Id, Node} <- StreamIds]),
    CheckInputStream =
        make_check_outliers_generator_init(node(), CheckOutliersPeriod,
                                           CheckOutliersHeartbeatPeriod,
                                           BeginSimulationTime, EndSimulationTime,
                                           RateMultiplier),
    ProducerInit = CheckInputStream ++ InputStreams,

    LoggerInitFun =
        case DoLog of
            log_latency_throughput ->
                _ThroughputLoggerPid = spawn_link(log_mod, num_logger_process, ["throughput", ConfTree]),
                fun() ->
                        log_mod:initialize_message_logger_state("producer",
                                                                sets:from_list([check_local_outliers]))
                end;
            no_log ->
                fun log_mod:no_message_logger/0
        end,

    producer:make_producers(ProducerInit, ConfTree, Topology, steady_timestamp,
                            LoggerInitFun, BeginSimulationTime),

    SinkPid ! finished.

