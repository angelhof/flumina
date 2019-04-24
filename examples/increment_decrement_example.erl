-module(increment_decrement_example).

% -export([distributed_experiment/11
%     	]).

-include_lib("eunit/include/eunit.hrl").
-include("type_definitions.hrl").

%% =====================================
%% ===== Computation Specification =====
%% =====================================

%% State types, events, and dependencies
-type state() :: {integer(), integer()}. % Total today and max today
-type events() :: message('i', 'none')
                | message('d', 'none')
                | message('#', 'none').
-type event_tags() :: 'i' | 'd' | '#'.

-spec dependencies() -> #{event_tags() := [event_tags()]}.
dependencies() ->
    #{'i' => ['d','#'],
      'd' => ['i','#'],
      '#' => ['i','d','#']
     }.

%% Sequential Specification
-spec init_state() -> state().
init_state() -> {0,0}.

-spec update(events(), state(), pid()) -> state().
update({'#', 'none'}, {_Total, Max}, SinkPid) ->
    SinkPid ! {Max},
    {0,0};
update({Tag, 'none'}, {Total, Max}, _SinkPid) ->
    case Tag of
        'i' ->
            NewTotal = Total + 1;
        'd' ->
            NewTotal = Total - 1
    end,
    NewMax = max(NewTotal,Max),
    {NewTotal,NewMax}.

%% Parallelization Primitives
-spec fork(split_preds(), state()) -> {state(), state()}.
fork(_, {Total, Max}) ->
    {{Total, Max}, {0, 0}}.

-spec join(state(), state()) -> state().
join({X1, Y1}, {X2, _Y2}) -> % Y2 not used
    X = X1 + X2,
    {X, max(Y1,X)}.

%
% %% ============================
% %% ===== Experiment Setup =====
% %% ============================
%
% % -spec distributed_experiment(atom(), non_neg_integer(), non_neg_integer(), float(), float(), float(), float(), float(), float(), optimizer_type(), float(), non_neg_integer()) -> ok.
% distributed_experiment(NumINodes, NumDNodes, SharpRate, IRate, DRate, SharpHBRate, IHBRate, DHBRate, Optimizer, RateMultiplier, UpdateCost) ->
%     StateTypesMap =
% 	   #{'state' => {maps:keys(dependencies()), fun update/3}
%         },
%     SplitsMerges = [{{'state', 'state', 'state'}, {fun fork/2, fun join/2}}],
%     Dependencies = dependencies(),
%     InitState = {'state', init_state()},
%     Specification =
% 	conf_gen:make_specification(
%             StateTypesMap, SplitsMerges, Dependencies, InitState),
%     setup:distributed_setup(
%         Specification,
%         [
%             {'#',1,SharpRate,SharpHBRate},
%             {'i',NumINodes,IRate,IHBRate},
%             {'d',NumDNodes,DRate,DHBRate}
%         ],
%         Optimizer,
%         RateMultiplier,
%         UpdateCost
%     ),
%     ok.
%
%
