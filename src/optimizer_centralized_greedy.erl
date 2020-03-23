-module(optimizer_centralized_greedy).

-export([generate_setup_tree/2]).

-include("type_definitions.hrl").
-include("config.hrl").

%%
%% This optimizer calls the greedy optimizer with the centralized option.
%%
-spec generate_setup_tree(specification(), topology()) -> temp_setup_tree().
generate_setup_tree(Specification, Topology) ->
    optimizer_greedy:generate_setup_tree(Specification, Topology, centralized).
