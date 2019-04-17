-module(util).

-export([]).

-include("type_definitions.hrl").

% generate_implementation_tags
% Given a set of specification tags and a number of tags for each ID, generate a set of implementation tags.
-spec generate_implementation_tags(tag(),integer()) -> [tag()]
generate_implementation_tags(Tag,NumTag) ->
    [{Tag,Id} || Id <- lists:seq(1,NumTag)].

-spec generate_all_implementation_tags([tag(),integer()]) -> [tag()]
generate_implementation_tags(TagNumTagList) ->
    flatmap(generate_implementation_tags, TagNumTagList).

-spec generate_input([tag(),integer()])


distributed_setup(SharpNodeName, INodeNames, DNodeNames, SharpRate, IRate, BRate, SharpHBRate, IHBRate, BHBRate, RateMultiplier, UpdateCost, Optimizer)



