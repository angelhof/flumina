-module(opt_lib).

-export([tags_to_predicate/1,
	 can_state_type_handle_tags/3]).

-include("type_definitions.hrl").

%%
%% This is a library with code that is common to 
%% several optimizers
%%

%% TODO: This will eventually disappear when predicates 
%%       become sets of tags
-spec tags_to_predicate(tags()) -> message_predicate().
tags_to_predicate(Tags) ->
    fun({MTag, _, _}) ->
	    lists:any(
	      fun(Tag) -> 
		      MTag =:= Tag
	      end,Tags)
    end.

-spec can_state_type_handle_tags(state_type_name(), sets:set(tag()), specification()) -> boolean().
can_state_type_handle_tags(StateType, Tags, Specification) ->
    {HandledTagsSet, _UpdFun} = conf_gen:get_state_type_tags_upd(StateType, Specification),
    sets:is_subset(Tags, HandledTagsSet).
