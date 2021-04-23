%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:10
%%%-------------------------------------------------------------------
-module(emqx_mod_rewrite).
-author("root").

-module(emqx_mod_rewrite).

-behaviour(emqx_gen_mod).
-include("../include/emqx.hrl").
-include("../include/emqx_mqtt.hrl").


-export([rewrite_subscribe/4, rewrite_unsubscribe/4, rewrite_publish/2]).

-export([load/1, unload/1, description/0]).

load(RawRules) ->
  {PubRules, SubRules} = compile(RawRules),
  emqx_hooks:put('client.subscribe', {emqx_mod_rewrite, rewrite_subscribe, [SubRules]}),
  emqx_hooks:put('client.unsubscribe', {emqx_mod_rewrite, rewrite_unsubscribe, [SubRules]}),
  emqx_hooks:put('message.publish', {emqx_mod_rewrite, rewrite_publish, [PubRules]}).

rewrite_subscribe(_ClientInfo, _Properties, TopicFilters, Rules) ->
  {ok, [{match_and_rewrite(Topic, Rules), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_unsubscribe(_ClientInfo, _Properties, TopicFilters, Rules) ->
  {ok, [{match_and_rewrite(Topic, Rules), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_publish(Message = #message{topic = Topic}, Rules) ->
  {ok, Message#message{topic = match_and_rewrite(Topic, Rules)}}.

unload(_) ->
  emqx_hooks:del('client.subscribe', {emqx_mod_rewrite, rewrite_subscribe}),
  emqx_hooks:del('client.unsubscribe', {emqx_mod_rewrite, rewrite_unsubscribe}),
  emqx_hooks:del('message.publish', {emqx_mod_rewrite, rewrite_publish}).

description() -> "EMQ X Topic Rewrite Module".

compile(Rules) ->
  PubRules = [begin
                {ok, MP} = re:compile(Re), {rewrite, Topic, MP, Dest}
              end
    || {rewrite, pub, Topic, Re, Dest} <- Rules],
  SubRules = [begin
                {ok, MP} = re:compile(Re), {rewrite, Topic, MP, Dest}
              end
    || {rewrite, sub, Topic, Re, Dest} <- Rules],
  {PubRules, SubRules}.

match_and_rewrite(Topic, []) -> Topic;
match_and_rewrite(Topic,
    [{rewrite, Filter, MP, Dest} | Rules]) ->
  case emqx_topic:match(Topic, Filter) of
    true -> rewrite(Topic, MP, Dest);
    false -> match_and_rewrite(Topic, Rules)
  end.

rewrite(Topic, MP, Dest) ->
  case re:run(Topic, MP, [{capture, all_but_first, list}])
  of
    {match, Captured} ->
      Vars = lists:zip(["\\$" ++ integer_to_list(I)
        || I <- lists:seq(1, length(Captured))],
        Captured),
      iolist_to_binary(lists:foldl(fun ({Var, Val}, Acc) ->
        re:replace(Acc,
          Var,
          Val,
          [global])
                                   end,
        Dest,
        Vars));
    nomatch -> Topic
  end.
