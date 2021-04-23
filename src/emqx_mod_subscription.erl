%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:05
%%%-------------------------------------------------------------------
-module(emqx_mod_subscription).
-author("root").
-behaviour(emqx_gen_mod).
-include("../include/emqx_mqtt.hrl").
-include("../include/emqx.hrl").

-export([load/1, unload/1, description/0]).
-export([on_client_connected/3]).

load(Topics) ->
  emqx_hooks:add('client.connected', {emqx_mod_subscription, on_client_connected, [Topics]}).

on_client_connected(#{clientid := ClientId, username := Username}, _ConnInfo = #{proto_ver := ProtoVer}, Topics) ->
  Replace = fun (Topic) -> rep(<<"%u">>, Username, rep(<<"%c">>, ClientId, Topic)) end,
  TopicFilters = case ProtoVer of
                   5 ->
                     [{Replace(Topic), SubOpts}
                       || {Topic, SubOpts} <- Topics];
                   _ ->
                     [{Replace(Topic), #{qos => Qos}}
                       || {Topic, #{qos := Qos}} <- Topics]
                 end,
  self() ! {subscribe, TopicFilters}.

unload(_) ->
  emqx_hooks:del('client.connected', {emqx_mod_subscription, on_client_connected}).

description() -> "EMQ X Subscription Module".

rep(<<"%c">>, ClientId, Topic) ->
  emqx_topic:feed_var(<<"%c">>, ClientId, Topic);
rep(<<"%u">>, undefined, Topic) -> Topic;
rep(<<"%u">>, Username, Topic) ->
  emqx_topic:feed_var(<<"%u">>, Username, Topic).