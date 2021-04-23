%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:04
%%%-------------------------------------------------------------------
-module(emqx_mod_acl_internal).
-author("root").

-module(emqx_mod_acl_internal).

-export([logger_header/0]).

-behaviour(emqx_gen_mod).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").


-export([check_acl/5, rules_from_file/1]).

-export([load/1, unload/1, reload/1, description/0]).

-type acl_rules() :: #{publish => [emqx_access_rule:rule()], subscribe => [emqx_access_rule:rule()]}.

load(Env) ->
  Rules = rules_from_file(proplists:get_value(acl_file, Env)),
  emqx_hooks:add('client.check_acl', {emqx_mod_acl_internal, check_acl, [Rules]}, -1).

unload(_Env) -> emqx_hooks:del('client.check_acl', {emqx_mod_acl_internal, check_acl}).

reload(Env) ->
  emqx_acl_cache:is_enabled() andalso
    lists:foreach(fun (Pid) -> erlang:send(Pid, clean_acl_cache) end, emqx_cm:all_channels()),
  unload(Env),
  load(Env).

description() -> "EMQ X Internal ACL Module".

-spec check_acl(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_topic:topic(), emqx_access_rule:acl_result(), acl_rules()) -> {ok, allow} |{ok, deny} |ok.
check_acl(Client, PubSub, Topic, _AclResult, Rules) ->
  case match(Client, Topic, lookup(PubSub, Rules)) of
    {matched, allow} -> {ok, allow};
    {matched, deny} -> {ok, deny};
    nomatch -> ok
  end.

lookup(PubSub, Rules) -> maps:get(PubSub, Rules, []).

match(_Client, _Topic, []) -> nomatch;
match(Client, Topic, [Rule | Rules]) ->
  case emqx_access_rule:match(Client, Topic, Rule) of
    nomatch -> match(Client, Topic, Rules);
    {matched, AllowDeny} -> {matched, AllowDeny}
  end.

-spec rules_from_file(file:filename()) -> map().

rules_from_file(AclFile) ->
  case file:consult(AclFile) of
    {ok, Terms} ->
      Rules = [emqx_access_rule:compile(Term)
        || Term <- Terms],
      #{publish =>
      [Rule || Rule <- Rules, filter(publish, Rule)],
        subscribe =>
        [Rule || Rule <- Rules, filter(subscribe, Rule)]};
    {error, eacces} ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Insufficient permissions to read the "
              "~s file",
              [AclFile]}
          end,
            mfa => {emqx_mod_acl_internal, rules_from_file, 1},
            line => 100})
      end,
      #{};
    {error, enoent} ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "The ~s file does not exist",
              [AclFile]}
          end,
            mfa => {emqx_mod_acl_internal, rules_from_file, 1},
            line => 103})
      end,
      #{};
    {error, Reason} ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to read ~s: ~p",
              [AclFile, Reason]}
          end,
            mfa => {emqx_mod_acl_internal, rules_from_file, 1},
            line => 106})
      end,
      #{}
  end.

filter(_PubSub, {allow, all}) -> true;
filter(_PubSub, {deny, all}) -> true;
filter(publish, {_AllowDeny, _Who, publish, _Topics}) ->
  true;
filter(_PubSub, {_AllowDeny, _Who, pubsub, _Topics}) ->
  true;
filter(subscribe,
    {_AllowDeny, _Who, subscribe, _Topics}) ->
  true;
filter(_PubSub, {_AllowDeny, _Who, _, _Topics}) ->
  false.

logger_header() -> "[ACL_INTERNAL] ".

