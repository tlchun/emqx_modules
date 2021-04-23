%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:10
%%%-------------------------------------------------------------------
-module(emqx_mod_presence).
-author("root").
-export([logger_header/0]).

-behaviour(emqx_gen_mod).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").

-export([load/1, unload/1, description/0]).
-export([on_client_connected/3, on_client_disconnected/4]).

load(Env) ->
  emqx_hooks:put('client.connected', {emqx_mod_presence, on_client_connected, [Env]}),
  emqx_hooks:put('client.disconnected', {emqx_mod_presence, on_client_disconnected, [Env]}).

unload(_Env) ->
  emqx_hooks:del('client.connected', {emqx_mod_presence, on_client_connected}),
  emqx_hooks:del('client.disconnected', {emqx_mod_presence, on_client_disconnected}).

description() -> "EMQ X Presence Module".

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, Env) ->
  Presence = connected_presence(ClientInfo, ConnInfo),
  case emqx_json:safe_encode(Presence) of
    {ok, Payload} ->
      emqx_broker:safe_publish(make_msg(qos(Env), topic(connected, ClientId), Payload));
    {error, _Reason} ->
      begin
        logger:log(error, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Failed to encode 'connected' presence: ~p", [Presence]} end,
            mfa => {emqx_mod_presence, on_client_connected, 3},
            line => 61})
      end
  end.

on_client_disconnected(_ClientInfo = #{clientid := ClientId, username := Username}, Reason, _ConnInfo = #{disconnected_at := DisconnectedAt}, Env) ->
  Presence = #{clientid => ClientId, username => Username, reason => reason(Reason), disconnected_at => DisconnectedAt, ts => erlang:system_time(millisecond)},
  case emqx_json:safe_encode(Presence) of
    {ok, Payload} ->
      emqx_broker:safe_publish(make_msg(qos(Env), topic(disconnected, ClientId), Payload));
    {error, _Reason} ->
      begin
        logger:log(error, #{}, #{report_cb => fun (_) -> {logger_header() ++ "Failed to encode 'disconnected' presence: ~p", [Presence]} end,
            mfa => {emqx_mod_presence, on_client_disconnected, 4}, line => 77})
      end
  end.

connected_presence(#{peerhost := PeerHost, sockport := SockPort, clientid := ClientId, username := Username},
    #{clean_start := CleanStart, proto_name := ProtoName, proto_ver := ProtoVer, keepalive := Keepalive, connected_at := ConnectedAt, expiry_interval := ExpiryInterval}) ->
  #{clientid => ClientId,
    username => Username,
    ipaddress => ntoa(PeerHost),
    sockport => SockPort,
    proto_name => ProtoName,
    proto_ver => ProtoVer,
    keepalive => Keepalive,
    connack => 0,
    clean_start => CleanStart,
    expiry_interval => ExpiryInterval,
    connected_at => ConnectedAt,
    ts => erlang:system_time(millisecond)}.

make_msg(QoS, Topic, Payload) ->
  emqx_message:set_flag(sys,emqx_message:make(emqx_mod_presence, QoS, Topic, iolist_to_binary(Payload))).

topic(connected, ClientId) ->
  emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
  emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/disconnected"])).

qos(Env) -> proplists:get_value(qos, Env, 0).

-compile({inline, [{reason, 1}]}).
reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

-compile({inline, [{ntoa, 1}]}).
ntoa(IpAddr) -> iolist_to_binary(inet:ntoa(IpAddr)).

logger_header() -> "[Presence] ".
