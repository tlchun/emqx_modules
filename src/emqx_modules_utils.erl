%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:09
%%%-------------------------------------------------------------------
-module(emqx_modules_utils).
-author("root").


-export([logger_header/0]).
-include("../include/emqx_modules.hrl").
-include("../include/logger.hrl").

-export([parse_timeout/1,
  password_hash/1,
  parse_host/1,
  parse_ip/1,
  save_upload_file/2,
  get_ssl_opts/2,
  pool_name/2]).

-export([parse_proto_option/2, format_listenon/1]).


parse_ip(Host) ->
  case inet:parse_address(binary_to_list(Host)) of
    {ok, IpAddr} -> IpAddr;
    _ -> binary_to_list(Host)
  end.

parse_host(Server) when is_binary(Server) ->
  case string:split(Server, ":", trailing) of
    [Host, Port] ->
      {parse_ip(Host), binary_to_integer(Port)};
    [Host] -> {parse_ip(Host), 0}
  end.

parse_timeout(B) when is_binary(B) ->
  case cuttlefish_duration:parse(binary_to_list(B)) of
    {error, Reason} -> error(Reason);
    T when is_integer(T) -> T
  end.

password_hash(HashValue) ->
  case string:tokens(HashValue, " ,") of
    [Hash] -> list_to_atom(Hash);
    [Prefix, Suffix] ->
      {list_to_atom(Prefix), list_to_atom(Suffix)};
    [Hash, MacFun, Iterations, Dklen] ->
      {list_to_atom(Hash),
        list_to_atom(MacFun),
        list_to_integer(Iterations),
        list_to_integer(Dklen)};
    _ -> plain
  end.

save_upload_file(#{<<"file">> := <<>>,
  <<"filename">> := <<>>},
    _ModuleId) ->
  "";
save_upload_file(#{<<"file">> := File,
  <<"filename">> := FileName},
    ModuleId) ->
  FullFilename = filename:join([emqx:get_env(data_dir),
    modules,
    ModuleId,
    FileName]),
  ok = filelib:ensure_dir(FullFilename),
  case file:write_file(FullFilename, File) of
    ok -> binary_to_list(FullFilename);
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Store file failed, ModuleId: ~p, ~0p",
              [ModuleId, Reason]}
          end,
            mfa => {emqx_module_utils, save_upload_file, 2},
            line => 59})
      end,
      error({ModuleId, store_file_fail})
  end;
save_upload_file(FilePath, _)
  when is_binary(FilePath) ->
  binary_to_list(FilePath);
save_upload_file(_, _) -> "".

get_ssl_opts(#{<<"cacertfile">> := CAFile,
  <<"certfile">> := CertFile, <<"keyfile">> := KeyFile} =
  Opts,
    ResId) ->
  Filter = fun (Opts0) ->
    [{K, V}
      || {K, V} <- Opts0, V =/= undefined, V =/= <<>>,
      V =/= ""]
           end,
  Key = save_upload_file(KeyFile, ResId),
  Cert = save_upload_file(CertFile, ResId),
  CA = save_upload_file(CAFile, ResId),
  Verify = case maps:get(<<"verify">>, Opts, false) of
             false -> verify_none;
             true -> verify_peer
           end,
  case Filter([{keyfile, Key},
    {certfile, Cert},
    {cacertfile, CA}])
  of
    [] -> [{verify, Verify}];
    SslOpts -> [{verify, Verify} | SslOpts]
  end;
get_ssl_opts(_, _) -> [].

pool_name(ErlModule, ModuleId) ->
  erlang:list_to_atom(atom_to_list(ErlModule) ++
    ":" ++ binary_to_list(ModuleId)).

format_listenon(Port) when is_integer(Port) ->
  io_lib:format("0.0.0.0:~w", [Port]);
format_listenon({Addr, Port}) when is_list(Addr) ->
  io_lib:format("~s:~w", [Addr, Port]);
format_listenon({Addr, Port}) when is_tuple(Addr) ->
  io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).

parse_proto_option(ModId,
    Options = #{<<"listeners">> := Listeners}) ->
  NOptions = [{binary_to_existing_atom(K, utf8), V}
    || {K, V}
      <- maps:to_list(maps:without([<<"listeners">>],
        Options))],
  NListeners = parse_listeners_option(ModId,
    Listeners,
    []),
  [{Proto, ListenOn, Opts ++ NOptions}
    || {Proto, ListenOn, Opts} <- NListeners].

parse_listeners_option(_ModId, [], Acc) ->
  lists:reverse(Acc);
parse_listeners_option(ModId,
    [LisOpts = #{<<"listen_on">> := ListenOn,
      <<"listener_type">> := ProtoType}
      | More],
    Acc) ->
  NListenOn = case string:split(ListenOn, ":", trailing)
              of
                [Port] -> binary_to_integer(Port);
                [IP, Port] ->
                  {ok, IP1} = inet:parse_address(binary_to_list(IP)),
                  {IP1, binary_to_integer(Port)}
              end,
  NProtoType = binary_to_existing_atom(ProtoType, utf8),
  NLisOpts = [{binary_to_existing_atom(K, utf8), V}
    || {K, V}
      <- maps:to_list(maps:without([<<"listen_on">>,
        <<"listener_type">>],
        LisOpts)),
    V =/= <<"">>],
  NLisOpts1 = compact_socket_options(ModId,
    NProtoType,
    NLisOpts),
  parse_listeners_option(ModId,
    More,
    [{NProtoType, NListenOn, NLisOpts1} | Acc]).

compact_socket_options(ModId, ssl, Opts) ->
  Ks = maps:keys(#{versions =>
  #{order => 1, type => string,
    enum =>
    [<<"tlsv1.2,tlsv1.1,tlsv1">>,
      <<"tlsv1.2">>,
      <<"tlsv1.2,tlsv1.1">>,
      <<"tlsv1">>],
    required => true,
    default => <<"tlsv1.2,tlsv1.1,tlsv1">>,
    title =>
    #{en => <<"TLS Version">>,
      zh => <<"TLS 版本"/utf8>>},
    description => #{en => <<"">>, zh => <<"">>}},
    ciphers =>
    #{order => 2, type => string,
      enum => [<<"default">>, <<"psk">>], required => true,
      default => <<"default">>,
      title =>
      #{en => <<"Ciphers">>, zh => <<"加密套件"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}},
    handshake_timeout =>
    #{order => 3, type => string, required => false,
      default => <<"15s">>,
      title =>
      #{en => <<"Handshake Timeout">>,
        zh => <<"握手超时时间"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}},
    verify =>
    #{order => 4, type => string, required => true,
      enum => [<<"verify_none">>, <<"verify_peer">>],
      default => <<"verify_none">>,
      title =>
      #{en => <<"Verify">>, zh => <<"校验类型"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}},
    keyfile =>
    #{order => 5, type => file, required => false,
      default => <<"">>,
      title =>
      #{en => <<"Keyfile">>, zh => <<"密钥文件"/utf8>>},
      description =>
      #{en => <<"The Key file path">>,
        zh => <<"秘钥文件路径"/utf8>>}},
    certfile =>
    #{order => 6, type => file, required => false,
      default => <<"">>,
      title =>
      #{en => <<"Certfile">>, zh => <<"证书文件"/utf8>>},
      description =>
      #{en => <<"The certificate file path">>,
        zh => <<"证书文件路径"/utf8>>}},
    cacertfile =>
    #{order => 7, type => file, required => false,
      default => <<"">>,
      title =>
      #{en => <<"CA Certfile">>,
        zh => <<"CA 证书文件"/utf8>>},
      description =>
      #{en => <<"The CA certificate file path">>,
        zh => <<"CA 证书文件路径"/utf8>>}},
    fail_if_no_peer_cert =>
    #{order => 8, type => boolean, required => false,
      default => false,
      title =>
      #{en => <<"fail_if_no_peer_cert">>,
        zh => <<"关闭无证书客户端连接"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}}}),
  {SslOpts, OtherOpts} = lists:foldl(fun ({K, V},
      {Acc1, Acc2}) ->
    case lists:member(K, Ks) of
      true ->
        {[{K, V} | Acc1], Acc2};
      _ -> {Acc1, [{K, V} | Acc2]}
    end
                                     end,
    {[], []},
    Opts),
  NSslOpts = convert_ssl_dtls_value(ModId, ssl, SslOpts),
  compact_socket_options(ModId,
    tcp,
    [{ssl_options, NSslOpts} | OtherOpts]);
compact_socket_options(_ModId, tcp, Opts) ->
  Ks0 = maps:keys(#{backlog =>
  #{order => 1, type => number, required => false,
    default => 1000,
    title =>
    #{en => <<"Backlog">>,
      zh => <<"TCP 连接队列长度"/utf8>>},
    description =>
    #{en =>
    <<"The TCP backlog defines the maximum "
    "length that the queue of pendingconnections "
    "can grow to">>,
      zh => <<"TCP 连接队列最大长度"/utf8>>}},
    send_timeout =>
    #{order => 2, type => string, required => false,
      default => <<"15s">>,
      title =>
      #{en => <<"Send Timeout">>,
        zh => <<"发送超时时间"/utf8>>},
      description =>
      #{en => <<"The TCP send timeout">>,
        zh => <<"TCP 报文发送超时时间"/utf8>>}},
    send_timeout_close =>
    #{order => 3, type => boolean, required => false,
      default => true,
      title =>
      #{en => <<"Send Timeout Close">>,
        zh => <<"关闭发送超时连接"/utf8>>},
      description =>
      #{en =>
      <<"Close the TCP connection if send timeout">>,
        zh => <<"关闭发送超时的 TCP 连接"/utf8>>}},
    nodelay =>
    #{order => 4, type => boolean, required => false,
      default => true,
      title =>
      #{en => <<"TCP_NODELAY Flag">>,
        zh => <<"TCP_NODELAY 标识"/utf8>>},
      description =>
      #{en =>
      <<"The TCP_NODELAY flag for MQTT connections. "
      "Small amounts of data are sent immediately "
      "if the option is enabled The TCP backlog "
      "defines the maximum length that the "
      "queue of pending">>,
        zh => <<"设置 TCP_NODELAY 标识"/utf8>>}}}),
  Ks = [sndbuf, recbuf, reuseaddr | Ks0],
  {SockOpts, OtherOpts} = lists:foldl(fun ({K, V},
      {Acc1, Acc2}) ->
    case lists:member(K, Ks) of
      true ->
        {[parse_val({K, V})
          | Acc1],
          Acc2};
      _ ->
        {Acc1,
          [parse_val({K, V})
            | Acc2]}
    end
                                      end,
    {[], []},
    Opts),
  [{tcp_options, SockOpts} | OtherOpts];
compact_socket_options(ModId, dtls, Opts) ->
  Ks = maps:keys(#{versions =>
  #{order => 1, type => string,
    enum =>
    [<<"dtlsv1.2,dtlsv1">>,
      <<"dtlsv1.2">>,
      <<"dtlsv1">>],
    required => true, default => <<"dtlsv1.2,dtlsv1">>,
    title =>
    #{en => <<"DTLS Version">>,
      zh => <<"DTLS 协议版本"/utf8>>},
    description => #{en => <<"">>, zh => <<"">>}},
    ciphers =>
    #{order => 2, type => string,
      enum => [<<"default">>, <<"psk">>], required => true,
      default => <<"default">>,
      title =>
      #{en => <<"Ciphers">>, zh => <<"加密套件"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}},
    verify =>
    #{order => 3, type => string, required => true,
      enum => [<<"verify_none">>, <<"verify_peer">>],
      default => <<"verify_none">>,
      title =>
      #{en => <<"Verify">>, zh => <<"校验类型"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}},
    keyfile =>
    #{order => 4, type => file, required => false,
      default => <<"">>,
      title =>
      #{en => <<"Keyfile">>, zh => <<"密钥文件"/utf8>>},
      description =>
      #{en => <<"The Key file path">>,
        zh => <<"秘钥文件路径"/utf8>>}},
    certfile =>
    #{order => 5, type => file, required => false,
      default => <<"">>,
      title =>
      #{en => <<"Certfile">>, zh => <<"证书文件"/utf8>>},
      description =>
      #{en => <<"The certificate file path">>,
        zh => <<"证书路径"/utf8>>}},
    cacertfile =>
    #{order => 6, type => file, required => false,
      default => <<"">>,
      title =>
      #{en => <<"CA Certfile">>,
        zh => <<"CA 证书文件"/utf8>>},
      description =>
      #{en => <<"The CA certificate file path">>,
        zh => <<"CA 证书文件路径"/utf8>>}},
    fail_if_no_peer_cert =>
    #{order => 7, type => boolean, required => false,
      default => false,
      title =>
      #{en => <<"fail_if_no_peer_cert">>,
        zh => <<"关闭无证书客户端连接"/utf8>>},
      description => #{en => <<"">>, zh => <<"">>}}}),
  {DtlsOpts, OtherOpts} = lists:foldl(fun ({K, V},
      {Acc1, Acc2}) ->
    case lists:member(K, Ks) of
      true ->
        {[parse_val({K, V})
          | Acc1],
          Acc2};
      _ ->
        {Acc1,
          [parse_val({K, V})
            | Acc2]}
    end
                                      end,
    {[], []},
    Opts),
  NDtlsOpts = convert_ssl_dtls_value(ModId,
    dtls,
    DtlsOpts),
  compact_socket_options(ModId,
    udp,
    [{dtls_options, NDtlsOpts} | OtherOpts]);
compact_socket_options(_ModId, udp, Opts) ->
  Ks0 = maps:keys(#{}),
  Ks = [sndbuf, recbuf, reuseaddr | Ks0],
  {SockOpts, OtherOpts} = lists:foldl(fun ({K, V},
      {Acc1, Acc2}) ->
    case lists:member(K, Ks) of
      true ->
        {[parse_val({K, V})
          | Acc1],
          Acc2};
      _ ->
        {Acc1,
          [parse_val({K, V})
            | Acc2]}
    end
                                      end,
    {[], []},
    Opts),
  [{udp_options, SockOpts} | OtherOpts].

convert_ssl_dtls_value(ModId, Type, Opts)
  when Type =:= ssl; Type =:= dtls ->
  DefaultTLSVers = case Type of
                     ssl -> <<"tlsv1.2,tlsv1.1,tlsv1">>;
                     dtls -> <<"dtlsv1.2,dtlsv1">>
                   end,
  TLSVers = [binary_to_existing_atom(V, utf8)
    || V
      <- re:split(proplists:get_value(versions,
        Opts,
        DefaultTLSVers),
        "[, ]")],
  CipersType =
    binary_to_existing_atom(proplists:get_value(ciphers,
      Opts,
      <<"default">>),
      utf8),
  Cipers = case CipersType of
             default ->
               lists:usort(lists:append([ssl:cipher_suites(all,
                 V,
                 openssl)
                 || V <- TLSVers]));
             psk ->
               [{psk, aes_128_cbc, sha},
                 {psk, aes_256_cbc, sha},
                 {psk, '3des_ede_cbc', sha},
                 {psk, rc4_128, sha}]
           end,
  UserLookupFunc = case CipersType of
                     default -> undefined;
                     psk -> {fun emqx_psk:lookup/3, <<>>}
                   end,
  [{versions, TLSVers},
    {ciphers, Cipers},
    {user_lookup_fun, UserLookupFunc}
    | convert_commom_tls_options(ModId, Opts, [])].

convert_commom_tls_options(_ModId, [], Acc) ->
  [{K, V} || {K, V} <- lists:reverse(Acc), V =/= ""];
convert_commom_tls_options(ModId, [{Ignore, _} | More],
    Acc)
  when Ignore =:= versions;
  Ignore =:= ciphers;
  Ignore =:= user_lookup_fun ->
  convert_commom_tls_options(ModId, More, Acc);
convert_commom_tls_options(ModId, [{verify, V} | More],
    Acc) ->
  convert_commom_tls_options(ModId,
    More,
    [{verify, binary_to_existing_atom(V, utf8)}
      | Acc]);
convert_commom_tls_options(ModId, [{CertK, V} | More],
    Acc)
  when CertK == certfile;
  CertK == keyfile;
  CertK == cacertfile ->
  Path = save_upload_file(V, ModId),
  convert_commom_tls_options(ModId,
    More,
    [{CertK, Path} | Acc]);
convert_commom_tls_options(ModId, [Oth | More], Acc) ->
  convert_commom_tls_options(ModId, More, [Oth | Acc]).

parse_val({sndbuf, Val}) when is_binary(Val) ->
  {sndbuf, bytesize(Val)};
parse_val({recbuf, Val}) when is_binary(Val) ->
  {recbuf, bytesize(Val)};
parse_val({send_timeout, Val}) when is_binary(Val) ->
  {send_timeout, duration(Val)};
parse_val({proxy_protocol_timeout, Val})
  when is_binary(Val) ->
  {proxy_protocol_timeout, duration(Val)};
parse_val({K, V}) -> {K, V}.

bytesize(Val) when is_binary(Val) ->
  cuttlefish_bytesize:parse(binary_to_list(Val)).

duration(Val) when is_binary(Val) ->
  cuttlefish_duration_parse:parse(Val).

logger_header() -> "".