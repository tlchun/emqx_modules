%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:04
%%%-------------------------------------------------------------------
-module(emqx_mod_delayed).
-author("root").
-export([logger_header/0]).

-behaviour(gen_server).
-behaviour(emqx_gen_mod).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-copy_mnesia({mnesia, [copy]}).

-export([load/1, unload/1, description/0]).

-export([start_link/0, on_message_publish/1]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(delayed_message, {key, msg}).

mnesia(boot) ->
  ok = ekka_mnesia:create_table(emqx_mod_delayed,
    [{type, ordered_set},
      {disc_copies, [node()]},
      {local_content, true},
      {record_name, delayed_message},
      {attributes,
        record_info(fields, delayed_message)}]);
mnesia(copy) ->
  ok = ekka_mnesia:copy_table(emqx_mod_delayed,
    disc_copies).

-spec load(list()) -> ok.

load(_Env) ->
  emqx_mod_sup:start_child(emqx_mod_delayed, worker),
  emqx:hook('message.publish',
    {emqx_mod_delayed, on_message_publish, []}).

-spec unload(list()) -> ok.

unload(_Env) ->
  emqx:unhook('message.publish',
    {emqx_mod_delayed, on_message_publish}),
  emqx_mod_sup:stop_child(emqx_mod_delayed).

description() -> "EMQ X Delayed Publish Module".

on_message_publish(Msg = #message{id = Id,
  topic = <<"$delayed/", Topic/binary>>,
  timestamp = Ts}) ->
  [Delay, Topic1] = binary:split(Topic, <<"/">>),
  PubAt = case binary_to_integer(Delay) of
            Interval when Interval < 4294967 ->
              Interval + erlang:round(Ts / 1000);
            Timestamp ->
              case Timestamp - erlang:round(Ts / 1000) > 4294967 of
                true -> error(invalid_delayed_timestamp);
                false -> Timestamp
              end
          end,
  PubMsg = Msg#message{topic = Topic1},
  Headers = PubMsg#message.headers,
  ok = store(#delayed_message{key = {PubAt, Id},
    msg = PubMsg}),
  {stop,
    PubMsg#message{headers =
    Headers#{allow_publish => false}}};
on_message_publish(Msg) -> {ok, Msg}.

-spec start_link() -> emqx_types:startlink_ret().

start_link() ->
  gen_server:start_link({local, emqx_mod_delayed},
    emqx_mod_delayed,
    [],
    []).

-spec store(#delayed_message{}) -> ok.

store(DelayedMsg) ->
  gen_server:call(emqx_mod_delayed,
    {store, DelayedMsg},
    infinity).

init([]) ->
  {ok,
    ensure_publish_timer(#{timer => undefined,
      publish_at => 0})}.

handle_call({store,
  DelayedMsg = #delayed_message{key = Key}},
    _From, State) ->
  ok = mnesia:dirty_write(emqx_mod_delayed, DelayedMsg),
  emqx_metrics:set('messages.delayed', delayed_count()),
  {reply, ok, ensure_publish_timer(Key, State)};
handle_call(Req, _From, State) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "[Delayed] Unexpected call: ~p",
          [Req]}
      end,
        mfa => {emqx_mod_delayed, handle_call, 3}, line => 138})
  end,
  {reply, ignored, State}.

handle_cast(Msg, State) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "[Delayed] Unexpected cast: ~p",
          [Msg]}
      end,
        mfa => {emqx_mod_delayed, handle_cast, 2}, line => 142})
  end,
  {noreply, State}.

handle_info({timeout, TRef, do_publish},
    State = #{timer := TRef}) ->
  DeletedKeys =
    do_publish(mnesia:dirty_first(emqx_mod_delayed),
      os:system_time(seconds)),
  lists:foreach(fun (Key) ->
    mnesia:dirty_delete(emqx_mod_delayed, Key)
                end,
    DeletedKeys),
  emqx_metrics:set('messages.delayed', delayed_count()),
  {noreply,
    ensure_publish_timer(State#{timer := undefined,
      publish_at := 0})};
handle_info(Info, State) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++
          "[Delayed] Unexpected info: ~p",
          [Info]}
      end,
        mfa => {emqx_mod_delayed, handle_info, 2}, line => 153})
  end,
  {noreply, State}.

terminate(_Reason, #{timer := TRef}) ->
  emqx_misc:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

ensure_publish_timer(State) ->
  ensure_publish_timer(mnesia:dirty_first(emqx_mod_delayed),
    State).

ensure_publish_timer('$end_of_table', State) ->
  State#{timer := undefined, publish_at := 0};
ensure_publish_timer({Ts, _Id},
    State = #{timer := undefined}) ->
  ensure_publish_timer(Ts,
    os:system_time(seconds),
    State);
ensure_publish_timer({Ts, _Id},
    State = #{timer := TRef, publish_at := PubAt})
  when Ts < PubAt ->
  ok = emqx_misc:cancel_timer(TRef),
  ensure_publish_timer(Ts,
    os:system_time(seconds),
    State);
ensure_publish_timer(_Key, State) -> State.

ensure_publish_timer(Ts, Now, State) ->
  Interval = max(1, Ts - Now),
  TRef = emqx_misc:start_timer(timer:seconds(Interval),
    do_publish),
  State#{timer := TRef, publish_at := Now + Interval}.

do_publish(Key, Now) -> do_publish(Key, Now, []).

do_publish('$end_of_table', _Now, Acc) -> Acc;
do_publish({Ts, _Id}, Now, Acc) when Ts > Now -> Acc;
do_publish(Key = {Ts, _Id}, Now, Acc) when Ts =< Now ->
  case mnesia:dirty_read(emqx_mod_delayed, Key) of
    [] -> ok;
    [#delayed_message{msg = Msg}] ->
      emqx_pool:async_submit(fun emqx:publish/1, [Msg])
  end,
  do_publish(mnesia:dirty_next(emqx_mod_delayed, Key),
    Now,
    [Key | Acc]).

-spec delayed_count() -> non_neg_integer().

delayed_count() ->
  mnesia:table_info(emqx_mod_delayed, size).

logger_header() -> "".
