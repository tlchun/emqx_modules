%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:05
%%%-------------------------------------------------------------------
-module(emqx_mod_topic_metrics).
-author("root").

-export([logger_header/0]).

-behaviour(gen_server).
-behaviour(emqx_gen_mod).
-include("../include/emqx.hrl").
-include("../include/logger.hrl").
-include("../include/emqx_mqtt.hrl").



-export([load/1, unload/1, description/0]).

-export([on_message_publish/1,
  on_message_delivered/2,
  on_message_dropped/3]).

-export([start_link/0, stop/0]).

-export([inc/2,
  inc/3,
  val/2,
  rate/2,
  metrics/1,
  register/1,
  unregister/1,
  unregister_all/0,
  is_registered/1,
  all_registered_topics/0]).

-export([rates/2]).

-export([init/1,
  handle_call/3,
  handle_info/2,
  handle_cast/2,
  terminate/2]).

-record(speed,
{last = 0 :: number(),
  last_v = 0 :: number(),
  last_medium = 0 :: number(),
  last_long = 0 :: number()}).

-record(state,
{speeds :: #{{binary(), atom()} => #speed{}}}).

load(_Env) ->
  emqx_mod_sup:start_child(emqx_mod_topic_metrics,
    worker),
  emqx_hooks:put('message.publish',
    {emqx_mod_topic_metrics, on_message_publish, []}),
  emqx_hooks:put('message.dropped',
    {emqx_mod_topic_metrics, on_message_dropped, []}),
  emqx_hooks:put('message.delivered',
    {emqx_mod_topic_metrics, on_message_delivered, []}).

unload(_Env) ->
  emqx_hooks:del('message.publish',
    {emqx_mod_topic_metrics, on_message_publish}),
  emqx_hooks:del('message.dropped',
    {emqx_mod_topic_metrics, on_message_dropped}),
  emqx_hooks:del('message.delivered',
    {emqx_mod_topic_metrics, on_message_delivered}),
  emqx_mod_sup:stop_child(emqx_mod_topic_metrics).

description() -> "EMQ X Topic Metrics Module".

on_message_publish(#message{topic = Topic,
  qos = QoS}) ->
  case is_registered(Topic) of
    true ->
      try_inc(Topic, 'messages.in'),
      case QoS of
        0 -> inc(Topic, 'messages.qos0.in');
        1 -> inc(Topic, 'messages.qos1.in');
        2 -> inc(Topic, 'messages.qos2.in')
      end;
    false -> ok
  end.

on_message_delivered(_,
    #message{topic = Topic, qos = QoS}) ->
  case is_registered(Topic) of
    true ->
      try_inc(Topic, 'messages.out'),
      case QoS of
        0 -> inc(Topic, 'messages.qos0.out');
        1 -> inc(Topic, 'messages.qos1.out');
        2 -> inc(Topic, 'messages.qos2.out')
      end;
    false -> ok
  end.

on_message_dropped(#message{topic = Topic}, _, _) ->
  case is_registered(Topic) of
    true -> inc(Topic, 'messages.dropped');
    false -> ok
  end.

start_link() ->
  gen_server:start_link({local, emqx_mod_topic_metrics},
    emqx_mod_topic_metrics,
    [],
    []).

stop() -> gen_server:stop(emqx_mod_topic_metrics).

try_inc(Topic, Metric) ->
  _ = inc(Topic, Metric),
  ok.

inc(Topic, Metric) -> inc(Topic, Metric, 1).

inc(Topic, Metric, Val) ->
  case get_counters(Topic) of
    {error, topic_not_found} -> {error, topic_not_found};
    CRef ->
      case metric_idx(Metric) of
        {error, invalid_metric} -> {error, invalid_metric};
        Idx -> counters:add(CRef, Idx, Val)
      end
  end.

val(Topic, Metric) ->
  case ets:lookup(emqx_mod_topic_metrics, Topic) of
    [] -> {error, topic_not_found};
    [{Topic, CRef}] ->
      case metric_idx(Metric) of
        {error, invalid_metric} -> {error, invalid_metric};
        Idx -> counters:get(CRef, Idx)
      end
  end.

rate(Topic, Metric) ->
  case rates(Topic, Metric) of
    #{short := Last} -> Last;
    {error, Reason} -> {error, Reason}
  end.

rates(Topic, Metric) ->
  gen_server:call(emqx_mod_topic_metrics,
    {get_rates, Topic, Metric}).

metrics(Topic) ->
  case ets:lookup(emqx_mod_topic_metrics, Topic) of
    [] -> {error, topic_not_found};
    [{Topic, CRef}] ->
      lists:foldl(fun (Metric, Acc) ->
        [{to_count(Metric),
          counters:get(CRef, metric_idx(Metric))},
          {to_rate(Metric), rate(Topic, Metric)}
          | Acc]
                  end,
        [],
        ['messages.in',
          'messages.out',
          'messages.qos0.in',
          'messages.qos0.out',
          'messages.qos1.in',
          'messages.qos1.out',
          'messages.qos2.in',
          'messages.qos2.out',
          'messages.dropped'])
  end.

register(Topic) when is_binary(Topic) ->
  gen_server:call(emqx_mod_topic_metrics,
    {register, Topic}).

unregister(Topic) when is_binary(Topic) ->
  gen_server:call(emqx_mod_topic_metrics,
    {unregister, Topic}).

unregister_all() ->
  gen_server:call(emqx_mod_topic_metrics,
    {unregister, all}).

is_registered(Topic) ->
  ets:member(emqx_mod_topic_metrics, Topic).

all_registered_topics() ->
  [Topic
    || {Topic, _CRef}
    <- ets:tab2list(emqx_mod_topic_metrics)].

init([]) ->
  erlang:process_flag(trap_exit, true),
  ok = emqx_tables:new(emqx_mod_topic_metrics,
    [{read_concurrency, true}]),
  erlang:send_after(timer:seconds(1), self(), ticking),
  {ok, #state{speeds = #{}}, hibernate}.

handle_call({register, Topic}, _From,
    State = #state{speeds = Speeds}) ->
  case is_registered(Topic) of
    true -> {reply, {error, already_existed}, State};
    false ->
      case number_of_registered_topics() < 512 of
        true ->
          CRef = counters:new(counters_size(),
            [write_concurrency]),
          true = ets:insert(emqx_mod_topic_metrics,
            {Topic, CRef}),
          [counters:put(CRef, Idx, 0)
            || Idx <- lists:seq(1, counters_size())],
          NSpeeds = lists:foldl(fun (Metric, Acc) ->
            maps:put({Topic, Metric},
              #speed{},
              Acc)
                                end,
            Speeds,
            ['messages.in',
              'messages.out',
              'messages.qos0.in',
              'messages.qos0.out',
              'messages.qos1.in',
              'messages.qos1.out',
              'messages.qos2.in',
              'messages.qos2.out',
              'messages.dropped']),
          {reply, ok, State#state{speeds = NSpeeds}};
        false -> {reply, {error, quota_exceeded}, State}
      end
  end;
handle_call({unregister, all}, _From, State) ->
  [delete_counters(Topic)
    || {Topic, _CRef}
    <- ets:tab2list(emqx_mod_topic_metrics)],
  {reply, ok, State#state{speeds = #{}}};
handle_call({unregister, Topic}, _From,
    State = #state{speeds = Speeds}) ->
  case is_registered(Topic) of
    false -> {reply, ok, State};
    true ->
      ok = delete_counters(Topic),
      NSpeeds = lists:foldl(fun (Metric, Acc) ->
        maps:remove({Topic, Metric}, Acc)
                            end,
        Speeds,
        ['messages.in',
          'messages.out',
          'messages.qos0.in',
          'messages.qos0.out',
          'messages.qos1.in',
          'messages.qos1.out',
          'messages.qos2.in',
          'messages.qos2.out',
          'messages.dropped']),
      {reply, ok, State#state{speeds = NSpeeds}}
  end;
handle_call({get_rates, Topic, Metric}, _From,
    State = #state{speeds = Speeds}) ->
  case is_registered(Topic) of
    false -> {reply, {error, topic_not_found}, State};
    true ->
      case maps:get({Topic, Metric}, Speeds, undefined) of
        undefined -> {reply, {error, invalid_metric}, State};
        #speed{last = Short, last_medium = Medium,
          last_long = Long} ->
          {reply,
            #{short => Short, medium => Medium, long => Long},
            State}
      end
  end.

handle_cast(Msg, State) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++ "Unexpected cast: ~p",
          [Msg]}
      end,
        mfa => {emqx_mod_topic_metrics, handle_cast, 2},
        line => 287})
  end,
  {noreply, State}.

handle_info(ticking, State = #state{speeds = Speeds}) ->
  NSpeeds = maps:map(fun ({Topic, Metric}, Speed) ->
    case val(Topic, Metric) of
      {error, topic_not_found} ->
        maps:remove({Topic, Metric}, Speeds);
      Val -> calculate_speed(Val, Speed)
    end
                     end,
    Speeds),
  erlang:send_after(timer:seconds(1), self(), ticking),
  {noreply, State#state{speeds = NSpeeds}};
handle_info(Info, State) ->
  begin
    logger:log(error,
      #{},
      #{report_cb =>
      fun (_) ->
        {logger_header() ++ "Unexpected info: ~p",
          [Info]}
      end,
        mfa => {emqx_mod_topic_metrics, handle_info, 2},
        line => 302})
  end,
  {noreply, State}.

terminate(_Reason, _State) -> ok.

metric_idx('messages.in') -> 1;
metric_idx('messages.out') -> 2;
metric_idx('messages.qos0.in') -> 3;
metric_idx('messages.qos0.out') -> 4;
metric_idx('messages.qos1.in') -> 5;
metric_idx('messages.qos1.out') -> 6;
metric_idx('messages.qos2.in') -> 7;
metric_idx('messages.qos2.out') -> 8;
metric_idx('messages.dropped') -> 9;
metric_idx(_) -> {error, invalid_metric}.

to_count('messages.in') -> 'messages.in.count';
to_count('messages.out') -> 'messages.out.count';
to_count('messages.qos0.in') ->
  'messages.qos0.in.count';
to_count('messages.qos0.out') ->
  'messages.qos0.out.count';
to_count('messages.qos1.in') ->
  'messages.qos1.in.count';
to_count('messages.qos1.out') ->
  'messages.qos1.out.count';
to_count('messages.qos2.in') ->
  'messages.qos2.in.count';
to_count('messages.qos2.out') ->
  'messages.qos2.out.count';
to_count('messages.dropped') ->
  'messages.dropped.count'.

to_rate('messages.in') -> 'messages.in.rate';
to_rate('messages.out') -> 'messages.out.rate';
to_rate('messages.qos0.in') -> 'messages.qos0.in.rate';
to_rate('messages.qos0.out') ->
  'messages.qos0.out.rate';
to_rate('messages.qos1.in') -> 'messages.qos1.in.rate';
to_rate('messages.qos1.out') ->
  'messages.qos1.out.rate';
to_rate('messages.qos2.in') -> 'messages.qos2.in.rate';
to_rate('messages.qos2.out') ->
  'messages.qos2.out.rate';
to_rate('messages.dropped') -> 'messages.dropped.rate'.

delete_counters(Topic) ->
  true = ets:delete(emqx_mod_topic_metrics, Topic),
  ok.

get_counters(Topic) ->
  case ets:lookup(emqx_mod_topic_metrics, Topic) of
    [] -> {error, topic_not_found};
    [{Topic, CRef}] -> CRef
  end.

counters_size() ->
  length(['messages.in',
    'messages.out',
    'messages.qos0.in',
    'messages.qos0.out',
    'messages.qos1.in',
    'messages.qos1.out',
    'messages.qos2.in',
    'messages.qos2.out',
    'messages.dropped']).

number_of_registered_topics() ->
  proplists:get_value(size,
    ets:info(emqx_mod_topic_metrics)).

calculate_speed(CurVal,
    #speed{last = Last, last_v = LastVal,
      last_medium = LastMedium, last_long = LastLong}) ->
  CurSpeed = (CurVal - LastVal) / 1,
  #speed{last_v = CurVal,
    last = short_mma(Last, CurSpeed),
    last_medium = medium_mma(LastMedium, CurSpeed),
    last_long = long_mma(LastLong, CurSpeed)}.

mma(WindowSize, LastSpeed, CurSpeed) ->
  (LastSpeed * (WindowSize - 1) + CurSpeed) / WindowSize.

short_mma(LastSpeed, CurSpeed) ->
  mma(5, LastSpeed, CurSpeed).

medium_mma(LastSpeed, CurSpeed) ->
  mma(60, LastSpeed, CurSpeed).

long_mma(LastSpeed, CurSpeed) ->
  mma(300, LastSpeed, CurSpeed).

logger_header() -> "[TOPIC_METRICS] ".