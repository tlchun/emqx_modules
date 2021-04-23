%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:21
%%%-------------------------------------------------------------------
-author("root").
-record(subscription, {topic, subid, subopts}).

-record(message,
{id :: binary(),
  qos = 0,
  from :: atom() | binary(),
  flags = #{} :: emqx_types:flags(),
  headers = #{} :: emqx_types:headers(),
  topic :: emqx_types:topic(),
  payload :: emqx_types:payload(),
  timestamp :: integer()}).

-record(delivery,
{sender :: pid(), message :: #message{}}).

-record(route,
{topic :: binary(),
  dest :: node() | {binary(), node()}}).

-type trie_node_id() :: binary() | atom().

-record(trie_node,
{node_id :: trie_node_id(),
  edge_count = 0 :: non_neg_integer(),
  topic :: binary() | undefined,
  flags :: [atom()] | undefined}).

-record(trie_edge,
{node_id :: trie_node_id(), word :: binary() | atom()}).

-record(trie,
{edge :: #trie_edge{}, node_id :: trie_node_id()}).

-type trie_node() :: #trie_node{}.

-record(plugin,
{name :: atom(),
  dir :: string() | undefined,
  descr :: string(),
  vendor :: string() | undefined,
  active = false :: boolean(),
  info = #{} :: map(),
  type :: atom()}).

-record(command,
{name :: atom(),
  action :: atom(),
  args = [] :: list(),
  opts = [] :: list(),
  usage :: string(),
  descr :: string()}).

-record(banned,
{who ::
{clientid, binary()} |
{peerhost, inet:ip_address()} |
{username, binary()} |
{ip_address, inet:ip_address()},
  by :: binary(),
  reason :: binary(),
  at :: integer(),
  until :: integer()}).
