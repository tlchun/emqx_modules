%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:09
%%%-------------------------------------------------------------------
-module(emqx_modules_sup).
-author("root").
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, emqx_modules_sup}, emqx_modules_sup, []).

init([]) ->
  Registry = #{id => emqx_modules_registry,
    start => {emqx_modules_registry, start_link, []},
    restart => permanent,
    shutdown => 1000,
    type => worker,
    modules => [emqx_modules_registry]},
  {ok, {{one_for_one, 1, 5}, [Registry]}}.
