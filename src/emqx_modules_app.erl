%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4ζ 2021 δΈε8:09
%%%-------------------------------------------------------------------
-module(emqx_modules_app).
-author("root").

-behaviour(application).
-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
  _ = application:load(emqx),
  {ok, Pid} = emqx_modules_sup:start_link(),
  ok = emqx_modules:load(),
  emqx_ctl:register_command(modules, {emqx_modules, cli}, []),
  {ok, Pid}.

stop(_State) ->
  emqx_ctl:unregister_command(modules),
  emqx_modules:unload().

