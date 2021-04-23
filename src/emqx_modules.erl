%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:08
%%%-------------------------------------------------------------------
-module(emqx_modules).
-author("root").

-export([logger_header/0]).
-include("../include/logger.hrl").


-export([list/0,
  load/0,
  load/1,
  unload/0,
  unload/1,
  reload/1,
  find_module/1,
  load_module/2]).

-export([cli/1]).

-spec list() -> [{atom(), boolean()}].
list() -> ets:tab2list(emqx_modules).

-spec load() -> ok.
load() ->
  case emqx:get_env(modules_loaded_file) of
    undefined -> ok;
    File -> load_modules(File)
  end.

load(ModuleName) ->
  case find_module(ModuleName) of
    [] ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Module ~s not found, cannot load it",
              [ModuleName]}
          end,
            mfa => {emqx_modules, load, 1}, line => 52})
      end,
      {error, not_found};
    [{ModuleName, true}] ->
      begin
        logger:log(notice,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Module ~s is already started",
              [ModuleName]}
          end,
            mfa => {emqx_modules, load, 1}, line => 55})
      end,
      {error, already_started};
    [{ModuleName, false}] ->
      emqx_modules:load_module(ModuleName, true)
  end.

-spec unload() -> ok.
unload() ->
  case emqx:get_env(modules_loaded_file) of
    undefined -> ignore;
    File -> unload_modules(File)
  end.

unload(ModuleName) ->
  case find_module(ModuleName) of
    [] ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Module ~s not found, cannot load it",
              [ModuleName]}
          end,
            mfa => {emqx_modules, unload, 1}, line => 73})
      end,
      {error, not_found};
    [{ModuleName, false}] ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Module ~s is not started",
              [ModuleName]}
          end,
            mfa => {emqx_modules, unload, 1}, line => 76})
      end,
      {error, not_started};
    [{ModuleName, true}] -> unload_module(ModuleName, true)
  end.

-spec reload(module()) -> ok | ignore | {error, any()}.
reload(emqx_mod_acl_internal) ->
  Modules = emqx:get_env(modules, []),
  Env = proplists:get_value(emqx_mod_acl_internal,
    Modules,
    undefined),
  case emqx_mod_acl_internal:reload(Env) of
    ok ->
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Reload ~s module successfully.",
              [emqx_mod_acl_internal]}
          end,
            mfa => {emqx_modules, reload, 1}, line => 88})
      end,
      ok;
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Reload module ~s failed, cannot start "
              "for ~0p",
              [emqx_mod_acl_internal, Error]}
          end,
            mfa => {emqx_modules, reload, 1}, line => 91})
      end,
      {error, Error}
  end;
reload(_) -> ignore.

find_module(ModuleName) ->
  ets:lookup(emqx_modules, ModuleName).

filter_module(ModuleNames) ->
  filter_module(ModuleNames, emqx:get_env(modules, [])).

filter_module([], Acc) -> Acc;
filter_module([{ModuleName, true} | ModuleNames],
    Acc) ->
  filter_module(ModuleNames,
    lists:keydelete(ModuleName, 1, Acc));
filter_module([{_, false} | ModuleNames], Acc) ->
  filter_module(ModuleNames, Acc).

load_modules(File) ->
  case file:consult(File) of
    {ok, ModuleNames} ->
      lists:foreach(fun ({ModuleName, _}) ->
        ets:insert(emqx_modules, {ModuleName, false})
                    end,
        filter_module(ModuleNames)),
      lists:foreach(fun load_module/1, ModuleNames);
    {error, Error} ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to read: ~p, error: ~p",
              [File, Error]}
          end,
            mfa => {emqx_modules, load_modules, 1},
            line => 117})
      end
  end.

load_module({ModuleName, true}) ->
  emqx_modules:load_module(ModuleName, false);
load_module({ModuleName, false}) ->
  ets:insert(emqx_modules, {ModuleName, false});
load_module(ModuleName) ->
  load_module({ModuleName, true}).

load_module(ModuleName, Persistent) ->
  Modules = emqx:get_env(modules, []),
  Env = proplists:get_value(ModuleName,
    Modules,
    undefined),
  case ModuleName:load(Env) of
    ok ->
      ets:insert(emqx_modules, {ModuleName, true}),
      ok = write_loaded(Persistent),
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Load ~s module successfully.",
              [ModuleName]}
          end,
            mfa => {emqx_modules, load_module, 2},
            line => 134})
      end;
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Load module ~s failed, cannot load for "
              "~0p",
              [ModuleName, Error]}
          end,
            mfa => {emqx_modules, load_module, 2},
            line => 136})
      end,
      {error, Error}
  end.

unload_modules(File) ->
  case file:consult(File) of
    {ok, ModuleNames} ->
      lists:foreach(fun unload_module/1, ModuleNames);
    {error, Error} ->
      begin
        logger:log(alert,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Failed to read: ~p, error: ~p",
              [File, Error]}
          end,
            mfa => {emqx_modules, unload_modules, 1},
            line => 145})
      end
  end.

unload_module({ModuleName, true}) ->
  unload_module(ModuleName, false);
unload_module({ModuleName, false}) ->
  ets:insert(emqx_modules, {ModuleName, false});
unload_module(ModuleName) ->
  unload_module({ModuleName, true}).

unload_module(ModuleName, Persistent) ->
  Modules = emqx:get_env(modules, []),
  Env = proplists:get_value(ModuleName,
    Modules,
    undefined),
  case ModuleName:unload(Env) of
    ok ->
      ets:insert(emqx_modules, {ModuleName, false}),
      ok = write_loaded(Persistent),
      begin
        logger:log(info,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Unload ~s module successfully.",
              [ModuleName]}
          end,
            mfa => {emqx_modules, unload_module, 2},
            line => 161})
      end;
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Unload module ~s failed, cannot unload "
              "for ~0p",
              [ModuleName, Error]}
          end,
            mfa => {emqx_modules, unload_module, 2},
            line => 163})
      end
  end.

write_loaded(true) ->
  FilePath = emqx:get_env(modules_loaded_file),
  case file:write_file(FilePath,
    [io_lib:format("~p.~n", [Name]) || Name <- list()])
  of
    ok -> ok;
    {error, Error} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Write File ~p Error: ~p",
              [FilePath, Error]}
          end,
            mfa => {emqx_modules, write_loaded, 1},
            line => 171})
      end,
      ok
  end;
write_loaded(false) -> ok.

cli(["list"]) ->
  lists:foreach(fun ({Name, Active}) ->
    emqx_ctl:print("Module(~s, description=~s, active=~s)~n",
      [Name, Name:description(), Active])
                end,
    emqx_modules:list());
cli(["load", Name]) ->
  case emqx_modules:load(list_to_atom(Name)) of
    ok ->
      emqx_ctl:print("Module ~s loaded successfully.~n",
        [Name]);
    {error, Reason} ->
      emqx_ctl:print("Load module ~s error: ~p.~n",
        [Name, Reason])
  end;
cli(["unload", Name]) ->
  case emqx_modules:unload(list_to_atom(Name)) of
    ok ->
      emqx_ctl:print("Module ~s unloaded successfully.~n",
        [Name]);
    {error, Reason} ->
      emqx_ctl:print("Unload module ~s error: ~p.~n",
        [Name, Reason])
  end;
cli(["reload", "emqx_mod_acl_internal" = Name]) ->
  case emqx_modules:reload(list_to_atom(Name)) of
    ok ->
      emqx_ctl:print("Module ~s reloaded successfully.~n",
        [Name]);
    {error, Reason} ->
      emqx_ctl:print("Reload module ~s error: ~p.~n",
        [Name, Reason])
  end;
cli(["reload", Name]) ->
  emqx_ctl:print("Module: ~p does not need to be reloaded.~n",
    [Name]);
cli(_) ->
  emqx_ctl:usage([{"modules list", "Show loaded modules"},
    {"modules load <Module>", "Load module"},
    {"modules unload <Module>", "Unload module"},
    {"modules reload <Module>", "Reload module"}]).

logger_header() -> "[Modules] ".
