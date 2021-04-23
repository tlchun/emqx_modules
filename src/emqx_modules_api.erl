%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:08
%%%-------------------------------------------------------------------
-module(emqx_modules_api).
-author("root").

-import(minirest, [return/1]).

-rest_api(#{descr => "List all modules in the cluster",
  func => list, method => 'GET', name => list_all_modules,
  path => "/modules/"}).

-rest_api(#{descr => "List all modules on a node",
  func => list, method => 'GET',
  name => list_node_modules,
  path => "/nodes/:atom:node/modules/"}).

-rest_api(#{descr => "Load a module", func => load,
  method => 'PUT', name => load_node_module,
  path => "/nodes/:atom:node/modules/:atom:module/load"}).

-rest_api(#{descr => "Unload a module", func => unload,
  method => 'PUT', name => unload_node_module,
  path =>
  "/nodes/:atom:node/modules/:atom:module/unload"}).

-rest_api(#{descr => "Reload a module", func => reload,
  method => 'PUT', name => reload_node_module,
  path => "/nodes/:atom:node/modules/:atom:module/reload"}).

-rest_api(#{descr => "load a module in the cluster",
  func => load, method => 'PUT', name => load_module,
  path => "/modules/:atom:module/load"}).

-rest_api(#{descr => "Unload a module in the cluster",
  func => unload, method => 'PUT', name => unload_module,
  path => "/modules/:atom:module/unload"}).

-rest_api(#{descr => "Reload a module in the cluster",
  func => reload, method => 'PUT', name => reload_module,
  path => "/modules/:atom:module/reload"}).

-export([list/2, list_modules/1, load/2, unload/2, reload/2]).

-export([do_load_module/2, do_unload_module/2]).

list(#{node := Node}, _Params) -> return({ok, [format(Module) || Module <- list_modules(Node)]});
list(_Bindings, _Params) -> return({ok, [format(Node, Modules) || {Node, Modules} <- list_modules()]}).

load(#{node := Node, module := Module}, _Params) ->
  return(do_load_module(Node, Module));
load(#{module := Module}, _Params) ->
  Results = [do_load_module(Node, Module) || Node <- ekka_mnesia:running_nodes()],
  case lists:filter(fun (Item) -> Item =/= ok end, Results) of
    [] -> return(ok);
    Errors -> return(lists:last(Errors))
  end.

unload(#{node := Node, module := Module}, _Params) ->
  return(do_unload_module(Node, Module));
unload(#{module := Module}, _Params) ->
  Results = [do_unload_module(Node, Module)
    || Node <- ekka_mnesia:running_nodes()],
  case lists:filter(fun (Item) -> Item =/= ok end,
    Results)
  of
    [] -> return(ok);
    Errors -> return(lists:last(Errors))
  end.

reload(#{node := Node, module := Module}, _Params) ->
  case reload_module(Node, Module) of
    ignore -> return(ok);
    Result -> return(Result)
  end;
reload(#{module := Module}, _Params) ->
  Results = [reload_module(Node, Module)
    || Node <- ekka_mnesia:running_nodes()],
  case lists:filter(fun (Item) -> Item =/= ok end,
    Results)
  of
    [] -> return(ok);
    Errors -> return(lists:last(Errors))
  end.

format(Node, Modules) ->
  #{node => Node,
    modules => [format(Module) || Module <- Modules]}.

format({Name, Active}) ->
  #{name => Name,
    description => iolist_to_binary(Name:description()),
    active => Active}.

list_modules() ->
  [{Node, list_modules(Node)}
    || Node <- ekka_mnesia:running_nodes()].

list_modules(Node) when Node =:= node() ->
  emqx_modules:list();
list_modules(Node) ->
  rpc_call(Node, list_modules, [Node]).

do_load_module(Node, Module) when Node =:= node() ->
  emqx_modules:load(Module);
do_load_module(Node, Module) ->
  rpc_call(Node, do_load_module, [Node, Module]).

do_unload_module(Node, Module) when Node =:= node() ->
  emqx_modules:unload(Module);
do_unload_module(Node, Module) ->
  rpc_call(Node, do_unload_module, [Node, Module]).

reload_module(Node, Module) when Node =:= node() ->
  emqx_modules:reload(Module);
reload_module(Node, Module) ->
  rpc_call(Node, reload_module, [Node, Module]).

rpc_call(Node, Fun, Args) ->
  case rpc:call(Node, emqx_modules_api, Fun, Args) of
    {badrpc, Reason} -> {error, Reason};
    Res -> Res
  end.

