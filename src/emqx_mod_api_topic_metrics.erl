%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 4月 2021 下午8:04
%%%-------------------------------------------------------------------
-module(emqx_mod_api_topic_metrics).
-author("root").
-import(minirest, [return/1]).

-rest_api(#{descr =>
"A list of all topic metrics of all nodes in the cluster",
  func => list, method => 'GET',
  name => list_all_topic_metrics,
  path => "/topic-metrics"}).

-rest_api(#{descr =>
"A list of specfied topic metrics of all nodes in the cluster",
  func => list, method => 'GET',
  name => list_topic_metrics,
  path => "/topic-metrics/:bin:topic"}).

-rest_api(#{descr => "Register topic metrics",
  func => register, method => 'POST',
  name => register_topic_metrics,
  path => "/topic-metrics"}).

-rest_api(#{descr => "Unregister all topic metrics",
  func => unregister, method => 'DELETE',
  name => unregister_all_topic_metrics,
  path => "/topic-metrics"}).

-rest_api(#{descr => "Unregister topic metrics",
  func => unregister, method => 'DELETE',
  name => unregister_topic_metrics,
  path => "/topic-metrics/:bin:topic"}).

-export([list/2, register/2, unregister/2]).

list(#{topic := Topic0}, _Params) ->
  execute_when_enabled(fun () ->
    Topic = emqx_mgmt_util:urldecode(Topic0),
    case safe_validate(Topic) of
      true ->
        case get_topic_metrics(Topic) of
          {error, Reason} ->
            return({error, Reason});
          Metrics ->
            return({ok,
              maps:from_list(Metrics)})
        end;
      false ->
        return({error, invalid_topic_name})
    end
                       end);
list(_Bindings, _Params) ->
  execute_when_enabled(fun () ->
    case get_all_topic_metrics() of
      {error, Reason} -> return({error, Reason});
      Metrics -> return({ok, Metrics})
    end
                       end).

register(_Bindings, Params) ->
  execute_when_enabled(fun () ->
    case proplists:get_value(<<"topic">>, Params)
    of
      undefined ->
        return({error,
          missing_required_params});
      Topic ->
        case safe_validate(Topic) of
          true ->
            register_topic_metrics(Topic),
            return(ok);
          false ->
            return({error,
              invalid_topic_name})
        end
    end
                       end).

unregister(Bindings, _Params)
  when map_size(Bindings) =:= 0 ->
  execute_when_enabled(fun () ->
    unregister_all_topic_metrics(),
    return(ok)
                       end);
unregister(#{topic := Topic0}, _Params) ->
  execute_when_enabled(fun () ->
    Topic = emqx_mgmt_util:urldecode(Topic0),
    case safe_validate(Topic) of
      true ->
        unregister_topic_metrics(Topic),
        return(ok);
      false ->
        return({error, invalid_topic_name})
    end
                       end).

execute_when_enabled(Fun) ->
  Enabled = case
              emqx_modules:find_module(emqx_mod_topic_metrics)
            of
              [{_, false}] -> false;
              [{_, true}] -> true
            end,
  case Enabled of
    true -> Fun();
    false -> return({error, module_not_loaded})
  end.

safe_validate(Topic) ->
  try emqx_topic:validate(name, Topic) of
    true -> true
  catch
    error:_Error -> false
  end.

get_all_topic_metrics() ->
  lists:foldl(fun (Topic, Acc) ->
    case get_topic_metrics(Topic) of
      {error, _Reason} -> Acc;
      Metrics ->
        [#{topic => Topic, metrics => Metrics} | Acc]
    end
              end,
    [],
    emqx_mod_topic_metrics:all_registered_topics()).

get_topic_metrics(Topic) ->
  lists:foldl(fun (Node, Acc) ->
    case get_topic_metrics(Node, Topic) of
      {error, _Reason} -> Acc;
      Metrics ->
        case Acc of
          [] -> Metrics;
          _ ->
            lists:foldl(fun ({K, V}, Acc0) ->
              [{K,
                V +
                  proplists:get_value(K,
                    Metrics,
                    0)}
                | Acc0]
                        end,
              [],
              Acc)
        end
    end
              end,
    [],
    ekka_mnesia:running_nodes()).

get_topic_metrics(Node, Topic) when Node =:= node() ->
  emqx_mod_topic_metrics:metrics(Topic);
get_topic_metrics(Node, Topic) ->
  rpc_call(Node, get_topic_metrics, [Node, Topic]).

register_topic_metrics(Topic) ->
  Results = [register_topic_metrics(Node, Topic)
    || Node <- ekka_mnesia:running_nodes()],
  case lists:any(fun (Item) -> Item =:= ok end, Results)
  of
    true -> ok;
    false -> lists:last(Results)
  end.

register_topic_metrics(Node, Topic)
  when Node =:= node() ->
  emqx_mod_topic_metrics:register(Topic);
register_topic_metrics(Node, Topic) ->
  rpc_call(Node, register_topic_metrics, [Node, Topic]).

unregister_topic_metrics(Topic) ->
  Results = [unregister_topic_metrics(Node, Topic)
    || Node <- ekka_mnesia:running_nodes()],
  case lists:any(fun (Item) -> Item =:= ok end, Results)
  of
    true -> ok;
    false -> lists:last(Results)
  end.

unregister_topic_metrics(Node, Topic)
  when Node =:= node() ->
  emqx_mod_topic_metrics:unregister(Topic);
unregister_topic_metrics(Node, Topic) ->
  rpc_call(Node, unregister_topic_metrics, [Node, Topic]).

unregister_all_topic_metrics() ->
  Results = [unregister_all_topic_metrics(Node)
    || Node <- ekka_mnesia:running_nodes()],
  case lists:any(fun (Item) -> Item =:= ok end, Results)
  of
    true -> ok;
    false -> lists:last(Results)
  end.

unregister_all_topic_metrics(Node)
  when Node =:= node() ->
  emqx_mod_topic_metrics:unregister_all();
unregister_all_topic_metrics(Node) ->
  rpc_call(Node, unregister_topic_metrics, [Node]).

rpc_call(Node, Fun, Args) ->
  case rpc:call(Node,
    emqx_mod_api_topic_metrics,
    Fun,
    Args)
  of
    {badrpc, Reason} -> {error, Reason};
    Res -> Res
  end.
