%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Mar 2015 2:34 PM
%%%-------------------------------------------------------------------
-module(codd_postgres).
-author("isergey").

%% API
-export([start/0, stop/0]).
-export([start_pools/1]).
-export([equery/3, equery/4, transaction/1]).
-export([get/2, get/3, find/2, find/3]).
-export([insert/1, insert/2, update/1, update/2]).
-export([delete/1, delete/2]).
-export([count/2, count/3]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start_pools(Pools) ->
    lists:foreach(
        fun({PoolName, SizeArgs, WorkerArgs}) ->
            ok = start_pool(PoolName, SizeArgs, WorkerArgs),
            ok = application:set_env(codd_postgres, pool, PoolName)
        end, Pools),
    ok.

start_pool(PoolName, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [
            {name, {local, PoolName}},
            {worker_module, codd_postgres_worker}
            | SizeArgs
        ],
    Spec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),
    {ok, _} = supervisor:start_child(codd_postgres_sup, Spec),
    ok.

equery(Module, Sql, Args) ->
    equery(Module, Sql, Args, #{}).
equery(Module, Sql, Args, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:equery(Module, Sql, Args, Opts);
equery(Module, Sql, Args, #{pool := _Conn} = Opts) ->
    codd_postgres_runtime:equery(Module, Sql, Args, Opts);
equery(Module, Sql, Args, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:equery(Module, Sql, Args, Opts2).

transaction(Fun) ->
    {ok, PoolName} = application:get_env(codd_postgres, pool),
    codd_postgres_runtime:transaction(PoolName, Fun).

get(Module, IndexFV) ->
    get(Module, IndexFV, #{}).
get(Module, IndexFV, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:get(Module, IndexFV, Opts);
get(Module, IndexFV, #{pool := _Pool} = Opts) ->
    codd_postgres_runtime:get(Module, IndexFV, Opts);
get(Module, IndexFV, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:get(Module, IndexFV, Opts2).

find(Module, FindConditions) ->
    find(Module, FindConditions, #{}).
find(Module, FindConditions, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:find(Module, FindConditions, Opts);
find(Module, FindConditions, #{pool := _Conn} = Opts) ->
    codd_postgres_runtime:find(Module, FindConditions, Opts);
find(Module, FindConditions, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:find(Module, FindConditions, Opts2).

update(Model) ->
    update(Model, #{}).
update(Model, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:update(Model, Opts);
update(Model, #{pool := _Conn} = Opts) ->
    codd_postgres_runtime:update(Model, Opts);
update(Model, Opts)  ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:update(Model, Opts2).

insert(Model) ->
    insert(Model, #{}).
insert(Model, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:insert(Model, Opts);
insert(Model, #{pool := _Conn} = Opts) ->
    codd_postgres_runtime:insert(Model, Opts);
insert(Model, Opts)  ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:insert(Model, Opts2).

delete(Model) ->
    delete(Model, #{}).
delete(Model, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:delete(Model, Opts);
delete(Model, #{pool := _Conn} = Opts) ->
    codd_postgres_runtime:delete(Model, Opts);
delete(Model, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:delete(Model, Opts2).

count(Module, CountConditions) ->
    count(Module, CountConditions, #{}).
count(Module, CountConditions, #{connection := _Conn} = Opts) ->
    codd_postgres_runtime:count(Module, CountConditions, Opts);
count(Module, CountConditions, #{pool := _Conn} = Opts) ->
    codd_postgres_runtime:count(Module, CountConditions, Opts);
count(Module, CountConditions, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    Opts2 = maps:put(pool, Pool, Opts),
    codd_postgres_runtime:count(Module, CountConditions, Opts2).
