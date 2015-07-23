%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Mar 2015 2:34 PM
%%%-------------------------------------------------------------------
-module(codd_pg_driver).
-author("isergey").

%% API
-export([start/0, stop/0]).
-export([start_pool/3]).
-export([equery/3, equery/4, transaction/1]).
-export([find/3, find/4, get/2, get/3, insert/1, insert/2, update/1, update/2, delete/1, delete/2, count/2, count/3]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start_pool(PoolName, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [
            {name, {local, PoolName}},
            {worker_module, codd_pg_driver_worker}
            | SizeArgs
        ],
    Spec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),
    application:set_env(codd_pg_driver, pool, PoolName),
    supervisor:start_child(codd_pg_driver_sup, Spec).

equery(Module, Sql, Args) ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    codd_pg_driver_runtime:equery(Pool, Module, Sql, Args).
equery(Interface, Module, Sql, Args) ->
    codd_pg_driver_runtime:equery(Interface, Module, Sql, Args).

transaction(Fun) ->
    {ok, PoolName} = application:get_env(codd_pg_driver, pool),
    codd_pg_driver_runtime:transaction(PoolName, Fun).

get(Module, IndexFV) ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    get(Pool, Module, IndexFV).
get(Interface, Module, IndexFV) ->
    codd_pg_driver_runtime:get(Interface, Module, IndexFV).

update(Model)  ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    update(Pool, Model).
update(Interface, Model) ->
    codd_pg_driver_runtime:update(Interface, Model).

insert(Model) ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    insert(Pool, Model).
insert(Interface, Model) ->
    codd_pg_driver_runtime:insert(Interface, Model).

find(Module, IndexFV, Opts) ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    find(Pool, Module, IndexFV, Opts).
find(Interface, Module, IndexFV, Opts) ->
    codd_pg_driver_runtime:find(Interface, Module, IndexFV, Opts).

delete(Model) ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    delete(Pool, Model).
delete(Interface, Model) ->
    codd_pg_driver_runtime:delete(Interface, Model).

count(Module, IndexFV) ->
    {ok, Pool} = application:get_env(codd_pg_driver, pool),
    count(Pool, Module, IndexFV).
count(Interface, Module, IndexFV) ->
    codd_pg_driver_runtime:count(Interface, Module, IndexFV).
