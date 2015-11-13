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
-export([start_pool/3]).
-export([equery/3, equery/4, transaction/1]).
-export([find/3, find/4, get/3, get/4, insert/1, insert/2, update/1, update/2, delete/1, delete/2, count/2, count/3]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start_pool(PoolName, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [
            {name, {local, PoolName}},
            {worker_module, codd_postgres_worker}
            | SizeArgs
        ],
    Spec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),
    ok = application:set_env(codd_postgres, pool, PoolName),
    {ok, _} = supervisor:start_child(codd_postgres_sup, Spec),
    ok.

equery(Module, Sql, Args) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    codd_postgres_runtime:equery(Pool, Module, Sql, Args).
equery(Interface, Module, Sql, Args) ->
    codd_postgres_runtime:equery(Interface, Module, Sql, Args).

transaction(Fun) ->
    {ok, PoolName} = application:get_env(codd_postgres, pool),
    codd_postgres_runtime:transaction(PoolName, Fun).

get(Module, IndexFV, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    get(Pool, Module, IndexFV, Opts).
get(Interface, Module, IndexFV, Opts) ->
    codd_postgres_runtime:get(Interface, Module, IndexFV, Opts).

update(Model)  ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    update(Pool, Model).
update(Interface, Model) ->
    codd_postgres_runtime:update(Interface, Model).

insert(Model) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    insert(Pool, Model).
insert(Interface, Model) ->
    codd_postgres_runtime:insert(Interface, Model).

find(Module, IndexFV, Opts) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    find(Pool, Module, IndexFV, Opts).
find(Interface, Module, IndexFV, Opts) ->
    codd_postgres_runtime:find(Interface, Module, IndexFV, Opts).

delete(Model) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    delete(Pool, Model).
delete(Interface, Model) ->
    codd_postgres_runtime:delete(Interface, Model).

count(Module, IndexFV) ->
    {ok, Pool} = application:get_env(codd_postgres, pool),
    count(Pool, Module, IndexFV).
count(Interface, Module, IndexFV) ->
    codd_postgres_runtime:count(Interface, Module, IndexFV).
