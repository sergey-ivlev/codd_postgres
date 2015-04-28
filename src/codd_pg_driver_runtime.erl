%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Mar 2015 10:36 AM
%%%-------------------------------------------------------------------
-module(codd_pg_driver_runtime).
-author("isergey").
-compile({parse_transform, do}).

%% API
-export([db_equery/3, transaction/2]).
-export([equery/4, find/4, get/3, insert/2, update/2, delete/2]).


%% =============================================================================
%% Api
%% =============================================================================

%% =============================================================================
%% Common querys
%% =============================================================================
db_equery(PoolName, Sql, Args) when is_atom(PoolName) or is_binary(PoolName) ->
    equery_transaction(PoolName, Sql, Args);
db_equery(Connection, Sql, Args) when is_pid(Connection) ->
    codd_pg_driver_db_query:equery(Connection, Sql, Args).

transaction(PoolName, Fun) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            codd_pg_driver_worker:transaction(
                Worker, Fun)
        end).

equery_transaction(PoolName, Sql, Args) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            codd_pg_driver_worker:equery(
                Worker, Sql, Args)
        end).

%% =============================================================================
%% codd's request
%% =============================================================================
equery(Interface, Module, Sql, FV) ->
    case codd_pg_driver_utils:typecast_args(FV) of
        {ok, TypecastArgs} ->
            Result = ?MODULE:db_equery(Interface, Sql, TypecastArgs),
            case result(Result, Module) of
                {ok, []} ->
                    {error, undefined};
                {ok, [Data]} ->
                    {ok, Data};
                {ok, Data} ->
                    {ok, Data};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

find(Interface, Module, IndexFV, Opts) ->
    do([error_m ||
        Table = Module:db_table(),
        Fields <- codd_pg_driver_utils:db_keys(Module),
        Where = codd_pg_driver_utils:where(IndexFV),
        Args <- codd_pg_driver_utils:typecast_args(IndexFV),
        BinOpts <- codd_pg_driver_utils:opts(Opts),
        Sql =
            <<"SELECT ", Fields/binary,
            " FROM ", Table/binary,
            Where/binary,
            BinOpts/binary, ";">>,
        EqueryResult = db_equery(Interface, Sql, Args),
        FindResult <- find_result(EqueryResult, Module),
        return(FindResult)
    ]).

get(Interface, Module, IndexFV) ->
    do([error_m ||
        Table = Module:db_table(),
        Fields <- codd_pg_driver_utils:db_keys(Module),
        Where = codd_pg_driver_utils:where(IndexFV),
        Args <- codd_pg_driver_utils:typecast_args(Module, IndexFV),
        Sql =
            <<"SELECT ", Fields/binary,
            " FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Interface, Sql, Args),
        GetResult <- get_result(EqueryResult, Module),
        return(GetResult)
    ]).

insert(Interface, {Module, _, _Data} = Model) ->
    do([error_m ||
        Table = Model:db_table(),
        InsertData <- codd_pg_driver_utils:insert_data(Model),
        Args <- codd_pg_driver_utils:typecast_args(Module, InsertData),
        {Keys, Iterations} = codd_pg_driver_utils:insert_args(InsertData),
        RFields <-  codd_pg_driver_utils:db_keys(Model),
        Sql =
            <<"INSERT INTO ", Table/binary,
            " ( ", Keys/binary, " ) ",
            " VALUES ( ", Iterations/binary, " )"
            " RETURNING ", RFields/binary,";">>,
        EqueryResult = ?MODULE:db_equery(Interface, Sql, Args),
        InsertResult <- insert_result(EqueryResult, Module),
        return(InsertResult)
    ]).

update(Interface, {Module, _, _Data} = Model) ->
    do([error_m ||
        Table = Model:db_table(),
        ChangeFields = codd_model:changed_fields(Model),
        {NextCount, SetValues} <- codd_pg_driver_utils:set_values(ChangeFields),
        PData <- codd_pg_driver_utils:primary_data(Model),
        Where = codd_pg_driver_utils:where(NextCount, PData),
        Args1 <- codd_pg_driver_utils:typecast_args(Module, ChangeFields),
        Args2 <- codd_pg_driver_utils:typecast_args(Module, PData),
        Args = Args1 ++ Args2,
        RFields <- codd_pg_driver_utils:db_keys(Model),
        Sql =
            <<"UPDATE ", Table/binary,
            " SET ", SetValues/binary,
            Where/binary,
            " RETURNING ", RFields/binary, ";">>,
        EqueryResult = ?MODULE:db_equery(Interface, Sql, Args),
        UpdateResult <- update_result(EqueryResult, Module),
        return(UpdateResult)
    ]).

delete(Interface, {Module, _, _Data} =Model) ->
    do([error_m ||
        Table = Model:db_table(),
        PData <- codd_pg_driver_utils:primary_data(Model),
        Where = codd_pg_driver_utils:where(PData),
        Args <- codd_pg_driver_utils:typecast_args(Module, PData),
        Sql =
            <<"DELETE FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Interface, Sql, Args),
        DeleteResult <- delete_result(EqueryResult),
        return(DeleteResult)
    ]).


get_result(Result, Module)->
    case Result of
        {ok, []} ->
            {error, undefined};
        {ok, _, []} ->
            {error, undefined};
        {ok, Columns, Rows} ->
            [Model] = to_model(Columns, Rows, Module),
            {ok, Model};
        {error, Reason} ->
            {error, Reason}
    end.

find_result(Result, Module)->
    case Result of
        {ok, []} ->
            {error, undefined};
        {ok, Columns, Rows} ->
            Model = to_model(Columns, Rows, Module),
            {ok, Model};
        {error, Reason} ->
            {error, Reason}
    end.

insert_result(Result, Module) ->
    case Result of
        {ok, 1, Columns, Rows} ->
            [Model] = to_model(Columns, Rows, Module),
            {ok, Model};
        {error, Reason} ->
            {error, Reason}
    end.

update_result(Result, Module) ->
    case Result of
        {ok, 1, Columns, Rows} ->
            [Model] = to_model(Columns, Rows, Module),
            {ok, Model};
        {ok, 0, []} ->
            {error, undefined};
        {error, Reason} ->
            {error, Reason}
    end.

delete_result(Result) ->
    case Result of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

to_model(Colums, Rows, Module) ->
    [row_to_model(Colums, Row, Module) || Row <- Rows].

row_to_model(Colums, Row, Module) ->
    PL = lists:zipwith(fun({column,Key,_,_,_,_}, Data) -> {Key, Data} end, Colums, tuple_to_list(Row)),
    {ok, Model} = Module:from_db(PL),
    Model.

result(Result, Module) ->
    case Result of
        {ok, Count} when is_integer(Count) ->
            {ok, Count};
        {ok, Columns, Rows} ->
            Models = to_model(Columns, Rows, Module),
            {ok, Models};
        {ok, _Count, Columns, Rows} ->
            Models = to_model(Columns, Rows, Module),
            {ok, Models};
        {error, Reason} ->
            {error, Reason}
    end.
