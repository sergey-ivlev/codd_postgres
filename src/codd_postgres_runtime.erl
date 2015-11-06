%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Mar 2015 10:36 AM
%%%-------------------------------------------------------------------
-module(codd_postgres_runtime).
-author("isergey").

-compile({parse_transform, do}).

%% API
-export([db_equery/3, transaction/2]).
-export([equery/4, find/4, get/3, insert/2, update/2, delete/2, count/3]).


%% =============================================================================
%% Api
%% =============================================================================

%% =============================================================================
%% Common querys
%% =============================================================================
db_equery(PoolName, Sql, Args) when is_atom(PoolName) or is_binary(PoolName) ->
    equery_transaction(PoolName, Sql, Args);
db_equery(Connection, Sql, Args) when is_pid(Connection) ->
    codd_postgres_db_query:equery(Connection, Sql, Args).

transaction(PoolName, Fun) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            codd_postgres_worker:transaction(
                Worker, Fun)
        end).

equery_transaction(PoolName, Sql, Args) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            codd_postgres_worker:equery(
                Worker, Sql, Args)
        end).

%% =============================================================================
%% codd's request
%% =============================================================================
equery(Interface, Module, Sql, FV) ->
    case codd_postgres_utils:typecast_args(FV) of
        {ok, TypecastArgs} ->
            Result = ?MODULE:db_equery(Interface, Sql, TypecastArgs),
            result(Result, Module);
        {error, Reason} ->
            {error, Reason}
    end.

find(Interface, Module, IndexFV, Opts) ->
    do([error_m ||
        Table = Module:db_table(),
        Fields <- codd_postgres_utils:db_keys(Module),
        Where = codd_postgres_utils:where(IndexFV),
        Args <- codd_postgres_utils:typecast_args(IndexFV),
        BinOpts <- codd_postgres_utils:opts(Opts),
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
        Fields <- codd_postgres_utils:db_keys(Module),
        Where = codd_postgres_utils:where(IndexFV),
        Args <- codd_postgres_utils:typecast_args(Module, IndexFV),
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
        InsertData <- codd_postgres_utils:insert_data(Model),
        Args <- codd_postgres_utils:typecast_args(Module, InsertData),
        {Keys, Iterations} = codd_postgres_utils:insert_args(InsertData),
        RFields <-  codd_postgres_utils:db_keys(Model),
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
    ChangeFields = codd_model:changed_fields(Model),
    case map_size(ChangeFields) > 0 of
        true ->
            do([error_m ||
                Table = Model:db_table(),
                {NextCount, SetValues} <- codd_postgres_utils:set_values(ChangeFields),
                PData <- codd_postgres_utils:primary_data(Model),
                Where = codd_postgres_utils:where(NextCount, PData),
                Args1 <- codd_postgres_utils:typecast_args(Module, ChangeFields),
                Args2 <- codd_postgres_utils:typecast_args(Module, PData),
                Args = Args1 ++ Args2,
                RFields <- codd_postgres_utils:db_keys(Model),
                Sql =
                    <<"UPDATE ", Table/binary,
                    " SET ", SetValues/binary,
                    Where/binary,
                    " RETURNING ", RFields/binary, ";">>,
                EqueryResult = ?MODULE:db_equery(Interface, Sql, Args),
                UpdateResult <- update_result(EqueryResult, Module),
                return(UpdateResult)
            ]);
        false ->
            {ok, Model}
    end.

delete(Interface, {Module, _, _Data} =Model) ->
    do([error_m ||
        Table = Model:db_table(),
        PData <- codd_postgres_utils:primary_data(Model),
        Where = codd_postgres_utils:where(PData),
        Args <- codd_postgres_utils:typecast_args(Module, PData),
        Sql =
            <<"DELETE FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Interface, Sql, Args),
        DeleteResult <- delete_result(EqueryResult),
        return(DeleteResult)
    ]).

count(Interface, Module, IndexFV) ->
    do([error_m ||
        Table = Module:db_table(),
        Where = codd_postgres_utils:where(IndexFV),
        Args <- codd_postgres_utils:typecast_args(Module, IndexFV),
        Sql =
            <<"SELECT COUNT(*)",
            " FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Interface, Sql, Args),
        CountResult <- count_result(EqueryResult),
        return(CountResult)
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

count_result(Result) ->
    case Result of
        {ok, _,[{Count}]} when is_integer(Count) ->
            {ok, Count};
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
