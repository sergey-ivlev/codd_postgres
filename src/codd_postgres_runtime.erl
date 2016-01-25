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
-export([equery/4, find/3, get/3, insert/2, update/2, delete/2, delete/3, count/3]).


%% =============================================================================
%% Api
%% =============================================================================

%% =============================================================================
%% Common querys
%% =============================================================================
db_equery(Sql, Args, #{connection := _Connection} = Opts) ->
    codd_postgres_db_query:equery(Sql, Args, Opts);
db_equery(Sql, Args, #{pool := PoolName}) ->
    equery_transaction(PoolName, Sql, Args).

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
equery(Module, Sql, Args, Opts) ->
    case codd_postgres_utils:typecast_args(Args) of
        {ok, TypecastArgs} ->
            Result = ?MODULE:db_equery(Sql, TypecastArgs, Opts),
            result(Module, Result);
        {error, Reason} ->
            {error, Reason}
    end.

find(Module, Condition, Opts) when is_map(Condition) ->
    Condition2 = [extract_oparation(Module, K, V) || {K,V} <- maps:to_list(Condition)],
    find(Module, Condition2, Opts);
find(Module, Condition, Opts) ->
    do([error_m ||
        Table = codd_postgres_utils:table(Module),
        Fields <- codd_postgres_utils:db_keys(Module),
        {Where, Args} <- codd_postgres_utils:where(Condition, #{typecast => true}),
        FindOpts <- codd_postgres_utils:opts(Opts),
        Sql =
            <<"SELECT ", Fields/binary,
            " FROM ", Table/binary,
            Where/binary,
            FindOpts/binary, ";">>,
        EqueryResult = db_equery(Sql, Args, Opts),
        FindResult <- find_result(Module, EqueryResult, Opts),
        return(FindResult)
    ]).

get(Module, Condition, Opts) when is_map(Condition) ->
    Condition2 = [extract_oparation(Module, K, V) || {K,V} <- maps:to_list(Condition)],
    get(Module, Condition2, Opts);
get(Module, Condition, Opts) ->
    do([error_m ||
        Table = codd_postgres_utils:table(Module),
        Fields <- codd_postgres_utils:db_keys(Module),
        {Where, Args} <- codd_postgres_utils:where(Condition, #{typecast => true}),
        Sql =
            <<"SELECT ", Fields/binary,
            " FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Sql, Args, Opts),
        GetResult <- get_result(Module, EqueryResult, Opts),
        return(GetResult)
    ]).

insert({Module, _, _} = Model, Opts) ->
    do([error_m ||
        Table = codd_postgres_utils:table(Module),
        {Keys, Iterations, Args} <- codd_postgres_utils:insert_set(Model),
        RFields <-  codd_postgres_utils:db_keys(Model),
        Sql =
            <<"INSERT INTO ", Table/binary,
            " ( ", Keys/binary, " ) ",
            " VALUES ( ", Iterations/binary, " )"
            " RETURNING ", RFields/binary,";">>,
        EqueryResult = ?MODULE:db_equery(Sql, Args, Opts),
        InsertResult <- insert_result(Module, EqueryResult),
        return(InsertResult)
    ]).

update({Module, _, _} = Model, Opts) ->
    ChangeFields = codd_model:changed_fields(Model),
    case map_size(ChangeFields) > 0 of
        true ->
            do([error_m ||
                Table = codd_postgres_utils:table(Model),
                {NextCount, SetValues, Args1} <- codd_postgres_utils:update_set(ChangeFields),
                PData <- codd_postgres_utils:primary_data(Model),
                {Where, Args2} <- codd_postgres_utils:where(PData, #{typecast => false, start_count => NextCount}),
                Args = Args1 ++ Args2,
                RFields <- codd_postgres_utils:db_keys(Model),
                Sql =
                    <<"UPDATE ", Table/binary,
                    " SET ", SetValues/binary,
                    Where/binary,
                    " RETURNING ", RFields/binary, ";">>,
                EqueryResult = ?MODULE:db_equery(Sql, Args, Opts),
                UpdateResult <- update_result(Module, EqueryResult),
                return(UpdateResult)
            ]);
        false ->
            {ok, Model}
    end.

delete(Model, Opts) ->
    do([error_m ||
        Table = codd_postgres_utils:table(Model),
        PData <- codd_postgres_utils:primary_data(Model),
        {Where, Args} <- codd_postgres_utils:where(PData, #{typecast => false}),
        Sql =
            <<"DELETE FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Sql, Args, Opts),
        DeleteResult <- delete_result(EqueryResult),
        return(DeleteResult)
    ]).
delete(Module, Condtion, Opts) ->
    do([error_m ||
        Table = codd_postgres_utils:table(Module),
        {Where, Args} <- codd_postgres_utils:where(Condtion, #{typecast => true}),
        Sql =
            <<"DELETE FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Sql, Args, Opts),
        DeleteResult <- delete_result(EqueryResult),
        return(DeleteResult)
    ]).

count(Module, Condition, Opts) ->
    do([error_m ||
        Table = codd_postgres_utils:table(Module),
        {Where, Args} <- codd_postgres_utils:where(Condition, #{typecast => true}),
        Sql =
            <<"SELECT COUNT(*)",
            " FROM ", Table/binary,
            Where/binary, ";">>,
        EqueryResult = db_equery(Sql, Args, Opts),
        CountResult <- count_result(EqueryResult),
        return(CountResult)
    ]).

get_result(Module, Result, Opts) ->
    case Result of
        {ok, []} ->
            {error, undefined};
        {ok, _, []} ->
            {error, undefined};
        {ok, Columns, Rows} ->
            [Model] = to_model(Module, Columns, Rows, Opts),
            {ok, Model};
        {error, Reason} ->
            {error, Reason}
    end.

find_result(Module, Result, Opts)->
    case Result of
        {ok, []} ->
            {error, undefined};
        {ok, Columns, Rows} ->
            Model = to_model(Module, Columns, Rows, Opts),
            {ok, Model};
        {error, Reason} ->
            {error, Reason}
    end.

insert_result(Module, Result) ->
    case Result of
        {ok, 1, Columns, Rows} ->
            [Model] = to_model(Module, Columns, Rows),
            {ok, Model};
        {error, Reason} ->
            {error, Reason}
    end.

update_result(Module, Result) ->
    case Result of
        {ok, 1, Columns, Rows} ->
            [Model] = to_model(Module, Columns, Rows),
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

result(Module, Result) ->
    case Result of
        {ok, Count} when is_integer(Count) ->
            {ok, Count};
        {ok, Columns, Rows} ->
            Models = to_model(Module, Columns, Rows),
            {ok, Models};
        {ok, _Count, Columns, Rows} ->
            Models = to_model(Module, Columns, Rows),
            {ok, Models};
        {error, Reason} ->
            {error, Reason}
    end.

to_model(Module, Colums, Rows) ->
    to_model(Module, Colums, Rows, #{}).
to_model(Module, Colums, Rows, Opts) ->
    Keys = colums_to_keys(Module, Colums),
    [row_to_model(Module, Keys, Row, Opts) || Row <- Rows].

colums_to_keys(Module, Colums) ->
    F = fun({column,BinKey,_,_,_,_}) ->
            Module:ext_key(BinKey)
        end,
    lists:map(F, Colums).

row_to_model(Module, Colums, Row, Opts) ->
    PL = lists:zipwith(fun(Key, Data) ->
        case maps:find(check_data, Opts) of
            {ok, true} ->
                {ok, TypecastValue} = codd_typecast:typecast(Module, Key, round_datetime(Data)),
                {Key, TypecastValue};
            _ ->
                {Key, round_datetime(Data)}
        end
    end, Colums, tuple_to_list(Row)),
    {ok, Model} = Module:from_db(PL),
    Model.

round_datetime({{Y,M,D},{H,Min,S}}) ->
    {{Y,M,D},{H,Min,round(S)}};
round_datetime(Data) -> Data.

extract_oparation(Module, {Key, Op}, V) ->
    {Module, Key, Op, V};
extract_oparation(Module, Key, V) ->
    {Module, Key, V}.
