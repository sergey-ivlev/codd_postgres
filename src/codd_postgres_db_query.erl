%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Apr 2015 9:29 AM
%%%-------------------------------------------------------------------
-module(codd_postgres_db_query).
-author("isergey").

%% API
-export([equery/3]).
-export([transaction/2]).
-include_lib("../../../deps/epgsql/include/pgsql.hrl").

equery(Conn, Sql, Args) ->
    case pgsql:equery(Conn, Sql, Args) of
        {ok, Count} ->
            {ok, Count};
        {ok, Columns, Rows} ->
            {ok, Columns, Rows};
        {ok, Count, Columns, Rows} ->
            {ok, Count, Columns, Rows};
        {error, Reason} ->
            Reason2 = transform_error(Sql, Args, Reason),
            {error, Reason2}
    end.

transaction(Conn, Fun) ->
    pgsql:with_transaction(Conn, Fun).

transform_error(_Sql, _Args, #error{code = <<"23505">>}) ->
    not_unique;
transform_error(_Sql, _Args, #error{code = <<"23502">>}) ->
    not_null_error;
transform_error(Sql, Args, Error) ->
    {db_error,
        [
            {code, Error#error.code},
            {message, Error#error.message},
            {extra, Error#error.extra},
            {sql, Sql},
            {args, Args}
        ]}.
