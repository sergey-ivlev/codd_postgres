%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Apr 2015 10:02 AM
%%%-------------------------------------------------------------------
-module(codd_postgres_utils).
-author("isergey").
-compile({parse_transform, do}).

%% API
-export([typecast_args/1, typecast_args/2]).
-export([primary_data/1, insert_data/1, insert_args/1, db_keys/1, db_keys/2]).
-export([join_and_with_count/1, join_and_with_count/2]).
-export([where/1, where/2, set_values/1]).
-export([opts/1]).

primary_data({Module, Data, _}) ->
    Fun = fun(K,V, {Acc, Errors}) ->
        case Module:is_primary(K) of
            true ->
                case V of
                    undefined -> {Acc, [codd_error:required_error(K) | Errors]};
                    _ -> {maps:put(K, V, Acc), Errors}
                end;
            false ->
                {Acc, Errors}
        end
    end,
    case maps:fold(Fun,{maps:new(), []},Data) of
        {Acc, []} ->
            case maps:size(Acc) =:= 0 of
                true ->
                    {error, no_pkeys};
                false ->
                    {ok, Acc}
            end;
        {_, Error} ->
            {error, Error}
    end.

insert_data({Module, Data, _}) ->
    Fun = fun(K,V, {Acc, Errors}) ->
        case V of
            undefined ->
                case Module:is_required(K) of
                    true -> {Acc, [codd_error:required_error(K) | Errors]};
                    false -> {Acc, Errors}
                end;
            _ ->
                {maps:put(K, V, Acc), Errors}
        end
    end,
    case maps:fold(Fun,{maps:new(), []},Data) of
        {Acc, []} -> {ok, Acc};
        {_, Error} -> {error, Error}
    end.

db_keys(Model) ->
    Keys = codd_model:db_keys(Model),
    case Keys of
        [] -> {error, no_db_keys};
        _ ->
            Table = Model:db_table(),
            <<$,, BinResult/binary>> =
                << <<$,, "\"", Table/binary, "\".\"", F/binary,"\" ">> || F <- Keys>>,
            {ok, BinResult}
    end.
db_keys(Keys, Model) ->
    ModelKeys = codd_model:db_keys(Keys, Model),
    case ModelKeys of
        [] -> {error, no_db_keys};
        _ ->
            Table = Model:db_table(),
            <<$,, BinResult/binary>> =
                << <<$,, "\"", Table/binary, "\".\"", F/binary,"\" ">> || F <- ModelKeys>>,
            {ok, BinResult}
    end.

opts(Opts) ->
    do([error_m ||
        Limit <- limit(Opts),
        Offset <- offset(Opts),
        Order <- order_by(Opts),
        return(<<Order/binary, Limit/binary, Offset/binary>>)
    ]).

limit(Opts) ->
    case maps:find(limit, Opts) of
        {ok, Limit} when is_integer(Limit) ->
            {ok, <<" LIMIT ",(integer_to_binary(Limit))/binary>>};
        error ->
            {ok, <<>>};
        {ok, _} ->
            {error, codd_error:unvalid_error(limit)}
    end.

offset(Opts) ->
    case maps:find(offset, Opts) of
        {ok, Offset} when is_integer(Offset) ->
            {ok, <<" OFFSET ",(integer_to_binary(Offset))/binary>>};
        error ->
            {ok, <<>>};
        {ok, _} ->
            {error, codd_error:unvalid_error(offset)}
    end.

order_by(Opts) ->
    case maps:find(order_by, Opts) of
        {ok, {Field, desc}} when is_atom(Field)->
            {ok, <<" ORDER BY \"",((atom_to_binary(Field, latin1)))/binary, "\" DESC">>};
        {ok, {Field, asc}} when is_atom(Field)->
            {ok, <<" ORDER BY \"",((atom_to_binary(Field, latin1)))/binary, "\" ASC">>};
        {ok, Field} when is_atom(Field)->
            {ok, <<" ORDER BY \"",((atom_to_binary(Field, latin1)))/binary, "\" ASC">>};
        error ->
            {ok, <<>>};
        {ok, _} ->
            {error, codd_error:unvalid_error(order_by)}
    end.

where(Fields) ->
    where(1, Fields).
where(_, []) ->
    <<>>;
where(_, Map) when is_map(Map) andalso map_size(Map) =:= 0 ->
    <<>>;
where(Count, IndexFV) ->
    {_, Data} = join_and_with_count(Count, IndexFV),
    <<" WHERE ", Data/binary>>.


insert_args(Fields) ->
    Fun = fun(K,_, {I, KeyAcc, IterationAcc}) ->
        {I+1,
            <<KeyAcc/binary, ", \"", (atom_to_binary(K, latin1))/binary, "\"">>,
            <<IterationAcc/binary, ", $", (integer_to_binary(I))/binary>>
        }
    end,
    {_, <<", ", Keys/binary>>, <<", ", Iterations/binary>>} = maps:fold(Fun, {1, <<"">>, <<"">>}, Fields),
    {Keys, Iterations}.

set_values([]) ->
    {error, nothing_changed};
set_values(Map) when is_map(Map) andalso map_size(Map) =:= 0 ->
    {error, nothing_changed};
set_values(Fields) ->
    Result = join_comma_with_count(Fields),
    {ok, Result}.

join_and_with_count(FV) ->
    join_and_with_count(1, FV).

join_and_with_count(Count, FV) when is_map(FV) ->
    Fun = fun(K,_, {I, Acc}) ->
        {I+1, <<Acc/binary, " AND \"", (atom_to_binary(K, latin1))/binary, "\" = $", (integer_to_binary(I))/binary>>}
    end,
    {NextCount, <<" AND ", TotalAcc/binary>>} = maps:fold(Fun, {Count, <<"">>}, FV),
    {NextCount, TotalAcc};

join_and_with_count(Count, FV) when is_list(FV) ->
    Fun = fun({Module, K,_}, {I, Acc}) ->
            Table = Module:db_table(),
            {I+1, <<Acc/binary, " AND \"", (Table)/binary, "\".\"", (atom_to_binary(K, latin1))/binary, "\" = $", (integer_to_binary(I))/binary>>};
        ({Module, K, Op, _}, {I, Acc}) ->
            Table = Module:db_table(),
            BinOpt = op_to_bin(Op),
            {I+1, <<Acc/binary, " AND \"", (Table)/binary, "\".\"", (atom_to_binary(K, latin1))/binary, "\" ",BinOpt/binary," $", (integer_to_binary(I))/binary>>}
    end,
    {NextCount, <<" AND ", TotalAcc/binary>>} = lists:foldl(Fun, {Count, <<"">>}, FV),
    {NextCount, TotalAcc}.

join_comma_with_count(FV) ->
    join_comma_with_count(1, FV).

join_comma_with_count(Count, FV) when is_map(FV) ->
    Fun = fun(K,_, {I, Acc}) ->
        {I+1, <<Acc/binary, " , \"", (atom_to_binary(K, latin1))/binary, "\" = $", (integer_to_binary(I))/binary>>}
    end,
    {NextCount, <<" , ", TotalAcc/binary>>} = maps:fold(Fun, {Count, <<"">>}, FV),
    {NextCount, TotalAcc};

join_comma_with_count(Count, FV) when is_list(FV) ->
    Fun = fun({Module, K,_}, {I, Acc}) ->
        Table = Module:db_table(),
        {I+1, <<Acc/binary, " , \"", (Table)/binary, "\".\"", (atom_to_binary(K, latin1))/binary, "\" = $", (integer_to_binary(I))/binary>>}
    end,
    {NextCount, <<" , ", TotalAcc/binary>>} = lists:foldl(Fun, {Count, <<"">>}, FV),
    {NextCount, TotalAcc}.

typecast_args(Args) ->
    Fun = fun({Module, Key, Value}, {Acc, Errors}) ->
            case codd_typecast:typecast(Module, Key, Value) of
                {ok, ValidValue} -> { [ValidValue | Acc], Errors};
                {error, Error} ->  {Acc, [Error | Errors]}
            end;
        ({Module, Key, _, Value}, {Acc, Errors}) ->
            case codd_typecast:typecast(Module, Key, Value) of
                {ok, ValidValue} -> { [ValidValue | Acc], Errors};
                {error, Error} ->  {Acc, [Error | Errors]}
            end
    end,
    case lists:foldl(Fun, {[], []}, Args) of
        {Acc, []} ->
            {ok, lists:reverse(Acc)};
        {_, Errors} ->
            {error, Errors}
    end.
typecast_args(_, Args) when is_list(Args)->
    typecast_args(Args);
typecast_args(Module, Args) when is_map(Args) ->
    Fun = fun (Key, Value, {Acc, Errors}) ->
        case codd_typecast:typecast(Module, Key, Value) of
            {ok, ValidValue} -> { [ValidValue | Acc], Errors};
            {error, Error} ->  {Acc, [Error | Errors]}
        end
    end,
    case maps:fold(Fun, {[], []}, Args) of
        {Acc, []} ->
            {ok, lists:reverse(Acc)};
        {_, Errors} ->
            {error, Errors}
    end.

op_to_bin(Op) ->
    case Op of
        '=' -> <<"=">>;
        '>' -> <<">">>;
        '<' -> <<"<">>;
        '>=' -> <<">=">>;
        '<=' -> <<"<=">>
    end.

-include_lib("eunit/include/eunit.hrl").
opts_test() ->
    %% one option
    ?assertEqual({ok, <<>>}, opts(#{})),
    ?assertEqual({ok, <<" LIMIT 1">>}, opts(#{limit => 1})),
    ?assertEqual({ok, <<" OFFSET 10">>}, opts(#{offset => 10})),
    ?assertEqual({ok, <<" ORDER BY \"id\" ASC">>}, opts(#{order_by => id})),
    ?assertEqual({ok, <<" ORDER BY \"id\" ASC">>}, opts(#{order_by => {id, asc}})),
    ?assertEqual({ok, <<" ORDER BY \"id\" DESC">>}, opts(#{order_by => {id, desc}})),
    %% pair
    ?assertEqual({ok, <<" LIMIT 1 OFFSET 10">>}, opts(#{limit => 1, offset => 10})),

    ?assertEqual({ok, <<" ORDER BY \"id\" ASC LIMIT 1">>}, opts(#{limit => 1, order_by => id})),
    ?assertEqual({ok, <<" ORDER BY \"id\" ASC LIMIT 1">>}, opts(#{limit => 1, order_by => {id, asc}})),
    ?assertEqual({ok, <<" ORDER BY \"id\" DESC LIMIT 1">>}, opts(#{limit => 1, order_by => {id, desc}})),

    ?assertEqual({ok, <<" ORDER BY \"id\" ASC OFFSET 10">>}, opts(#{offset => 10, order_by => id})),
    ?assertEqual({ok, <<" ORDER BY \"id\" ASC OFFSET 10">>}, opts(#{offset => 10, order_by => {id, asc}})),
    ?assertEqual({ok, <<" ORDER BY \"id\" DESC OFFSET 10">>}, opts(#{offset => 10, order_by => {id, desc}})),
    %% limit, offset, order
    ?assertEqual({ok, <<" ORDER BY \"id\" ASC LIMIT 1 OFFSET 10">>}, opts(#{limit => 1, offset => 10, order_by => id})),
    ?assertEqual({ok, <<" ORDER BY \"id\" ASC LIMIT 1 OFFSET 10">>}, opts(#{limit => 1, offset => 10, order_by => {id, asc}})),
    ?assertEqual({ok, <<" ORDER BY \"id\" DESC LIMIT 1 OFFSET 10">>}, opts(#{limit => 1, offset => 10, order_by => {id, desc}})).
