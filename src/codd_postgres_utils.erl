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
-export([typecast_args/1]).
-export([primary_data/1, insert_set/1, db_keys/1, db_keys/2]).
-export([join_and_with_count/1, join_and_with_count/2]).
-export([where/1, where/2, update_set/1]).
-export([opts/1]).
-export([quoted/1, table/1]).
-export([table/0, type/1]).

-include_lib("eunit/include/eunit.hrl").

primary_data({Module, Data, _}) ->
    Fun = fun(K,V, {Acc, Errors}) ->
        case Module:is_primary(K) of
            true ->
                case V of
                    undefined -> {Acc, [codd_error:required_error(K) | Errors]};
                    _ -> {[{Module, K, V} | Acc], Errors}
                end;
            false ->
                {Acc, Errors}
        end
    end,
    case maps:fold(Fun,{[], []},Data) of
        {[], []} ->
            {error, no_pkeys};
        {Acc, []} ->
            {ok, Acc};
        {_, Error} ->
            {error, Error}
    end.

insert_set({Module, Data, _}) ->
    Fun = fun(K,V, {{I, KeyAcc, IterationAcc, Args}=Acc, Errors}) ->
        case V of
            undefined ->
                case Module:is_required(K) of
                    true -> {Acc, [codd_error:required_error(K) | Errors]};
                    false -> {Acc, Errors}
                end;
            _ ->
                {{
                    I+1,
                    <<KeyAcc/binary, ", ", (quoted(K))/binary>>,
                    <<IterationAcc/binary, ", $", (integer_to_binary(I))/binary>>,
                    [V | Args]
                }, Errors}

        end
    end,
    case maps:fold(Fun, {{1, <<"">>, <<"">>, []},[]}, Data) of
        {Acc, []} ->
            {_, <<", ", Keys/binary>>, <<", ", Iterations/binary>>, ReversedArgs} = Acc,
            Args = lists:reverse(ReversedArgs),
            {ok, {Keys, Iterations, Args}};
        {_, Error} -> {error, Error}
    end.

update_set([]) ->
    {error, nothing_changed};
update_set(Map) when is_map(Map) andalso map_size(Map) =:= 0 ->
    {error, nothing_changed};
update_set(Fields) ->
    Fun = fun(K,V, {I, Acc, Args}) ->
        {
            I + 1,
            <<Acc/binary, " , ", (quoted(K))/binary, " = $", (integer_to_binary(I+1))/binary>>,
            [V | Args]
        }
    end,
    {LastCount, <<" , ", ResultAcc/binary>>, ReversedArgs} = maps:fold(Fun, {0, <<"">>, []}, Fields),
    ResultArgs = lists:reverse(ReversedArgs),
    {ok, {LastCount, ResultAcc, ResultArgs}}.

db_keys(Model) ->
    Keys = codd_model:db_keys(Model),
    case Keys of
        [] -> {error, no_db_keys};
        _ ->
            Table = Model:table(),
            <<$,, BinResult/binary>> =
                << <<$,, (quoted(Table))/binary, ".", (quoted(F))/binary," ">> || F <- Keys>>,
            {ok, BinResult}
    end.
db_keys(Keys, Model) ->
    ModelKeys = codd_model:db_keys(Keys, Model),
    case ModelKeys of
        [] -> {error, no_db_keys};
        _ ->
            Table = Model:table(),
            <<$,, BinResult/binary>> =
                << <<$,, (quoted(Table))/binary, ".", (quoted(F))/binary," ">> || F <- Keys>>,
            {ok, BinResult}
    end.

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
            Table = Module:table(),
            {I+1, <<Acc/binary, " AND \"", (Table)/binary, "\".\"", (atom_to_binary(K, latin1))/binary, "\" = $", (integer_to_binary(I))/binary>>};
        ({Module, K, Op, _}, {I, Acc}) ->
            Table = Module:table(),
            BinOpt = op_to_bin(Op),
            {I+1, <<Acc/binary, " AND \"", (Table)/binary, "\".\"", (atom_to_binary(K, latin1))/binary, "\" ",BinOpt/binary," $", (integer_to_binary(I))/binary>>}
    end,
    {NextCount, <<" AND ", TotalAcc/binary>>} = lists:foldl(Fun, {Count, <<"">>}, FV),
    {NextCount, TotalAcc}.

table({Module,_,_}) ->
    quoted(Module:table());
table(Module) ->
    quoted(Module:table()).

quoted(Bin) when is_binary(Bin) ->
    <<"\"",Bin/binary,"\"">>;
quoted(Atom) when is_atom(Atom) ->
    quoted(atom_to_binary(Atom, latin1)).

quoted_test() ->
    ?assertEqual(<<"\"binary\"">>, quoted(<<"binary">>)),
    ?assertEqual(<<"\"atom\"">>, quoted(atom)).

%%========select option===========
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

%%======== WHERE condition ===========
where(Condition) ->
    where(Condition, #{typecast => true, start_count => 0}).
where([], _) ->
    {ok, {<<>>, []}};
where(Condition, Opts) ->
    case parse_condition(Condition, Opts) of
        {ok, {Sql, Args}} ->
            {ok, {<<" WHERE ", Sql/binary>>, Args}};
        {error, Reason} ->
            {error, Reason}
    end.

parse_condition(Condition) ->
    parse_condition(Condition, #{}).
parse_condition(Condition, Opts) ->
    Typecast = maps:get(typecast, Opts, true),
    StartCount = maps:get(start_count, Opts, 0),
    {Sql, Args, _} = parse_tree(Condition, [], StartCount),
    RArgs = lists:reverse(Args),
    case Typecast of
        true ->
            case typecast_args(RArgs) of
                {ok, ValidArgs} ->
                    {ok, {Sql, ValidArgs}};
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            L = [Value || {_,_,Value} <- RArgs],
            {ok, {Sql, L}}
    end.

parse_tree({'or', []}, Args, StartCount) ->
    {<<>>, Args, StartCount};
parse_tree({'or', List}, Args, StartCount) when is_list(List) ->
    Fun = fun(Condition, {BinAcc, ArgsAcc, Count}) ->
        {SubBin, SubArgs, LastCount} = parse_tree(Condition, ArgsAcc, Count),
        {<<BinAcc/binary, " OR ", SubBin/binary>>, SubArgs, LastCount}
    end,
    {<<" OR ", TotalAcc/binary>>, ResultArgs, ResultCount} = lists:foldl(Fun, {<<"">>, Args, StartCount}, List),
    {<<"(", TotalAcc/binary,")">>, ResultArgs, ResultCount};

parse_tree({'and', []}, Args, StartCount) ->
    {<<>>, Args, StartCount};
parse_tree({'and', List}, Args, StartCount) when is_list(List) ->
    Fun = fun(Condition, {BinAcc, ArgsAcc, Count}) ->
        {SubBin, SubArgs, LastCount} = parse_tree(Condition, ArgsAcc, Count),
        {<<BinAcc/binary, " AND ", SubBin/binary>>, SubArgs, LastCount}
    end,
    {<<" AND ", TotalAcc/binary>>, ResultArgs, ResultCount} = lists:foldl(Fun, {<<"">>, Args, StartCount}, List),
    {<<"(", TotalAcc/binary,")">>, ResultArgs, ResultCount};

parse_tree(List, Args, StartCount) when is_list(List) ->
    Fun = fun(Condition, {BinAcc, ArgsAcc, Count}) ->
        {SubBin, SubArgs, LastCount} = parse_tree(Condition, ArgsAcc, Count),
        {<<BinAcc/binary, " AND ", SubBin/binary>>, SubArgs, LastCount}
    end,
    {<<" AND ", TotalAcc/binary>>, ResultArgs, ResultCount} = lists:foldl(Fun, {<<"">>, Args, StartCount}, List),
    {<<"(", TotalAcc/binary,")">>, ResultArgs, ResultCount};
%%
%% parse_tree({Key, Value}, Args, StartCount)->
%%     BinKey = quoted(Key),
%%     Count = StartCount + 1,
%%     BinCount = integer_to_binary(Count),
%%     {<<BinKey/binary," = $",BinCount/binary>>, [{Key, Value} | Args], Count};
parse_tree({Module, Key, Value}, Args, StartCount)->
    Table = quoted(Module:table()),
    BinKey = quoted(Key),
    Count = StartCount + 1,
    BinCount = integer_to_binary(Count),
    {<<Table/binary,".",BinKey/binary," = $",BinCount/binary>>, [{Module, Key, Value} | Args], Count};
parse_tree({Module, Key, Op, Value}, Args, StartCount)->
    Table = quoted(Module:table()),
    BinKey = quoted(Key),
    Count = StartCount + 1,
    BinCount = integer_to_binary(Count),
    BinOp = op_to_bin(Op),
    {<<Table/binary,".",BinKey/binary," ", BinOp/binary," $",BinCount/binary>>, [{Module, Key, Value} | Args], Count}.

typecast_args(Args) ->
    Fun = fun
        ({Module, Key, Value}, {Acc, Errors}) ->
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

op_to_bin(Op) ->
    case Op of
        '=' -> <<"=">>;
        '>' -> <<">">>;
        '<' -> <<"<">>;
        '>=' -> <<">=">>;
        '<=' -> <<"<=">>;
        '<>' -> <<"<>">>;
        '!=' -> <<"!=">>
    end.

parse_condition_test() ->
    ?assertEqual({ok, {<<"\"table\".\"key\" = $1">>, [1]}}, parse_condition({?MODULE, key, 1})),
    ?assertEqual({ok, {<<>>, []}}, parse_condition({'or', []})),
    ?assertEqual({ok, {<<>>, []}}, parse_condition({'and', []})),
    ?assertEqual({ok, {<<"(\"table\".\"key1\" = $1 OR \"table\".\"key2\" = $2 OR \"table\".\"key3\" = $3)">>, [1,2,3]}},
        parse_condition({'or', [{?MODULE, key1, 1},{?MODULE, key2, 2},{?MODULE, key3, 3}]})),
    ?assertEqual({ok, {<<"(\"table\".\"key1\" = $1 AND \"table\".\"key2\" = $2 AND \"table\".\"key3\" = $3)">>, [1,2,3]}},
        parse_condition({'and', [{?MODULE, key1, 1},{?MODULE, key2, 2},{?MODULE, key3, 3}]})),
    ?assertEqual({ok, {<<"(\"table\".\"key1\" = $1 AND \"table\".\"key2\" = $2 AND \"table\".\"key3\" = $3)">>, [1,2,3]}},
        parse_condition([{?MODULE, key1, 1},{?MODULE, key2, 2},{?MODULE, key3, 3}])),

    ?assertEqual({ok, {<<"\"table\".\"key\" > $1">>, [1]}}, parse_condition({?MODULE, key, '>', 1})),
    ?assertEqual({ok, {<<"\"table\".\"key\" >= $1">>, [1]}}, parse_condition({?MODULE, key, '>=', 1})),
    ?assertEqual({ok, {<<"\"table\".\"key\" != $1">>, [1]}}, parse_condition({?MODULE, key, '!=', 1})),
    ?assertEqual({ok, {<<"(\"table\".\"key1\" > $1 OR \"table\".\"key2\" < $2)">>, [1,2]}},
        parse_condition({'or', [{?MODULE, key1, '>', 1},{?MODULE, key2, '<', 2}]})),
    ?assertEqual({ok, {<<"(\"table\".\"key1\" = $1 AND \"table\".\"key2\" < $2)">>, [1,2]}},
        parse_condition([{?MODULE, key1, 1},{?MODULE, key2, '<', 2}])),
    ?assertEqual({ok, {<<"(\"table\".\"key1\" = $1 AND (\"table\".\"key2\" = $2 OR \"table\".\"key3\" < $3))">>, [0,1,2]}},
        parse_condition({'and', [{?MODULE, key1, 0},{'or',[{?MODULE, key2, 1},{?MODULE, key3, '<', 2}]}]})).


table() -> <<"table">>.
type(_) -> integer.
