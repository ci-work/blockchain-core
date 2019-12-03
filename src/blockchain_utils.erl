%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_utils).

-export([
    shuffle_from_hash/2,
    shuffle/1,
    rand_from_hash/1, rand_state/1,
    normalize_float/1,
    challenge_interval/1,
    serialize_hash/1, deserialize_hash/1,
    hex_to_bin/1, bin_to_hex/1,
    pmap/2,
    addr2name/1,
    distance/2,
    score_gateways/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(CHALLENGE_INTERVAL, poc_challenge_interval).

%%--------------------------------------------------------------------
%% @doc Shuffle a list deterministically using a random binary as the seed.
%% @end
%%--------------------------------------------------------------------
-spec shuffle_from_hash(binary(), list()) -> list().
shuffle_from_hash(Hash, L) ->
    ?MODULE:rand_from_hash(Hash),
    [X ||{_, X} <- lists:sort([{rand:uniform(), E} || E <- L])].

%%--------------------------------------------------------------------
%% @doc Shuffle a list randomly.
%% @end
%%--------------------------------------------------------------------
shuffle(List) ->
    [X || {_,X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rand_from_hash(binary()) -> any().
rand_from_hash(Hash) ->
    <<I1:86/integer, I2:85/integer, I3:85/integer, _/binary>> = Hash,
    rand:seed(exs1024, {I1, I2, I3}).

%%--------------------------------------------------------------------
%% @doc normalize a float by converting it to fixed point and back
%% using 16 bits of exponent precision. This should be well above
%% the floating point error threshold and doing this will prevent
%% errors from accumulating.
%% @end
%%--------------------------------------------------------------------
normalize_float(Float) ->
    round(Float * 65536) / 65536.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenge_interval(blockchain_ledger_v1:ledger()) -> non_neg_integer().
challenge_interval(Ledger) ->
    {ok, Interval} = blockchain:config(?CHALLENGE_INTERVAL, Ledger),
    Interval.

-spec serialize_hash(binary()) -> string().
serialize_hash(Hash) ->
    libp2p_crypto:bin_to_b58(Hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize_hash(string()) -> binary().
deserialize_hash(String) ->
    libp2p_crypto:b58_to_bin(String).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec bin_to_hex(binary()) -> string().
bin_to_hex(Bin) ->
  lists:flatten([[io_lib:format("~2.16.0b",[X]) || <<X:8>> <= Bin ]]).

-spec hex_to_bin(binary()) -> binary().
hex_to_bin(Hex) ->
  << begin {ok, [V], []} = io_lib:fread("~16u", [X, Y]), <<V:8/integer-little>> end || <<X:8/integer, Y:8/integer>> <= Hex >>.

pmap(F, L) ->
    Parent = self(),
    Width = application:get_env(blockchain, validation_width, 3),
    Len = length(L),
    case Len =< Width of
        true ->
            lists:map(F, L);
        false ->
            Ct = ceil(Len/Width),
            OL = [lists:sublist(L, 1 + Ct * N, Ct) || N <- lists:seq(0, Width - 1)],
            lists:foldl(
              fun([], N) ->
                      N;
                 (IL, N) ->
                      spawn(
                        fun() ->
                                Parent ! {pmap, N, lists:map(F, IL)}
                        end),
                      N+1
              end, 0, OL),
            L2 = [receive
                      {pmap, N, R} -> {N,R}
                  end || _ <- OL],
            {_, L3} = lists:unzip(lists:keysort(1, L2)),
            lists:flatten(L3)
    end.

addr2name(Addr) ->
    B58Addr = libp2p_crypto:bin_to_b58(Addr),
    {ok, N} = erl_angry_purple_tiger:animal_name(B58Addr),
    N.

-spec rand_state(Hash :: binary()) -> rand:state().
rand_state(Hash) ->
    <<A:85/integer-unsigned-little, B:85/integer-unsigned-little,
      C:86/integer-unsigned-little, _/binary>> = crypto:hash(sha256, Hash),
    rand:seed_s(exs1024s, {A, B, C}).

distance(L1, L1) ->
    %% Same location, defaulting the distance to 1m
    0.001;
distance(L1, L2) ->
    %% distance in kms
    case vincenty:distance(h3:to_geo(L1), h3:to_geo(L2)) of
        {error, _} ->
            %% An off chance that the points are antipodal and
            %% vincenty_distance fails to converge. In this case
            %% we default to some max distance we consider good enough
            %% for witnessing
            1000;
        {ok, D} ->
            D - hex_adjustment(L1) - hex_adjustment(L2)
    end.

hex_adjustment(Loc) ->
    %% Distance from hex center to edge, sqrt(3)*edge_length/2.
    Res = h3:get_resolution(Loc),
    EdgeLength = h3:edge_length_kilometers(Res),
    EdgeLength * (round(math:sqrt(3) * math:pow(10, 3)) / math:pow(10, 3)) / 2.

score_gateways(Ledger) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case blockchain_ledger_v1:mode(Ledger) of
        delayed ->
            %% Use the cache in delayed ledger mode
            e2qc:cache(gw_cache, {Height},
                       fun() ->
                               score_tagged_gateways(Height, Ledger)
                       end);
        active ->
            %% recalculate in active ledger mode
            score_tagged_gateways(Height, Ledger)
    end.

score_tagged_gateways(Height, Ledger) ->
    Gateways = blockchain_ledger_v1:active_gateways(Ledger),
    maps:map(fun(A, G) ->
                     {_, _, S} = blockchain_ledger_gateway_v2:score(A, G, Height, Ledger),
                     {G, S}
             end, Gateways).


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

serialize_deserialize_test() ->
    Hash = <<"123abc">>,
    ?assertEqual(Hash, deserialize_hash(serialize_hash(Hash))).

-endif.
