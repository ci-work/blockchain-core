-module(blockchain_txn_bundle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("blockchain/include/blockchain_vars.hrl").

-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         basic_test/1,
         negative_test/1,
         double_spend_test/1,
         successive_test/1,
         invalid_successive_test/1,
         single_payer_test/1,
         single_payer_invalid_test/1,
         full_circle_test/1,
         add_assert_test/1,
         invalid_add_assert_test/1,
         single_txn_bundle_test/1,
         bundleception_test/1
        ]).

%% Setup ----------------------------------------------------------------------

all() -> [
          basic_test,
          negative_test,
          double_spend_test,
          successive_test,
          invalid_successive_test,
          single_payer_test,
          single_payer_invalid_test,
          full_circle_test,
          add_assert_test,
          invalid_add_assert_test,
          single_txn_bundle_test,
          bundleception_test
         ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(TestCase, Cfg0) ->
    Cfg1 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Cfg0),
    Dir = ?config(base_dir, Cfg1),
    StartingBalance = 5000,
    {ok, Sup, Keys={_, _}, _Opts} = test_utils:init(Dir),
    {
        ok,
        _GenesisMembers,
        _GenesisBlock,
        ConsensusMembers0,
        {master_key, MasterKey={_, _}}
    } =
        test_utils:init_chain(
            StartingBalance,
            Keys,
            true,
            #{
                %% Setting vars_commit_delay to 1 is crucial,
                %% otherwise var changes will not take effect.
                ?vars_commit_delay => 1
            }
        ),
    %% Shuffling to discourage reliance on order.
    ConsensusMembers = blockchain_ct_utils:shuffle(ConsensusMembers0),
    N = length(ConsensusMembers),
    ?assert(N > 0, N),
    ?assertEqual(7, N),
    Chain = blockchain_worker:blockchain(), % TODO Return from init_chain instead
    [
        {sup, Sup},
        {master_key, MasterKey},
        {consensus_members, ConsensusMembers},
        {chain, Chain}
    |
        Cfg1
    ].

end_per_testcase(_TestCase, Cfg) ->
    Sup = ?config(sup, Cfg),
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok.

%% Cases ----------------------------------------------------------------------

basic_test(Cfg) ->
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% Src needs a starting balance, so we pick one from the consensus group.
    %% Dst has no need for a starting balance, so we use an arbitrary address.
    {Src, _} = user_pick_from_cg(ConsensusMembers),
    Dst = user_new(),

    SrcBalance0 = user_balance(Chain, Src),
    DstBalance0 = user_balance(Chain, Dst),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, DstBalance0),

    AmountPerTxn = 1000,
    Txns =
        [
            user_pay(Src, Dst, AmountPerTxn, 1),
            user_pay(Src, Dst, AmountPerTxn, 2)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),
    ?assertMatch(ok, chain_commit(Chain, ConsensusMembers, TxnBundle)),

    AmountTotal = length(Txns) * AmountPerTxn,
    ?assertEqual(SrcBalance0 - AmountTotal, user_balance(Chain, Src)),
    ?assertEqual(DstBalance0 + AmountTotal, user_balance(Chain, Dst)),

    ok.

negative_test(Cfg) ->
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% Src needs a starting balance, so we pick one from the consensus group.
    {Src, _} = user_pick_from_cg(ConsensusMembers),
    SrcBalance0 = user_balance(Chain, Src),

    %% Dst has no need for a starting balance, so we use an arbitrary address.
    Dst = user_new(),
    DstBalance0 = user_balance(Chain, Dst),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, DstBalance0),

    AmountPerTxn = 1000,
    Txns =
        %% Reversed order of nonces, invalidating the bundle:
        [
            user_pay(Src, Dst, AmountPerTxn, 2),
            user_pay(Src, Dst, AmountPerTxn, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),

    %% Balances should not have changed since the bundle was invalid:
    ?assertEqual(SrcBalance0, user_balance(Chain, Src)),
    ?assertEqual(DstBalance0, user_balance(Chain, Dst)),

    ok.

double_spend_test(Cfg) ->
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% Src needs a starting balance, so we pick one from the consensus group.
    {Src, _} = user_pick_from_cg(ConsensusMembers),
    SrcBalance0 = user_balance(Chain, Src),
    SrcNonce = 1,

    %% A destination can be arbitrary, since it has no need for a starting balance.
    %% A destination can be dead-end (no priv key), since we'll never pay from it.
    Dst1 = user_new(),
    Dst2 = user_new(),
    Dst1Balance0 = user_balance(Chain, Dst1),
    Dst2Balance0 = user_balance(Chain, Dst2),

    %% Expected initial balances:
    ?assertEqual(5000, SrcBalance0),
    ?assertEqual(   0, Dst1Balance0),
    ?assertEqual(   0, Dst2Balance0),

    AmountPerTxn = 1000,
    Txns =
        [
            %% good txn: first spend
            user_pay(Src, Dst1, AmountPerTxn, SrcNonce),

            %% bad txn: double-spend = same nonce, diff dst
            user_pay(Src, Dst2, AmountPerTxn, SrcNonce)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),

    %% All balances remain, since all txns were rejected, not just the bad one.
    ?assertEqual(SrcBalance0 , user_balance(Chain, Src)),
    ?assertEqual(Dst1Balance0, user_balance(Chain, Dst1)),
    ?assertEqual(Dst2Balance0, user_balance(Chain, Dst2)),

    ok.

successive_test(Cfg) ->
    %% Test a successive valid bundle payment
    %% A -> B
    %% B -> C
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {A, _} = user_pick_from_cg(ConsensusMembers),
    B = user_new(),
    C = user_new(),

    A_Balance0 = user_balance(Chain, A),
    B_Balance0 = user_balance(Chain, B),
    C_Balance0 = user_balance(Chain, C),

    %% Expected initial balances:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = A_Balance0,
    AmountBToC = AmountAToB - 1,
    Txns =
        [
            user_pay(A, B, AmountAToB, 1),
            user_pay(B, C, AmountBToC, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, chain_commit(Chain, ConsensusMembers, TxnBundle)),
    ?assertEqual(A_Balance0 - AmountAToB             , user_balance(Chain, A)),
    ?assertEqual(B_Balance0 + AmountAToB - AmountBToC, user_balance(Chain, B)),
    ?assertEqual(C_Balance0 + AmountBToC             , user_balance(Chain, C)),

    ok.

invalid_successive_test(Cfg) ->
    %% Test a successive invalid bundle payment
    %% A -> B
    %% B -> C <-- this is invalid
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {A, _} = user_pick_from_cg(ConsensusMembers),
    B = user_new(),
    C = user_new(),

    A_Balance0 = user_balance(Chain, A),
    B_Balance0 = user_balance(Chain, B),
    C_Balance0 = user_balance(Chain, C),

    %% Expected initial balances:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = A_Balance0,
    AmountBToC = B_Balance0 + AmountAToB + 1,  % overdraw attempt
    Txns =
        [
            user_pay(A, B, AmountAToB, 1),
            user_pay(B, C, AmountBToC, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),
    ?assertEqual(A_Balance0, user_balance(Chain, A)),
    ?assertEqual(B_Balance0, user_balance(Chain, B)),
    ?assertEqual(C_Balance0, user_balance(Chain, C)),

    ok.

single_payer_test(Cfg) ->
    %% Test a bundled payment from single payer
    %% A -> B
    %% A -> C
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {A, _} = user_pick_from_cg(ConsensusMembers),
    B = user_new(),
    C = user_new(),

    A_Balance0 = user_balance(Chain, A),
    B_Balance0 = user_balance(Chain, B),
    C_Balance0 = user_balance(Chain, C),

    %% Expected initial values:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = 2000,
    AmountAToC = 3000,
    Txns =
        [
            user_pay(A, B, AmountAToB, 1),
            user_pay(A, C, AmountAToC, 2)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, chain_commit(Chain, ConsensusMembers, TxnBundle)),
    ?assertEqual(A_Balance0 - AmountAToB - AmountAToC, user_balance(Chain, A)),
    ?assertEqual(B_Balance0 + AmountAToB             , user_balance(Chain, B)),
    ?assertEqual(C_Balance0 + AmountAToC             , user_balance(Chain, C)),

    ok.

single_payer_invalid_test(Cfg) ->
    %% Test a bundled payment from single payer
    %% Given:
    %%   N < (K + M)
    %%   A: N
    %%   B: 0
    %% Attempt:
    %%   A -M-> B : OK
    %%   A -K-> C : insufficient funds
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {A, _} = user_pick_from_cg(ConsensusMembers),
    B = user_new(),
    C = user_new(),

    A_Balance0 = user_balance(Chain, A),
    B_Balance0 = user_balance(Chain, B),
    C_Balance0 = user_balance(Chain, C),

    %% Expected initial values:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    Overage = 1000,
    AmountAToB = 2000,
    AmountAToC = (A_Balance0 - AmountAToB) + Overage,

    % Sanity checks
    ?assert(A_Balance0 >= AmountAToB),
    ?assert(A_Balance0 <  (AmountAToB + AmountAToC)),

    Txns =
        [
            user_pay(A, B, AmountAToB, 1),
            user_pay(A, C, AmountAToC, 2)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),

    %% Because of one invalid txn (A->C), the whole bundle was rejected, so
    %% nothing changed:
    ?assertEqual(A_Balance0, user_balance(Chain, A)),
    ?assertEqual(B_Balance0, user_balance(Chain, B)),
    ?assertEqual(C_Balance0, user_balance(Chain, C)),

    ok.

full_circle_test(Cfg) ->
    %% Test a full-circle transfer of funds:
    %% Given:
    %%   A: NA
    %%   B: NB
    %%   C: NC
    %% Attempt:
    %%   A -(NA      )-> B
    %%   B -(NA+NB   )-> C
    %%   C -(NA+NB+NC)-> A
    %% Expect:
    %%   A: NA + NB + NC
    %%   B: 0
    %%   C: 0
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {A, _} = user_pick_from_cg(ConsensusMembers),
    B = user_new(),
    C = user_new(),

    A_Balance0 = user_balance(Chain, A),
    B_Balance0 = user_balance(Chain, B),
    C_Balance0 = user_balance(Chain, C),

    %% Expected initial values:
    ?assertEqual(5000, A_Balance0),
    ?assertEqual(   0, B_Balance0),
    ?assertEqual(   0, C_Balance0),

    AmountAToB = A_Balance0,
    AmountBToC = A_Balance0 + B_Balance0,
    AmountCToA = A_Balance0 + B_Balance0 + C_Balance0,
    BalanceExpectedA = AmountCToA,
    BalanceExpectedB = 0,
    BalanceExpectedC = 0,

    Txns =
        [
            user_pay(A, B, AmountAToB, 1),
            user_pay(B, C, AmountBToC, 1),
            user_pay(C, A, AmountCToA, 1)
        ],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, chain_commit(Chain, ConsensusMembers, TxnBundle)),
    ?assertEqual(BalanceExpectedA, user_balance(Chain, A)),
    ?assertEqual(BalanceExpectedB, user_balance(Chain, B)),
    ?assertEqual(BalanceExpectedC, user_balance(Chain, C)),

    ok.

add_assert_test(Config) ->
    %% Test add + assert in a bundled txn
    %% A -> [add_gateway, assert_location]
    Miners = ?config(miners, Config),
    [MinerA | _Tail] = Miners,
    MinerAPubkeyBin = ct_rpc:call(MinerA, blockchain_swarm, pubkey_bin, []),

    {ok, _OwnerPubkey, OwnerSigFun, _OwnerECDHFun} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),

    %% Create add_gateway txn
    [{GatewayPubkeyBin, {_GatewayPubkey, _GatewayPrivkey, GatewaySigFun}}] = miner_ct_utils:generate_keys(1),
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(MinerAPubkeyBin, GatewayPubkeyBin),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedAddGatewayTxn = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
    ct:pal("SignedAddGatewayTxn: ~p", [SignedAddGatewayTxn]),

    %% Create assert loc txn
    Index = 631210968910285823,
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(GatewayPubkeyBin, MinerAPubkeyBin, Index, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTxn = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),
    ct:pal("SignedAssertLocationTxn: ~p", [SignedAssertLocationTxn]),

    %% Create bundle with txns
    BundleTxn = ct_rpc:call(MinerA, blockchain_txn_bundle_v1, new, [[SignedAddGatewayTxn, SignedAssertLocationTxn]]),
    ct:pal("BundleTxn: ~p", [BundleTxn]),

    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleTxn, Miners),

    %% wait till height 20, should be long enough I believe
    ok = miner_ct_utils:wait_for_gte(height, Miners, 20),

    %% Get active gateways
    Chain = ct_rpc:call(MinerA, blockchain_worker, blockchain, []),
    Ledger = ct_rpc:call(MinerA, blockchain, ledger, [Chain]),
    ActiveGateways = ct_rpc:call(MinerA, blockchain_ledger_v1, active_gateways, [Ledger]),

    %% Check that the gateway got added
    9 = maps:size(ActiveGateways),

    %% Check that it has the correct location
    AddedGw = maps:get(GatewayPubkeyBin, ActiveGateways),
    GwLoc = blockchain_ledger_gateway_v2:location(AddedGw),
    ?assertEqual(GwLoc, Index),

    ok.

invalid_add_assert_test(Config) ->
    %% Test add + assert in a bundled txn
    %% A -> [add_gateway, assert_location]
    Miners = ?config(miners, Config),
    [MinerA | _Tail] = Miners,
    MinerAPubkeyBin = ct_rpc:call(MinerA, blockchain_swarm, pubkey_bin, []),

    {ok, _OwnerPubkey, OwnerSigFun, _OwnerECDHFun} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),

    %% Create add_gateway txn
    [{GatewayPubkeyBin, {_GatewayPubkey, _GatewayPrivkey, GatewaySigFun}}] = miner_ct_utils:generate_keys(1),
    AddGatewayTx = blockchain_txn_add_gateway_v1:new(MinerAPubkeyBin, GatewayPubkeyBin),
    SignedOwnerAddGatewayTx = blockchain_txn_add_gateway_v1:sign(AddGatewayTx, OwnerSigFun),
    SignedAddGatewayTxn = blockchain_txn_add_gateway_v1:sign_request(SignedOwnerAddGatewayTx, GatewaySigFun),
    ct:pal("SignedAddGatewayTxn: ~p", [SignedAddGatewayTxn]),

    %% Create assert loc txn
    Index = 631210968910285823,
    AssertLocationRequestTx = blockchain_txn_assert_location_v1:new(GatewayPubkeyBin, MinerAPubkeyBin, Index, 1),
    PartialAssertLocationTxn = blockchain_txn_assert_location_v1:sign_request(AssertLocationRequestTx, GatewaySigFun),
    SignedAssertLocationTxn = blockchain_txn_assert_location_v1:sign(PartialAssertLocationTxn, OwnerSigFun),
    ct:pal("SignedAssertLocationTxn: ~p", [SignedAssertLocationTxn]),

    %% Create bundle with txns in bad order
    BundleTxn = ct_rpc:call(MinerA, blockchain_txn_bundle_v1, new, [[SignedAssertLocationTxn, SignedAddGatewayTxn]]),
    ct:pal("BundleTxn: ~p", [BundleTxn]),

    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleTxn, Miners),

    %% wait till height 20, should be long enough I believe
    ok = miner_ct_utils:wait_for_gte(height, Miners, 20),

    %% Get active gateways
    Chain = ct_rpc:call(MinerA, blockchain_worker, blockchain, []),
    Ledger = ct_rpc:call(MinerA, blockchain, ledger, [Chain]),
    ActiveGateways = ct_rpc:call(MinerA, blockchain_ledger_v1, active_gateways, [Ledger]),

    %% Check that the gateway did not get added
    8 = maps:size(ActiveGateways),

    ok.

single_txn_bundle_test(Config) ->
    Miners = ?config(miners, Config),
    [Src, Dst | _Tail] = Miners,
    SrcAddr = ct_rpc:call(Src, blockchain_swarm, pubkey_bin, []),
    DstAddr = ct_rpc:call(Dst, blockchain_swarm, pubkey_bin, []),

    %% check initial balances
    5000 = miner_ct_utils:get_balance(Src, SrcAddr),
    5000 = miner_ct_utils:get_balance(Dst, DstAddr),

    %% Create first payment txn
    Txn1 = ct_rpc:call(Src, blockchain_txn_payment_v1, new, [SrcAddr, DstAddr, 1000, 1]),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Src, blockchain_swarm, keys, []),
    SignedTxn1 = ct_rpc:call(Src, blockchain_txn_payment_v1, sign, [Txn1, SigFun]),
    ct:pal("SignedTxn1: ~p", [SignedTxn1]),

    %% Create bundle
    BundleTxn = ct_rpc:call(Src, blockchain_txn_bundle_v1, new, [[SignedTxn1]]),
    ct:pal("BundleTxn: ~p", [BundleTxn]),
    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleTxn, Miners),
    %% wait till height is 15, ideally should wait till the payment actually occurs
    %% it should be plenty fast regardless
    ok = miner_ct_utils:wait_for_gte(height, Miners, 15),

    %% The bundle is invalid since it does not contain atleast two txns in it
    5000 = miner_ct_utils:get_balance(Src, SrcAddr),
    5000 = miner_ct_utils:get_balance(Dst, DstAddr),

    ok.

bundleception_test(Config) ->
    Miners = ?config(miners, Config),
    [Src, Dst | _Tail] = Miners,
    SrcAddr = ct_rpc:call(Src, blockchain_swarm, pubkey_bin, []),
    DstAddr = ct_rpc:call(Dst, blockchain_swarm, pubkey_bin, []),

    %% check initial balances
    5000 = miner_ct_utils:get_balance(Src, SrcAddr),
    5000 = miner_ct_utils:get_balance(Dst, DstAddr),

    %% Src Sigfun
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Src, blockchain_swarm, keys, []),

    %% --------------------------------------------------------------
    %% Bundle 1 contents
    %% Create first payment txn
    Txn1 = ct_rpc:call(Src, blockchain_txn_payment_v1, new, [SrcAddr, DstAddr, 1000, 1]),
    SignedTxn1 = ct_rpc:call(Src, blockchain_txn_payment_v1, sign, [Txn1, SigFun]),
    ct:pal("SignedTxn1: ~p", [SignedTxn1]),

    %% Create second payment txn
    Txn2 = ct_rpc:call(Src, blockchain_txn_payment_v1, new, [SrcAddr, DstAddr, 1000, 2]),
    SignedTxn2 = ct_rpc:call(Src, blockchain_txn_payment_v1, sign, [Txn2, SigFun]),
    ct:pal("SignedTxn2: ~p", [SignedTxn2]),

    %% Create bundle
    BundleTxn1 = ct_rpc:call(Src, blockchain_txn_bundle_v1, new, [[SignedTxn1, SignedTxn2]]),
    ct:pal("BundleTxn1: ~p", [BundleTxn1]),
    %% --------------------------------------------------------------

    %% --------------------------------------------------------------
    %% Bundle 2 contents
    %% Create third payment txn
    Txn3 = ct_rpc:call(Src, blockchain_txn_payment_v1, new, [SrcAddr, DstAddr, 1000, 3]),
    SignedTxn3 = ct_rpc:call(Src, blockchain_txn_payment_v1, sign, [Txn3, SigFun]),
    ct:pal("SignedTxn3: ~p", [SignedTxn3]),

    %% Create fourth payment txn
    Txn4 = ct_rpc:call(Src, blockchain_txn_payment_v1, new, [SrcAddr, DstAddr, 1000, 4]),
    SignedTxn4 = ct_rpc:call(Src, blockchain_txn_payment_v1, sign, [Txn4, SigFun]),
    ct:pal("SignedTxn4: ~p", [SignedTxn4]),

    %% Create bundle
    BundleTxn2 = ct_rpc:call(Src, blockchain_txn_bundle_v1, new, [[SignedTxn3, SignedTxn4]]),
    ct:pal("BundleTxn2: ~p", [BundleTxn2]),
    %% --------------------------------------------------------------


    %% Do bundleception
    BundleInBundleTxn = ct_rpc:call(Src, blockchain_txn_bundle_v1, new, [[BundleTxn1, BundleTxn2]]),
    ct:pal("BundleInBundleTxn: ~p", [BundleInBundleTxn]),

    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleInBundleTxn, Miners),

    %% wait till height is 15, ideally should wait till the payment actually occurs
    %% it should be plenty fast regardless
    ok = miner_ct_utils:wait_for_gte(height, Miners, 15),

    %% Balances should not have changed
    5000 = miner_ct_utils:get_balance(Src, SrcAddr),
    5000 = miner_ct_utils:get_balance(Dst, DstAddr),

    ok.

%% Helpers --------------------------------------------------------------------

-type user() :: % Should be opaque when moved to a module.
    {Addr :: binary(), SigFun :: fun((binary()) -> binary())}.

-type consensus_member() ::
    {
        Addr :: binary(),
        {
            Pub  :: libp2p_crypto:pubkey(),
            Priv :: libp2p_crypto:privkey(),
            Sign :: fun((binary()) -> binary())
        }
    }.

-type consensus_group() ::
    [consensus_member()].

-spec user_new() -> user().
user_new() ->
    #{public := Pub, secret := Priv} = libp2p_crypto:generate_keys(ecc_compact),
    Addr = libp2p_crypto:pubkey_to_bin(Pub),
    SigFun = libp2p_crypto:mk_sig_fun(Priv),
    ?assert(is_binary(Addr)),
    ?assert(is_function(SigFun)),
    {Addr, SigFun}.

-spec user_addr(user()) -> binary().
user_addr({Addr, SigFun}) ->
    ?assert(is_binary(Addr)),
    ?assert(is_function(SigFun)),
    Addr.

-spec user_sig_fun(user()) -> fun((binary()) -> binary()).
user_sig_fun({Addr, SigFun}) ->
    ?assert(is_binary(Addr)),
    ?assert(is_function(SigFun)),
    SigFun.

-spec user_pick_from_cg(consensus_group()) -> {user(), consensus_group()}.
user_pick_from_cg([{Addr, {_, _, SigFun}} | CG]) ->
    ?assert(is_binary(Addr)),
    ?assert(is_function(SigFun)),
    {{Addr, SigFun}, CG}.

-spec user_balance(blockchain:blockchain(), user()) ->
    non_neg_integer().
user_balance(Chain, User) ->
    Addr = user_addr(User),
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(Addr, Ledger) of
        {error, address_entry_not_found} ->
            0;
        {ok, Entry} ->
            blockchain_ledger_entry_v1:balance(Entry)
    end.

-spec user_pay(user(), user(), non_neg_integer(), non_neg_integer()) ->
    Txn :: term(). % TODO Txn type
user_pay(Src, Dst, Amount, Nonce) ->
    Txn = blockchain_txn_payment_v1:new(user_addr(Src), user_addr(Dst), Amount, Nonce),
    blockchain_txn_payment_v1:sign(Txn, user_sig_fun(Src)).

-spec chain_commit(blockchain:blockchain(), [{_, {_, _, _}}], _) ->
    ok | {error, _}.
chain_commit(Chain, ConsensusMembers, Txn) ->
    case test_utils:create_block(ConsensusMembers, [Txn]) of
        {ok, Block} ->
            {ok, Height0} = blockchain:height(Chain),
            ok = blockchain:add_block(Block, Chain),
            {ok, Height1} = blockchain:height(Chain),
            ?assertEqual(1 + Height0, Height1),
            ok;
        {error, _}=Err ->
            Err
    end.
