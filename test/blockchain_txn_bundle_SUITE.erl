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
    [{SrcAddr, {_, _, SrcSigFun}} | _] = ConsensusMembers,
    SrcBalance = balance(Chain, SrcAddr),
    SrcNonce = nonce(Chain, SrcAddr),

    %% Dst has no need for a starting balance, so we use an arbitrary address.
    DstAddr = gen_addr(), % dead-end address, since we'll never pay from it.
    DstBalance = balance(Chain, DstAddr),

    %% Expected initial values:
    ?assertEqual(0, SrcNonce),
    ?assertEqual(5000, SrcBalance),
    ?assertEqual(0, DstBalance),

    AmountPerTxn = 1000,
    Txn1 = pay_txn(SrcAddr, DstAddr, AmountPerTxn, SrcSigFun, SrcNonce + 1),
    Txn2 = pay_txn(SrcAddr, DstAddr, AmountPerTxn, SrcSigFun, SrcNonce + 2),
    Txns = [Txn1, Txn2],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),
    ?assertMatch(ok, chain_commit(Chain, ConsensusMembers, TxnBundle)),

    AmountTotal = length(Txns) * AmountPerTxn,
    ?assertEqual(SrcBalance - AmountTotal, balance(Chain, SrcAddr)),
    ?assertEqual(DstBalance + AmountTotal, balance(Chain, DstAddr)),

    ok.

negative_test(Cfg) ->
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% Src needs a starting balance, so we pick one from the consensus group.
    [{SrcAddr, {_, _, SrcSigFun}} | _] = ConsensusMembers,
    SrcBalance = balance(Chain, SrcAddr),
    SrcNonce = nonce(Chain, SrcAddr),

    %% Dst has no need for a starting balance, so we use an arbitrary address.
    DstAddr = gen_addr(), % dead-end address, since we'll never pay from it.
    DstBalance = balance(Chain, DstAddr),

    %% Expected initial values:
    ?assertEqual(0, SrcNonce),
    ?assertEqual(5000, SrcBalance),
    ?assertEqual(0, DstBalance),

    AmountPerTxn = 1000,
    Txn1 = pay_txn(SrcAddr, DstAddr, AmountPerTxn, SrcSigFun, SrcNonce + 1),
    Txn2 = pay_txn(SrcAddr, DstAddr, AmountPerTxn, SrcSigFun, SrcNonce + 2),
    Txns = [Txn2, Txn1], % Reversed order, invalidating the bundle.
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),
    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),

    %% Balances should not have changed since the bundle was invalid:
    ?assertEqual(SrcBalance, balance(Chain, SrcAddr)),
    ?assertEqual(DstBalance, balance(Chain, DstAddr)),

    ok.

double_spend_test(Cfg) ->
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% Src needs a starting balance, so we pick one from the consensus group.
    [{SrcAddr, {_, _, SrcSigFun}} | _] = ConsensusMembers,
    SrcBalance = balance(Chain, SrcAddr),
    SrcNonce = nonce(Chain, SrcAddr) + 1,

    %% A destination can be arbitrary, since it has no need for a starting balance.
    %% A destination can be dead-end (no priv key), since we'll never pay from it.
    Dst1Addr = gen_addr(),
    Dst2Addr = gen_addr(),
    Dst1Balance = balance(Chain, Dst1Addr),
    Dst2Balance = balance(Chain, Dst2Addr),

    %% Expected initial values:
    ?assertEqual(1, SrcNonce),
    ?assertEqual(5000, SrcBalance),
    ?assertEqual(0, Dst1Balance),
    ?assertEqual(0, Dst2Balance),

    AmountPerTxn = 1000,
    %% good txn: first spend
    Txn1 = pay_txn(SrcAddr, Dst1Addr, AmountPerTxn, SrcSigFun, SrcNonce),
    %% bad txn: double-spend = same nonce, diff dst
    Txn2 = pay_txn(SrcAddr, Dst2Addr, AmountPerTxn, SrcSigFun, SrcNonce),
    TxnBundle = blockchain_txn_bundle_v1:new([Txn1, Txn2]),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),

    %% All balances remain, since all txns were rejected, not just the bad one.
    ?assertEqual(SrcBalance, balance(Chain, SrcAddr)),
    ?assertEqual(Dst1Balance, balance(Chain, Dst1Addr)),
    ?assertEqual(Dst2Balance, balance(Chain, Dst2Addr)),

    ok.

successive_test(Cfg) ->
    %% Test a successive valid bundle payment
    %% A -> B
    %% B -> C
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    [{A_Addr, {_, _, A_SigFun}} | _] = ConsensusMembers,
    A_Balance = balance(Chain, A_Addr),
    A_Nonce = nonce(Chain, A_Addr),

    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {B_Addr, B_SigFun} = gen_creds(),
    B_Balance = balance(Chain, B_Addr),
    B_Nonce = nonce(Chain, B_Addr),

    {C_Addr, _} = gen_creds(),
    C_Balance = balance(Chain, C_Addr),

    %% Expected initial values:
    ?assertEqual(   0, A_Nonce),
    ?assertEqual(   0, B_Nonce),
    ?assertEqual(5000, A_Balance),
    ?assertEqual(   0, B_Balance),
    ?assertEqual(   0, C_Balance),

    AmountAToB = 5000,
    AmountBToC = 1000,
    TxnAToB = pay_txn(A_Addr, B_Addr, AmountAToB, A_SigFun, A_Nonce + 1),
    TxnBToC = pay_txn(B_Addr, C_Addr, AmountBToC, B_SigFun, B_Nonce + 1),
    Txns = [TxnAToB, TxnBToC],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(ok, chain_commit(Chain, ConsensusMembers, TxnBundle)),
    ?assertEqual(A_Balance - AmountAToB             , balance(Chain, A_Addr)),
    ?assertEqual(B_Balance + AmountAToB - AmountBToC, balance(Chain, B_Addr)),
    ?assertEqual(C_Balance + AmountBToC             , balance(Chain, C_Addr)),

    ok.

invalid_successive_test(Cfg) ->
    %% Test a successive invalid bundle payment
    %% A -> B
    %% B -> C <-- this is invalid
    ConsensusMembers = ?config(consensus_members, Cfg),
    Chain = ?config(chain, Cfg),

    %% A needs a starting balance, so we pick one from the consensus group.
    [{A_Addr, {_, _, A_SigFun}} | _] = ConsensusMembers,
    A_Balance = balance(Chain, A_Addr),
    A_Nonce = nonce(Chain, A_Addr),

    %% B and C can be arbitrary, since they have no need for a starting balance
    %% - all funds will originate from A.
    {B_Addr, B_SigFun} = gen_creds(),
    B_Balance = balance(Chain, B_Addr),
    B_Nonce = nonce(Chain, B_Addr),

    {C_Addr, _} = gen_creds(),
    C_Balance = balance(Chain, C_Addr),

    %% Expected initial values:
    ?assertEqual(   0, A_Nonce),
    ?assertEqual(   0, B_Nonce),
    ?assertEqual(5000, A_Balance),
    ?assertEqual(   0, B_Balance),
    ?assertEqual(   0, C_Balance),

    AmountAToB = A_Balance,
    AmountBToC = B_Balance + AmountAToB + 1, % over limit
    TxnAToB = pay_txn(A_Addr, B_Addr, AmountAToB, A_SigFun, A_Nonce + 1),
    TxnBToC = pay_txn(B_Addr, C_Addr, AmountBToC, B_SigFun, B_Nonce + 1),
    Txns = [TxnAToB, TxnBToC],
    TxnBundle = blockchain_txn_bundle_v1:new(Txns),

    ?assertMatch(
        {error, {invalid_txns, [{_, invalid_bundled_txns}]}},
        chain_commit(Chain, ConsensusMembers, TxnBundle)
    ),
    ?assertEqual(A_Balance, balance(Chain, A_Addr)),
    ?assertEqual(B_Balance, balance(Chain, B_Addr)),
    ?assertEqual(C_Balance, balance(Chain, C_Addr)),

    ok.

single_payer_test(Config) ->
    %% Test a bundled payment from single payer
    %% A -> B 2000
    %% A -> C 3000
    Miners = ?config(miners, Config),
    [MinerA, MinerB, MinerC | _Tail] = Miners,
    MinerAPubkeyBin = ct_rpc:call(MinerA, blockchain_swarm, pubkey_bin, []),
    MinerBPubkeyBin = ct_rpc:call(MinerB, blockchain_swarm, pubkey_bin, []),
    MinerCPubkeyBin = ct_rpc:call(MinerC, blockchain_swarm, pubkey_bin, []),

    %% check initial balances
    5000 = miner_ct_utils:get_balance(MinerA, MinerAPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerB, MinerBPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerC, MinerCPubkeyBin),

    %% Create first payment txn from A -> B
    TxnAToB = ct_rpc:call(MinerA, blockchain_txn_payment_v1, new, [MinerAPubkeyBin, MinerBPubkeyBin, 2000, 1]),
    {ok, _PubkeyA, SigFunA, _ECDHFunA} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),
    SignedTxnAToB = ct_rpc:call(MinerA, blockchain_txn_payment_v1, sign, [TxnAToB, SigFunA]),
    ct:pal("SignedTxnAToB: ~p", [SignedTxnAToB]),

    %% Create second payment txn from B -> C
    TxnAToC = ct_rpc:call(MinerA, blockchain_txn_payment_v1, new, [MinerAPubkeyBin, MinerCPubkeyBin, 3000, 2]),
    {ok, _PubkeyA, SigFunA, _ECDHFunA} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),
    SignedTxnAToC = ct_rpc:call(MinerA, blockchain_txn_payment_v1, sign, [TxnAToC, SigFunA]),
    ct:pal("SignedTxnAToC: ~p", [SignedTxnAToC]),

    %% Create bundle with txns
    BundleTxn = ct_rpc:call(MinerA, blockchain_txn_bundle_v1, new, [[SignedTxnAToB, SignedTxnAToC]]),
    ct:pal("BundleTxn: ~p", [BundleTxn]),

    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleTxn, Miners),

    %% wait till height is 15, ideally should wait till the payment actually occurs
    %% it should be plenty fast regardless
    ok = miner_ct_utils:wait_for_gte(height, Miners, 15),

    %% Expectation is that the payments should go through
    0 = miner_ct_utils:get_balance(MinerA, MinerAPubkeyBin),
    7000 = miner_ct_utils:get_balance(MinerB, MinerBPubkeyBin),
    8000 = miner_ct_utils:get_balance(MinerC, MinerCPubkeyBin),

    ok.

single_payer_invalid_test(Config) ->
    %% Test a bundled payment from single payer
    %% A -> B 2000
    %% A -> C 4000
    Miners = ?config(miners, Config),
    [MinerA, MinerB, MinerC | _Tail] = Miners,
    MinerAPubkeyBin = ct_rpc:call(MinerA, blockchain_swarm, pubkey_bin, []),
    MinerBPubkeyBin = ct_rpc:call(MinerB, blockchain_swarm, pubkey_bin, []),
    MinerCPubkeyBin = ct_rpc:call(MinerC, blockchain_swarm, pubkey_bin, []),

    %% check initial balances
    5000 = miner_ct_utils:get_balance(MinerA, MinerAPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerB, MinerBPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerC, MinerCPubkeyBin),

    %% Create first payment txn from A -> B
    TxnAToB = ct_rpc:call(MinerA, blockchain_txn_payment_v1, new, [MinerAPubkeyBin, MinerBPubkeyBin, 2000, 1]),
    {ok, _PubkeyA, SigFunA, _ECDHFunA} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),
    SignedTxnAToB = ct_rpc:call(MinerA, blockchain_txn_payment_v1, sign, [TxnAToB, SigFunA]),
    ct:pal("SignedTxnAToB: ~p", [SignedTxnAToB]),

    %% Create second payment txn from B -> C
    TxnAToC = ct_rpc:call(MinerA, blockchain_txn_payment_v1, new, [MinerAPubkeyBin, MinerCPubkeyBin, 4000, 2]),
    {ok, _PubkeyA, SigFunA, _ECDHFunA} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),
    SignedTxnAToC = ct_rpc:call(MinerA, blockchain_txn_payment_v1, sign, [TxnAToC, SigFunA]),
    ct:pal("SignedTxnAToC: ~p", [SignedTxnAToC]),

    %% Create bundle with txns
    BundleTxn = ct_rpc:call(MinerA, blockchain_txn_bundle_v1, new, [[SignedTxnAToB, SignedTxnAToC]]),
    ct:pal("BundleTxn: ~p", [BundleTxn]),

    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleTxn, Miners),

    %% wait till height is 15, ideally should wait till the payment actually occurs
    %% it should be plenty fast regardless
    ok = miner_ct_utils:wait_for_gte(height, Miners, 15),

    %% Expectation is that the payments should not go through
    %% because A is trying to over-spend
    5000 = miner_ct_utils:get_balance(MinerA, MinerAPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerB, MinerBPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerC, MinerCPubkeyBin),

    ok.

full_circle_test(Config) ->
    %% Test a successive valid bundle payment
    %% A -> B -> C
    %% A -> B 5000
    %% B -> C 10000
    Miners = ?config(miners, Config),
    [MinerA, MinerB, MinerC | _Tail] = Miners,
    MinerAPubkeyBin = ct_rpc:call(MinerA, blockchain_swarm, pubkey_bin, []),
    MinerBPubkeyBin = ct_rpc:call(MinerB, blockchain_swarm, pubkey_bin, []),
    MinerCPubkeyBin = ct_rpc:call(MinerC, blockchain_swarm, pubkey_bin, []),

    %% check initial balances
    5000 = miner_ct_utils:get_balance(MinerA, MinerAPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerB, MinerBPubkeyBin),
    5000 = miner_ct_utils:get_balance(MinerC, MinerCPubkeyBin),

    %% Create first payment txn from A -> B
    TxnAToB = ct_rpc:call(MinerA, blockchain_txn_payment_v1, new, [MinerAPubkeyBin, MinerBPubkeyBin, 5000, 1]),
    {ok, _PubkeyA, SigFunA, _ECDHFunA} = ct_rpc:call(MinerA, blockchain_swarm, keys, []),
    SignedTxnAToB = ct_rpc:call(MinerA, blockchain_txn_payment_v1, sign, [TxnAToB, SigFunA]),
    ct:pal("SignedTxnAToB: ~p", [SignedTxnAToB]),

    %% Create second payment txn from B -> C
    TxnBToC = ct_rpc:call(MinerB, blockchain_txn_payment_v1, new, [MinerBPubkeyBin, MinerCPubkeyBin, 10000, 1]),
    {ok, _PubkeyB, SigFunB, _ECDHFunB} = ct_rpc:call(MinerB, blockchain_swarm, keys, []),
    SignedTxnBToC = ct_rpc:call(MinerB, blockchain_txn_payment_v1, sign, [TxnBToC, SigFunB]),
    ct:pal("SignedTxnBToC: ~p", [SignedTxnBToC]),

    %% Create third payment txn from C -> A
    TxnCToA = ct_rpc:call(MinerB, blockchain_txn_payment_v1, new, [MinerCPubkeyBin, MinerAPubkeyBin, 15000, 1]),
    {ok, _PubkeyC, SigFunC, _ECDHFunC} = ct_rpc:call(MinerC, blockchain_swarm, keys, []),
    SignedTxnCToA = ct_rpc:call(MinerC, blockchain_txn_payment_v1, sign, [TxnCToA, SigFunC]),
    ct:pal("SignedTxnCToA: ~p", [SignedTxnCToA]),

    %% Create bundle with txns
    BundleTxn = ct_rpc:call(MinerA, blockchain_txn_bundle_v1, new, [[SignedTxnAToB,
                                                                     SignedTxnBToC,
                                                                     SignedTxnCToA
                                                                    ]]),
    ct:pal("BundleTxn: ~p", [BundleTxn]),

    %% Submit the bundle txn
    miner_ct_utils:submit_txn(BundleTxn, Miners),

    %% wait till height is 15, ideally should wait till the payment actually occurs
    %% it should be plenty fast regardless
    ok = miner_ct_utils:wait_for_gte(height, Miners, 15),

    %% Expectation is that the full payment circle should complete
    15000 = miner_ct_utils:get_balance(MinerA, MinerAPubkeyBin),
    0 = miner_ct_utils:get_balance(MinerB, MinerBPubkeyBin),
    0 = miner_ct_utils:get_balance(MinerC, MinerCPubkeyBin),

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

pay_txn(Src, Dst, Amount, SrcSigFun, Nonce) ->
    Txn = blockchain_txn_payment_v1:new(Src, Dst, Amount, Nonce),
    blockchain_txn_payment_v1:sign(Txn, SrcSigFun).

-spec gen_creds() -> {binary(), fun((binary()) -> binary())}.
gen_creds() ->
    #{public := Pub, secret := Priv} = libp2p_crypto:generate_keys(ecc_compact),
    Addr = libp2p_crypto:pubkey_to_bin(Pub),
    SigFun = libp2p_crypto:mk_sig_fun(Priv),
    {Addr, SigFun}.

-spec gen_addr() -> binary().
gen_addr() ->
    #{public := Pub} = libp2p_crypto:generate_keys(ecc_compact),
    libp2p_crypto:pubkey_to_bin(Pub).

balance(Chain, <<Addr/binary>>) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(Addr, Ledger) of
        {error, address_entry_not_found} ->
            0;
        {ok, Entry} ->
            blockchain_ledger_entry_v1:balance(Entry)
    end.

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

-spec nonce(blockchain:blockchain(), binary()) -> integer().
nonce(Chain, Addr) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:find_entry(Addr, Ledger) of
        {error, address_entry_not_found} ->
            0;
        {ok, Entry} ->
            blockchain_ledger_entry_v1:nonce(Entry)
    end.
