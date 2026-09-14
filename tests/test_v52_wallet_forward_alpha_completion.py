from __future__ import annotations

from datetime import datetime, timedelta, timezone

from solana_roi.observation_store import ObservationEventStore
from solana_roi.v52_wallet_forward_alpha import (
    GraduationBuyerEvidence,
    ReplayComparisonObservation,
    STATUS_INCOMPLETE,
    STATUS_MATERIAL,
    VALIDATION_WINDOWS,
    WalletForwardAlphaEngine,
    WalletForwardOutcome,
    WalletIntegritySnapshot,
    WalletPointInTimeObservation,
    wallet_forward_shadow_profile,
)
from solana_roi import v52_wallet_forward_alpha_integration as integration


def _observation(wallet, context, candidate, detected):
    return WalletPointInTimeObservation(
        wallet=wallet, context_key=context, candidate_id=candidate,
        token_mint=f"mint-{candidate}", transaction_signature=f"sig-{candidate}",
        chain_timestamp=detected-timedelta(seconds=1),
        first_observable_at=detected-timedelta(milliseconds=500), detected_at=detected,
        lifecycle="bonding_curve", graduation_state="pre_graduation",
        observed_price=1.0, earliest_executable_price=1.01, liquidity_usd=10_000.0,
        slippage_fraction=.005, fee_fraction=.002, market_impact_fraction=.003,
        max_executable_usd=2_000.0, entity_id=wallet,
        relationships={"known_at_detection": True},
        integrity_known={"source": "point_in_time"},
        wallet_statistics_known={"future_outcomes_included": False},
        v52_candidate_state="eligible", v52_decision_state="pre_entry", could_enter=True,
    )


def _outcome(wallet, context, candidate, detected, alpha=.03):
    control=.01; net=control+alpha
    return WalletForwardOutcome(
        wallet=wallet, context_key=context, candidate_id=candidate, horizon="60s",
        available_at=detected+timedelta(seconds=60), gross_return=net+.01,
        net_executable_return=net, matched_control_net_return=control,
        exit_price=1.05, exit_liquidity_usd=10_000.0, exit_capacity_usd=2_000.0,
        max_favorable_excursion=.10, max_adverse_excursion=.02, copyable=True,
    )


def _mature(engine, wallet, context, start, n=60):
    assert engine.record_integrity(WalletIntegritySnapshot(wallet,start,1.0,False))
    last=start
    for i in range(n):
        detected=start+timedelta(seconds=i*400); last=detected
        assert engine.record_observation(_observation(wallet,context,f"c-{i}",detected))
        assert engine.record_forward_outcome(_outcome(wallet,context,f"c-{i}",detected))
    return last+timedelta(seconds=61)


def _material_rows():
    return [
        ReplayComparisonObservation(window,f"{window}-{i}",0.0,.005,.020,.10,.10,.10,True,True)
        for window in VALIDATION_WINDOWS for i in range(30)
    ]


def test_future_outcome_not_visible_before_available_at(tmp_path):
    store=ObservationEventStore(tmp_path/"pit.sqlite3"); engine=WalletForwardAlphaEngine(store)
    start=datetime(2026,9,13,tzinfo=timezone.utc); wallet="arbitrary-wallet-not-whitelisted"; context="elite_wallet_continuation|PUMP_FUN|bonding_curve|neutral|clean"; detected=start+timedelta(seconds=1)
    engine.record_integrity(WalletIntegritySnapshot(wallet,start,1.0,False))
    engine.record_observation(_observation(wallet,context,"one",detected)); engine.record_forward_outcome(_outcome(wallet,context,"one",detected))
    assert engine.score(wallet,context,as_of=detected+timedelta(seconds=30)).observations==0
    assert engine.score(wallet,context,as_of=detected+timedelta(seconds=61)).observations==1
    assert engine.status()["future_outcomes_excluded_until_available_at"] is True


def test_dynamic_tier_shrinkage_capacity_and_no_whitelist(tmp_path):
    store=ObservationEventStore(tmp_path/"tier.sqlite3"); engine=WalletForwardAlphaEngine(store)
    start=datetime(2026,9,13,tzinfo=timezone.utc); wallet="brand-new-wallet-z"; context="elite_wallet_continuation|PUMP_FUN|bonding_curve|neutral|clean"
    score=engine.score(wallet,context,as_of=_mature(engine,wallet,context,start))
    assert score.observations==60 and score.tier=="A" and score.eligible_for_strategy_influence
    assert 0<score.shrunk_expected_marginal_alpha<score.raw_expected_marginal_alpha
    assert score.capacity_coverage_for_500==1.0
    assert engine.status()["hard_coded_wallet_whitelist"] is False


def test_profitable_suspicious_wallet_stays_blocked(tmp_path):
    store=ObservationEventStore(tmp_path/"integrity.sqlite3"); engine=WalletForwardAlphaEngine(store)
    start=datetime(2026,9,13,tzinfo=timezone.utc); wallet="profitable-but-suspicious"; context="elite_wallet_continuation|PUMP_FUN|bonding_curve|neutral|clean"
    engine.record_integrity(WalletIntegritySnapshot(wallet,start,.10,True,common_funder_cluster="cluster-x",reasons=("common_funder",)))
    last=start
    for i in range(30):
        detected=start+timedelta(seconds=i*400); last=detected
        engine.record_observation(_observation(wallet,context,f"s-{i}",detected)); engine.record_forward_outcome(_outcome(wallet,context,f"s-{i}",detected,.07))
    score=engine.score(wallet,context,as_of=last+timedelta(seconds=61))
    assert score.shrunk_expected_marginal_alpha>0 and not score.eligible_for_strategy_influence
    assert "wallet_integrity_suspicious" in score.blockers


def test_graduation_quality_deduplicates_entities():
    q=WalletForwardAlphaEngine.graduation_quality([
        GraduationBuyerEvidence("a","entity-1",False,False,None,.10,.90,False),
        GraduationBuyerEvidence("b","entity-1",False,False,None,.20,.80,False),
        GraduationBuyerEvidence("c","entity-2",True,True,"funder-1",.10,.90,False),
    ])
    assert q.unique_wallets==3 and q.independent_entities==2 and q.correlated_signal_counted_once
    assert q.suspicious_entities==1


def test_three_window_replay_fail_closed_then_material():
    incomplete=WalletForwardAlphaEngine.evaluate_replay([ReplayComparisonObservation("24h","only",0,0,.1)])
    assert incomplete.status==STATUS_INCOMPLETE and not incomplete.strategy_influence_enabled
    material=WalletForwardAlphaEngine.evaluate_replay(_material_rows())
    assert material.status==STATUS_MATERIAL and material.strategy_influence_enabled and all(x.accepted for x in material.windows)
    leaked=_material_rows(); first=leaked[0]
    leaked[0]=ReplayComparisonObservation(first.window,first.candidate_id,0,.005,.020,.10,.10,.10,False,True)
    rejected=WalletForwardAlphaEngine.evaluate_replay(leaked)
    assert not rejected.strategy_influence_enabled and any("lookahead_leakage" in x for x in rejected.reasons)


def test_strategy_neutral_until_validation_then_bounded(tmp_path):
    store=ObservationEventStore(tmp_path/"gate.sqlite3"); engine=WalletForwardAlphaEngine(store)
    start=datetime(2026,9,13,tzinfo=timezone.utc); wallet="wallet-alpha"; context="elite_wallet_continuation|PUMP_FUN|bonding_curve|neutral|clean"; as_of=_mature(engine,wallet,context,start)
    neutral=engine.strategy_profile(wallet=wallet,context_key=context,as_of=as_of)
    assert neutral["validation_status"]==STATUS_INCOMPLETE and not neutral["strategy_influence_enabled"] and neutral["sizing_multiplier"]==1.0
    assert not neutral["may_create_eligibility"] and not neutral["may_bypass_risk_or_execution"] and not neutral["independent_trade_authority"]
    report=WalletForwardAlphaEngine.evaluate_replay(_material_rows()); engine.persist_validation(report,evaluated_at=as_of)
    enabled=engine.strategy_profile(wallet=wallet,context_key=context,as_of=as_of)
    assert enabled["strategy_influence_enabled"] and 1.0<=enabled["sizing_multiplier"]<=1.10


def test_authoritative_bridge_preserves_eligibility_and_lane_cap(monkeypatch,tmp_path):
    store=ObservationEventStore(tmp_path/"bridge.sqlite3"); adapter=type("Adapter",(),{"store":store})(); pre={"wallet":"wallet-a","venue":"PUMP_FUN","lifecycle":"bonding_curve","regime":"neutral","risk":{"risk_severity":0.0}}
    monkeypatch.setattr(integration,"_BASE_TARGET",lambda *_a,**_k:(None,0.0,{})); monkeypatch.setattr(integration,"wallet_forward_shadow_profile",lambda *_a,**_k:{"available":True,"strategy_influence_enabled":True,"sizing_multiplier":1.10})
    lane,target,profiles=integration._wallet_target(adapter,pre,chase=0.0,latency=1.0)
    assert lane is None and target==0.0 and profiles=={}
    monkeypatch.setattr(integration,"_BASE_TARGET",lambda *_a,**_k:("elite_wallet_continuation",.05,{"elite_wallet_continuation":{}})); monkeypatch.setattr(integration.strategy,"_lane_cap",lambda _l,_s:.052)
    lane,target,profiles=integration._wallet_target(adapter,pre,chase=0.0,latency=1.0)
    assert lane=="elite_wallet_continuation" and abs(target-.052)<1e-12
    assert profiles[lane]["wallet_forward_alpha"]["baseline_v52_eligibility_already_established"] is True


def test_hot_path_bridge_never_creates_schema(tmp_path):
    store=ObservationEventStore(tmp_path/"no-ddl.sqlite3")
    before={str(r[0]) for r in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    profile=wallet_forward_shadow_profile(store,wallet="wallet-a",context_key="ctx")
    after={str(r[0]) for r in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert before==after and profile["sizing_multiplier"]==1.0 and not profile["strategy_influence_enabled"]
    assert profile["reason"]=="forward_alpha_schema_not_installed" and not profile["independent_trade_authority"]
