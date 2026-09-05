from __future__ import annotations

from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import FinalPolicy
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi import robinhood_pumpfun_shadow_boundary as shadow
from solana_roi import robinhood_native_shadow_learning as native


def _quote(entry_price: float = 1.0, *, round_trip_cost: float = 0.0) -> dict[str, object]:
    return {
        "amount_in_wei": 1000,
        "token_out": 1000,
        "entry_gas_wei": 0,
        "entry_total_cost_wei": 1000,
        "entry_price_eth": entry_price,
        "immediate_exit_wei": max(1, int(round(1000 * (1.0 - round_trip_cost)))),
        "round_trip_cost_fraction": round_trip_cost,
    }


def test_shadow_boundary_keeps_pumpfun_forward_proof_gate_without_universal_timing_vetoes() -> None:
    pump = FinalPolicy()
    assert shadow.MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY == pump.min_forward_outcomes_for_selection == 30
    assert native.MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY == 30
    assert native.NATIVE_LEARNING_VERSION == "robinhood-native-shadow-learning-v1"
    assert native.LEARNING_COMPATIBILITY_VERSION == shadow.SHADOW_BOUNDARY_VERSION


def test_chase_latency_and_measurable_cost_are_context_not_universal_vetoes() -> None:
    result = shadow.copyability_assessment(
        _quote(1.22, round_trip_cost=0.18),
        signal_price_eth=1.0,
        signal_observed_ts=60.0,
        now_ts=95.0,
    )
    assert result["copyable"] is True
    assert result["mechanically_executable"] is True
    assert result["blockers"] == []
    assert result["chase_band"] == "15_25pct"
    assert result["latency_band"] == "20_60s"
    assert result["cost_band"] == "15_30pct"
    assert result["chase_is_context_not_veto"] is True
    assert result["latency_is_lane_context_not_veto"] is True
    assert result["measurable_cost_is_context_not_veto"] is True


def test_mechanically_unexitable_opportunity_still_fails_closed() -> None:
    quote = _quote(1.0)
    quote["immediate_exit_wei"] = 0
    result = shadow.copyability_assessment(
        quote,
        signal_price_eth=1.0,
        signal_observed_ts=90.0,
        now_ts=100.0,
    )
    assert result["copyable"] is False
    assert "exit_quote_unavailable" in result["blockers"]


def test_robinhood_context_bands_match_native_design() -> None:
    assert native.robinhood_chase_band(0.01) == "0_5pct"
    assert native.robinhood_chase_band(0.10) == "5_15pct"
    assert native.robinhood_chase_band(0.22) == "15_25pct"
    assert native.robinhood_chase_band(0.35) == "25_40pct"
    assert native.robinhood_chase_band(0.60) == "gt_40pct"
    assert native.robinhood_latency_band("fomo_continuation", 35.0) == "fomo_continuation:20_60s"
    assert native.robinhood_latency_band("elite_entity_continuation", 35.0) == "elite_entity_continuation:20_60s"


def _choose_kwargs() -> dict[str, object]:
    return {
        "entity": "0xentity",
        "role": "independent_entity",
        "venue": "UNISWAP_V3_DIRECT",
        "lifecycle": "new_weth_pool",
        "regime": "neutral",
        "risk_signature": "clean",
        "risk_severity": 0.0,
        "flow_state": "entity_accumulation",
        "lanes": ["elite_entity_continuation"],
        "shadow_chase_fraction": 0.22,
        "shadow_latency_seconds": 35.0,
        "shadow_round_trip_cost_fraction": 0.03,
    }


def _chooser_plane() -> SimpleNamespace:
    plane = SimpleNamespace()
    plane._v5_regime_multiplier = lambda _regime: 1.0
    plane._open_exposure = lambda: 0.0
    return plane


def test_unproven_context_cannot_receive_bootstrap_paper_allocation(monkeypatch) -> None:
    monkeypatch.setattr(shadow, "_ORIGINAL_CHOOSE", lambda _self, **_kwargs: ("elite_entity_continuation", 0.01, {}))
    monkeypatch.setattr(
        shadow,
        "_shadow_profile",
        lambda _self, **_kwargs: {
            "sample_count": 29,
            "state": "bootstrap_forward_evidence",
            "best_fraction": 0.05,
            "mean_return": 0.50,
            "best_expected_log_growth": 0.10,
            "evidence_source": "native_exact_context_bootstrap_cross_release",
        },
    )
    lane, fraction, profiles = shadow._choose_with_shadow_boundary(_chooser_plane(), **_choose_kwargs())
    assert lane is None
    assert fraction == 0.0
    assert profiles["_robinhood_shadow_boundary"]["paper_entry_eligible"] is False
    assert profiles["_robinhood_shadow_boundary"]["bootstrap_paper_allocation_allowed"] is False


def test_only_mature_positive_geometric_context_can_reach_sizing(monkeypatch) -> None:
    monkeypatch.setattr(shadow, "_ORIGINAL_CHOOSE", lambda _self, **_kwargs: ("elite_entity_continuation", 0.01, {}))
    monkeypatch.setattr(
        shadow,
        "_shadow_profile",
        lambda _self, **_kwargs: {
            "sample_count": 30,
            "state": "promoted_positive_log_growth",
            "best_fraction": 0.05,
            "mean_return": 0.12,
            "best_expected_log_growth": 0.004,
            "evidence_source": "native_exact_context_cross_release",
        },
    )
    lane, fraction, profiles = shadow._choose_with_shadow_boundary(_chooser_plane(), **_choose_kwargs())
    assert lane == "elite_entity_continuation"
    assert fraction == 0.05
    assert profiles["_robinhood_shadow_boundary"]["paper_entry_eligible"] is True


def test_nonpositive_geometric_edge_stays_out_of_paper_even_after_30(monkeypatch) -> None:
    monkeypatch.setattr(shadow, "_ORIGINAL_CHOOSE", lambda _self, **_kwargs: ("elite_entity_continuation", 0.01, {}))
    monkeypatch.setattr(
        shadow,
        "_shadow_profile",
        lambda _self, **_kwargs: {
            "sample_count": 30,
            "state": "demoted_nonpositive_log_growth",
            "best_fraction": 0.0,
            "mean_return": 0.02,
            "best_expected_log_growth": -0.001,
            "evidence_source": "native_exact_context_cross_release",
        },
    )
    lane, fraction, _ = shadow._choose_with_shadow_boundary(_chooser_plane(), **_choose_kwargs())
    assert lane is None
    assert fraction == 0.0


class _DummyPlane:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.release_commit = "release-a"


def _add_closed_shadow_outcome(
    plane: _DummyPlane,
    index: int,
    net_return: float,
    *,
    release_commit: str,
    entity: str = "0xentity",
    chase: float = 0.22,
    latency: float = 35.0,
) -> None:
    plane.release_commit = release_commit
    source = f"source-{release_commit}-{index}"
    lane = "elite_entity_continuation"
    quote = _quote(1.0 + chase)
    risk = {"risk_signature": "clean", "risk_severity": 0.0}
    shadow._insert_shadow_trials(
        plane,
        source_key=source,
        token=f"token-{release_commit}-{index}",
        market="market",
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        actor="0xactor",
        entity=entity,
        role="independent_entity",
        regime="neutral",
        flow_state="entity_accumulation",
        risk=risk,
        lanes=[lane],
        quote=quote,
        probe_fraction=0.01,
        signal_price_eth=1.0,
        chase_fraction=chase,
        latency_seconds=latency,
    )
    with plane.store._lock, plane.store.db:
        trial = plane.store.db.execute(
            "SELECT * FROM robinhood_v5_shadow_trials WHERE release_commit=? AND source_key=? AND lane=?",
            (plane.release_commit, source, lane),
        ).fetchone()
        assert trial is not None
        plane.store.db.execute(
            "INSERT INTO robinhood_v5_shadow_outcomes("
            "shadow_trial_id,release_commit,strategy_version,token,market,venue,lifecycle,trigger_entity,lane,regime,risk_signature,"
            "context_key,probe_fraction,net_return,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_allocation_fraction,"
            "paper_only,live_money_authority,paper_promotion_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0.0,1,0,0)",
            (
                int(trial["id"]),
                plane.release_commit,
                shadow.SHADOW_BOUNDARY_VERSION,
                str(trial["token"]),
                str(trial["market"]),
                str(trial["venue"]),
                str(trial["lifecycle"]),
                str(trial["trigger_entity"]),
                str(trial["lane"]),
                str(trial["regime"]),
                str(trial["risk_signature"]),
                str(trial["context_key"]),
                float(trial["probe_fraction"]),
                float(net_return),
                "1100",
                "0",
                "test",
                "2026-09-05T00:00:00+00:00",
            ),
        )
        plane.store.db.execute(
            "UPDATE robinhood_v5_shadow_trials SET settled_at=?,exit_reason=? WHERE id=?",
            ("2026-09-05T00:00:00+00:00", "test", int(trial["id"])),
        )


def _context_values(plane: _DummyPlane) -> tuple[list[float], str]:
    return shadow._context_returns_shadow(
        plane,
        entity="0xentity",
        role="independent_entity",
        lane="elite_entity_continuation",
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        regime="neutral",
        risk_signature="clean",
        flow_state="entity_accumulation",
        chase_fraction=0.22,
        latency_seconds=35.0,
        round_trip_cost_fraction=0.0,
    )


def test_shadow_learning_survives_compatible_deployments(tmp_path) -> None:
    plane = _DummyPlane(ObservationEventStore(tmp_path / "shadow.sqlite3"))
    shadow._ensure_schema(plane)
    for index in range(18):
        _add_closed_shadow_outcome(plane, index, 0.10, release_commit="release-a")
    for index in range(17):
        _add_closed_shadow_outcome(plane, index, 0.10, release_commit="release-b")
    plane.release_commit = "release-c"
    values, source = _context_values(plane)
    assert len(values) == 35
    assert source == "native_exact_context_cross_release"
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT COUNT(DISTINCT release_commit) releases FROM robinhood_v5_shadow_trials WHERE strategy_version=?",
            (shadow.SHADOW_BOUNDARY_VERSION,),
        ).fetchone()
    assert int(row["releases"]) == 2


def test_cross_release_source_dedup_does_not_consume_duplicate_shadow_slots(tmp_path) -> None:
    plane = _DummyPlane(ObservationEventStore(tmp_path / "dedup.sqlite3"))
    shadow._ensure_schema(plane)
    kwargs = {
        "source_key": "same-economic-signal",
        "token": "token",
        "market": "market",
        "venue": "UNISWAP_V3_DIRECT",
        "lifecycle": "new_weth_pool",
        "actor": "0xactor",
        "entity": "0xentity",
        "role": "independent_entity",
        "regime": "neutral",
        "flow_state": "entity_accumulation",
        "risk": {"risk_signature": "clean", "risk_severity": 0.0},
        "lanes": ["elite_entity_continuation"],
        "quote": _quote(1.22),
        "probe_fraction": 0.01,
        "signal_price_eth": 1.0,
        "chase_fraction": 0.22,
        "latency_seconds": 35.0,
    }
    plane.release_commit = "release-a"
    shadow._insert_shadow_trials(plane, **kwargs)
    plane.release_commit = "release-b"
    shadow._insert_shadow_trials(plane, **kwargs)
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT COUNT(*) count FROM robinhood_v5_shadow_trials WHERE source_key=?",
            ("same-economic-signal",),
        ).fetchone()
    assert int(row["count"]) == 1


def test_production_composition_installs_native_learning_without_breaking_v51_lineage() -> None:
    assert shadow.SHADOW_BOUNDARY_VERSION == "robinhood-chain-pumpfun-shadow-boundary-v1"
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_pumpfun_shadow_boundary_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_native_shadow_learning_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane._v5_choose_lane_fraction, "_roi_robinhood_pumpfun_shadow_boundary", False))
    assert bool(getattr(RobinhoodChainPaperPlane._v5_choose_lane_fraction, "_roi_robinhood_native_shadow_learning", False))
    assert bool(getattr(RobinhoodChainPaperPlane._v5_choose_lane_fraction, "_roi_robinhood_entity_universe", False))
    assert bool(getattr(RobinhoodChainPaperPlane._poll_once, "_roi_robinhood_entity_universe", False))
    assert bool(getattr(RobinhoodChainPaperPlane.status, "_roi_robinhood_entity_universe", False))
    assert RobinhoodChainPaperPlane._maybe_open_v3.__module__.endswith("robinhood_chain_profit_maximizer")
    assert RobinhoodChainPaperPlane._maybe_open_v2.__module__.endswith("risk_conditioned_alpha_v51")
    assert getattr(RobinhoodChainPaperPlane, "_roi_robinhood_strategy_alignment_composition_version") == (
        "robinhood-strategy-alignment-composition-v6-native-shadow-learning"
    )
