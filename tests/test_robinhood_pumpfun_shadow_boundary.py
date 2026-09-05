from __future__ import annotations

from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import FinalPolicy
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi import robinhood_pumpfun_shadow_boundary as shadow


def _quote(entry_price: float = 1.0) -> dict[str, object]:
    return {
        "amount_in_wei": 1000,
        "token_out": 1000,
        "entry_gas_wei": 0,
        "entry_total_cost_wei": 1000,
        "entry_price_eth": entry_price,
        "immediate_exit_wei": 1000,
        "round_trip_cost_fraction": 0.0,
    }


def test_shadow_boundary_matches_pumpfun_final_strategy_thresholds() -> None:
    pump = FinalPolicy()
    assert shadow.MIN_FORWARD_OUTCOMES_FOR_PAPER_ENTRY == pump.min_forward_outcomes_for_selection == 30
    assert shadow.MAX_COPYABLE_CHASE_FRACTION == pump.max_chase_fraction == 0.15
    assert shadow.MAX_COPYABLE_OBSERVATION_LATENCY_SECONDS == pump.max_certified_observation_latency_seconds == 20.0


def test_copyability_rejects_chase_above_15_percent_and_latency_above_20_seconds() -> None:
    good = shadow.copyability_assessment(
        _quote(1.15),
        signal_price_eth=1.0,
        signal_observed_ts=90.0,
        now_ts=110.0,
    )
    assert good["copyable"] is True

    chased = shadow.copyability_assessment(
        _quote(1.151),
        signal_price_eth=1.0,
        signal_observed_ts=90.0,
        now_ts=100.0,
    )
    assert chased["copyable"] is False
    assert "chase_above_15pct" in chased["blockers"]

    late = shadow.copyability_assessment(
        _quote(1.0),
        signal_price_eth=1.0,
        signal_observed_ts=90.0,
        now_ts=110.001,
    )
    assert late["copyable"] is False
    assert "observation_latency_above_20s" in late["blockers"]


def test_unproven_context_cannot_receive_bootstrap_paper_allocation(monkeypatch) -> None:
    def original(_self, **_kwargs):
        return (
            "elite_entity_continuation",
            0.01,
            {
                "elite_entity_continuation": {
                    "sample_count": 29,
                    "state": "bootstrap_forward_evidence",
                    "mean_return": 0.50,
                    "best_expected_log_growth": 0.10,
                }
            },
        )

    monkeypatch.setattr(shadow, "_ORIGINAL_CHOOSE", original)
    lane, fraction, profiles = shadow._choose_with_shadow_boundary(SimpleNamespace())
    assert lane is None
    assert fraction == 0.0
    assert profiles["_robinhood_shadow_boundary"]["paper_entry_eligible"] is False
    assert profiles["_robinhood_shadow_boundary"]["bootstrap_paper_allocation_allowed"] is False


def test_only_mature_positive_geometric_context_can_reach_sizing(monkeypatch) -> None:
    def original(_self, **_kwargs):
        return (
            "elite_entity_continuation",
            0.05,
            {
                "elite_entity_continuation": {
                    "sample_count": 30,
                    "state": "promoted_positive_log_growth",
                    "mean_return": 0.12,
                    "best_expected_log_growth": 0.004,
                }
            },
        )

    monkeypatch.setattr(shadow, "_ORIGINAL_CHOOSE", original)
    lane, fraction, profiles = shadow._choose_with_shadow_boundary(SimpleNamespace())
    assert lane == "elite_entity_continuation"
    assert fraction == 0.05
    assert profiles["_robinhood_shadow_boundary"]["paper_entry_eligible"] is True


def test_nonpositive_geometric_edge_stays_out_of_paper_even_after_30(monkeypatch) -> None:
    def original(_self, **_kwargs):
        return (
            "elite_entity_continuation",
            0.01,
            {
                "elite_entity_continuation": {
                    "sample_count": 30,
                    "state": "demoted_nonpositive_log_growth",
                    "mean_return": 0.02,
                    "best_expected_log_growth": -0.001,
                }
            },
        )

    monkeypatch.setattr(shadow, "_ORIGINAL_CHOOSE", original)
    lane, fraction, _ = shadow._choose_with_shadow_boundary(SimpleNamespace())
    assert lane is None
    assert fraction == 0.0


class _DummyPlane:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.release_commit = "shadow-release"

    @staticmethod
    def _v5_context_key(
        *,
        entity: str,
        role: str,
        lane: str,
        venue: str,
        lifecycle: str,
        regime: str,
        risk_signature: str,
        flow_state: str,
        latency: str = "chain_poll",
    ) -> str:
        return "|".join((entity, role, lane, venue, lifecycle, regime, risk_signature, flow_state, latency))


def _add_closed_shadow_outcome(plane: _DummyPlane, index: int, net_return: float) -> None:
    source = f"source-{index}"
    lane = "elite_entity_continuation"
    quote = _quote(1.0)
    risk = {"risk_signature": "clean", "risk_severity": 0.0}
    shadow._insert_shadow_trials(
        plane,
        source_key=source,
        token=f"token-{index}",
        market="market",
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        actor="0xactor",
        entity="0xentity",
        role="independent_entity",
        regime="neutral",
        flow_state="entity_accumulation",
        risk=risk,
        lanes=[lane],
        quote=quote,
        probe_fraction=0.01,
        signal_price_eth=1.0,
        chase_fraction=0.0,
        latency_seconds=1.0,
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


def test_contextual_promotion_evidence_comes_from_zero_allocation_shadow_outcomes(tmp_path) -> None:
    plane = _DummyPlane(ObservationEventStore(tmp_path / "shadow.sqlite3"))
    shadow._ensure_schema(plane)
    for index in range(30):
        _add_closed_shadow_outcome(plane, index, 0.10)
    values, source = shadow._context_returns_shadow(
        plane,
        entity="0xentity",
        role="independent_entity",
        lane="elite_entity_continuation",
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        regime="neutral",
        risk_signature="clean",
        flow_state="entity_accumulation",
    )
    assert len(values) == 30
    assert source == "shadow_exact_context"
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT MAX(paper_allocation_fraction) allocation,MAX(paper_promotion_authority) authority "
            "FROM robinhood_v5_shadow_trials"
        ).fetchone()
    assert float(row["allocation"]) == 0.0
    assert int(row["authority"]) == 0


def test_production_composition_installs_shadow_boundary_as_outer_strategy_gate() -> None:
    assert shadow.SHADOW_BOUNDARY_VERSION == "robinhood-chain-pumpfun-shadow-boundary-v1"
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_pumpfun_shadow_boundary_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane._v5_choose_lane_fraction, "_roi_robinhood_pumpfun_shadow_boundary", False))
    assert bool(getattr(RobinhoodChainPaperPlane._poll_once, "_roi_robinhood_entity_universe", False))
    assert bool(getattr(RobinhoodChainPaperPlane.status, "_roi_robinhood_entity_universe", False))
    assert getattr(RobinhoodChainPaperPlane, "_roi_robinhood_strategy_alignment_composition_version") == (
        "robinhood-strategy-alignment-composition-v5-pumpfun-shadow-boundary"
    )
