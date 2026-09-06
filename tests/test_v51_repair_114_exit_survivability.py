from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import continuous_strategy_learning as learning
from solana_roi import fomo_paper_strategy as fomo
from solana_roi import risk_conditioned_alpha_v5 as risk
from solana_roi import v51_exact_exit_execution as exact
from solana_roi import v51_exit_execution_terminal_fomo_followup as followup
from solana_roi import v51_exit_survivability_analytics as analytics


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class Adapter(SimpleNamespace):
    pass


def _adapter() -> Adapter:
    return Adapter(store=Store(), epoch_id="epoch-114", release_commit="c" * 40)


def _risk_context(adapter: Adapter, source: str, *, risk_signature: str = "hazard-x") -> None:
    risk._v5_schema(adapter)
    now = "2026-09-06T12:00:00+00:00"
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_trials("
            "release_commit,strategy_version,source_signature,token_mint,trigger_wallet,lane,selected,decision,decision_reason,"
            "venue,lifecycle,regime,trigger_role,flow_state,risk_signature,risk_severity,risk_json,context_key,chase_band,"
            "latency_band,threshold_challenger,position_fraction,quote_input_lamports,entry_fee_lamports,entry_token_raw,"
            "entry_cost_sol,immediate_exit_net_sol,round_trip_cost_fraction,entry_executable,exit_executable,observed_at,"
            "created_at,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,
                "v5-test",
                source,
                f"mint-{source}",
                "wallet-a",
                "hazard_continuation",
                1,
                "paper_enter_promoted_hazard",
                "test",
                "PUMP_AMM",
                "pump_amm_early_post_graduation_30_120s",
                "hot",
                "momentum",
                "creator_distribution",
                risk_signature,
                0.6,
                "{}",
                f"ctx|PUMP_AMM|early|hot|{risk_signature}",
                "le_15pct",
                "10_20s",
                0,
                0.05,
                1_000_000_000,
                0,
                10_000,
                1.0,
                1.0,
                0.0,
                1,
                1,
                now,
                now,
            ),
        )


def _liquidation(
    adapter: Adapter,
    source: str,
    *,
    creator_distribution: bool,
    scope: str = "final",
    actual_position_raw: int = 10_000,
) -> dict[str, object]:
    first_due = "2026-09-06T12:00:00+00:00"
    return exact._upsert_liquidation(
        adapter,
        {
            "position_scope": scope,
            "source_signature": source,
            "token_mint": f"mint-{source}",
            "actual_position_raw": actual_position_raw,
            "entry_cost_sol": 1.0,
            "position_fraction": 0.05 if scope == "final" else 0.02,
            "exit_signal_signature": f"sell-{source}",
            "exit_reason": "trigger_wallet_exit_baseline",
            "exit_features_json": (
                '{"creator_distribution":true,"linked_entity_distribution":true}'
                if creator_distribution
                else '{"creator_distribution":false,"linked_entity_distribution":false}'
            ),
            "first_exit_due_at": first_due,
        },
    )


def _evidence(*, horizon: int, impact: float, executable: bool, output_lamports: int = 0) -> dict[str, object]:
    at = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc) + timedelta(seconds=horizon)
    return {
        "attempted_at": at.isoformat(),
        "quote_input_raw": 10_000,
        "amount_match": True,
        "router": "jupiter-test",
        "expected_output_lamports": output_lamports if output_lamports else (1_000_000_000 if executable else None),
        "minimum_output_lamports": output_lamports - 10_000 if output_lamports else None,
        "route_hops": [{"amm": "pump"}],
        "price_impact_pct": impact,
        "quote_age_ms": 4.0,
        "token_account_requirements": {},
        "transaction_built": executable,
        "transaction_sha256": "abc" if executable else None,
        "transaction_size_bytes": 123 if executable else None,
        "last_valid_block_height": 123 if executable else None,
        "simulation_ok": executable,
        "simulation_error_class": None if executable else "route_unavailable",
        "units_consumed": 100 if executable else None,
        "simulation_slot": 50 if executable else None,
        "logs_count": 1,
        "route_valid": executable,
        "token_restriction": False,
        "account_failure": False,
        "transfer_failure": False,
        "signature_fee_lamports": 0,
        "prioritization_fee_lamports": 0,
        "rent_fee_lamports": 0,
        "total_fee_lamports": 0,
        "error": None if executable else "no executable route",
    }


def _record(
    adapter: Adapter,
    liquidation: dict[str, object],
    attempt_number: int,
    *,
    impact: float,
    executable: bool,
    output_lamports: int = 0,
    terminal: bool = False,
) -> None:
    horizon = exact.EXIT_RETRY_ELAPSED_SECONDS[attempt_number - 1]
    evidence = _evidence(
        horizon=horizon,
        impact=impact,
        executable=executable,
        output_lamports=output_lamports,
    )
    exact._record_attempt(
        adapter,
        liquidation,
        evidence,
        attempt_number=attempt_number,
        next_retry_at=None,
        status=(
            "paper_exit_terminal_unexitable"
            if terminal
            else ("paper_exit_executed" if executable else "paper_exit_execution_failed")
        ),
        terminal_assumption=exact.TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
    )


def _mfe(adapter: Adapter, source: str, value: float) -> None:
    learning._ensure_schema(adapter)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO strategy_learning_final_paths("
            "epoch_id,release_commit,source_signature,token_mint,reference_at,horizon_seconds,mark_count,captured_horizon_count,"
            "mfe_mark_return,mae_mark_return,time_to_mfe_seconds,time_to_mae_seconds,finalized_at) "
            "VALUES (?,?,?,?,?,300,3,3,?,?,10,20,?)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                source,
                f"mint-{source}",
                "2026-09-06T12:00:00+00:00",
                value,
                -0.1,
                "2026-09-06T12:05:10+00:00",
            ),
        )


@pytest.fixture(autouse=True)
def _active_epoch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(exact, "EXACT_EXIT_EXECUTION_MODEL_EPOCH", followup.ACTIVE_EXECUTION_MODEL_EPOCH)


def test_114_horizon_denominators_recovery_decay_and_mfe_are_execution_realistic() -> None:
    adapter = _adapter()
    for source, creator in (("a", False), ("b", True), ("c", True)):
        _risk_context(adapter, source)
        liquidation = _liquidation(adapter, source, creator_distribution=creator)
        if source == "a":
            _record(adapter, liquidation, 1, impact=0.01, executable=False)
            _record(adapter, liquidation, 2, impact=0.02, executable=True, output_lamports=1_300_000_000)
            _mfe(adapter, source, 0.50)
        elif source == "b":
            _record(adapter, liquidation, 1, impact=0.01, executable=False)
            _record(adapter, liquidation, 2, impact=0.03, executable=False)
            _record(adapter, liquidation, 3, impact=0.05, executable=True, output_lamports=1_150_000_000)
            _mfe(adapter, source, 0.40)
        else:
            _record(adapter, liquidation, 1, impact=0.02, executable=False)
            _record(adapter, liquidation, 2, impact=0.04, executable=False)
            _record(adapter, liquidation, 3, impact=0.06, executable=False)
            _record(adapter, liquidation, 4, impact=0.08, executable=False)
            _record(adapter, liquidation, 5, impact=0.10, executable=False)
            _record(adapter, liquidation, 6, impact=0.12, executable=False, terminal=True)
            _mfe(adapter, source, 0.80)

    before_changes = adapter.store.db.total_changes
    report = analytics.build_report(adapter)
    after_changes = adapter.store.db.total_changes
    assert after_changes == before_changes, "Repair 114 analytics must be read-only"
    assert report["execution_model_epoch"] == followup.ACTIVE_EXECUTION_MODEL_EPOCH
    assert report["strategy_authority"] is False
    assert report["promotion_authority"] is False
    assert report["attempt_count"] == 11
    assert report["context_count"] == 1

    context = report["contexts"][0]
    h10 = context["route_survivability_by_retry_horizon"]["10"]
    assert h10["at_risk_position_count"] == 3
    assert h10["executable_route_position_count"] == 1
    assert h10["conditional_executable_route_probability"] == pytest.approx(1 / 3)

    h30 = context["route_survivability_by_retry_horizon"]["30"]
    assert h30["at_risk_position_count"] == 2, "position a exited at 10s and must leave the later denominator"
    assert h30["executable_route_position_count"] == 1
    assert h30["conditional_executable_route_probability"] == 0.5

    h300 = context["route_survivability_by_retry_horizon"]["300"]
    assert h300["at_risk_position_count"] == 1
    assert h300["executable_route_position_count"] == 0
    assert h300["conditional_executable_route_probability"] == 0.0
    assert context["terminal_unexitable_position_rate"] == pytest.approx(1 / 3)

    recovery = context["time_to_recovered_exitability_seconds"]
    assert recovery["count"] == 2
    assert recovery["median"] == 20.0
    assert context["liquidity_decay"]["price_impact_pct_points_per_minute"]["median"] > 0.0

    mfe = context["mfe_realizability"]
    assert mfe["paired_position_count"] == 2
    assert mfe["reference_price_mfe_fraction"]["mean"] == pytest.approx(0.45)
    assert mfe["executable_realizable_mfe_fraction"]["mean"] == pytest.approx(0.225)
    assert mfe["reference_minus_realizable_mfe_gap_fraction"]["mean"] == pytest.approx(0.225)

    creator = report["creator_distribution_route_deterioration"]
    assert set(creator) == {"false", "true"}
    assert creator["true"]["terminal_unexitable_position_rate"] == 0.5
    assert report["hazard_signature_failed_exit"]["hazard-x"]["terminal_unexitable_position_rate"] == pytest.approx(1 / 3)


def test_114_fomo_is_its_own_exact_held_size_context_and_old_epoch_is_excluded(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _adapter()
    fomo._schema(adapter)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT INTO fomo_paper_trials("
            "release_commit,strategy_version,source_signature,token_mint,trigger_wallet,venue,lifecycle,regime,fomo_state,"
            "wallet_context_state,decision,decision_reason,position_fraction,entry_observed_at,signal_to_entry_seconds,"
            "entry_cost_sol,entry_token_raw,token_decimals,entry_all_in_price_sol,entry_executable,exit_executable,created_at,"
            "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,
                "fomo-test",
                "fomo-a",
                "mint-fomo-a",
                "wallet-fomo",
                "PUMP_AMM",
                "pump_amm_early_post_graduation_30_120s",
                "hot",
                "active_fomo",
                "bootstrap",
                "paper_enter_bootstrap_probe",
                "test",
                0.02,
                "2026-09-06T12:00:00+00:00",
                5.0,
                0.2,
                2_000,
                6,
                0.0001,
                1,
                1,
                "2026-09-06T12:00:00+00:00",
            ),
        )
    fomo_liquidation = _liquidation(
        adapter,
        "fomo-a",
        creator_distribution=False,
        scope="fomo",
        actual_position_raw=2_000,
    )
    _record(adapter, fomo_liquidation, 1, impact=0.025, executable=True, output_lamports=250_000_000)

    # Add a legacy-v2 exact-exit row for another position. Default Repair 114 report
    # must never pool it with the active v3 terminal/FOMO execution epoch.
    monkeypatch.setattr(exact, "EXACT_EXIT_EXECUTION_MODEL_EPOCH", "v51-execution-model-exact-exit-v2")
    old = _liquidation(adapter, "old-v2", creator_distribution=True)
    _record(adapter, old, 1, impact=0.99, executable=False)
    monkeypatch.setattr(exact, "EXACT_EXIT_EXECUTION_MODEL_EPOCH", followup.ACTIVE_EXECUTION_MODEL_EPOCH)

    report = analytics.build_report(adapter)
    assert report["attempt_count"] == 1
    assert report["context_count"] == 1
    context = report["contexts"][0]
    assert context["context"]["position_scope"] == "fomo"
    assert context["context"]["lane"] == "fomo_continuation_paper"
    assert context["context"]["venue"] == "PUMP_AMM"
    assert context["actual_held_size_price_impact_pct"]["mean"] == 0.025
    assert report["creator_distribution_route_deterioration"] == {
        "false": {
            "position_count": 1,
            "attempt_count": 1,
            "failed_attempt_rate": 0.0,
            "terminal_unexitable_position_rate": 0.0,
            "actual_size_price_impact_pct": {"count": 1, "mean": 0.025, "median": 0.025, "p90": 0.025},
            "price_impact_decay_pct_points_per_minute": {"count": 0, "mean": None, "median": None, "p90": None},
        }
    }


def test_114_contract_is_research_only_and_preserves_v51_authority() -> None:
    payload = analytics.status()
    assert payload["horizons_seconds"] == [10, 30, 60, 120, 300]
    assert payload["derived_read_only"] is True
    assert payload["v52_research_input_only"] is True
    assert payload["strategy_authority"] is False
    assert payload["promotion_authority"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
