from __future__ import annotations

import sqlite3
import threading
from types import SimpleNamespace

import pytest

from solana_roi import v52_robinhood_position_lifecycle as lifecycle
from solana_roi.strategy_v52_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, STRATEGY_VERSION


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def test_stressed_exit_capacity_is_stricter_than_legacy_two_x_depth() -> None:
    assert lifecycle.STRESSED_DEPTH_FRACTION == pytest.approx(0.35)
    assert lifecycle.STRESSED_DEPTH_UTILIZATION_LIMIT == pytest.approx(0.25)
    assert lifecycle.STRESSED_EXIT_COVERAGE_RATIO == pytest.approx(1.0 / 0.0875)
    assert lifecycle.STRESSED_EXIT_COVERAGE_RATIO > 2.0


def test_lifecycle_schema_is_durable_paper_only_and_big_raw_values_are_text() -> None:
    owner = SimpleNamespace(store=_Store())
    lifecycle._ensure_schema(owner)
    with owner.store._lock:
        tables = {
            str(row["name"])
            for row in owner.store.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        position_columns = {
            str(row["name"]): str(row["type"])
            for row in owner.store.db.execute(
                "PRAGMA table_info(v52_robinhood_positions)"
            ).fetchall()
        }
        lot_columns = {
            str(row["name"]): str(row["type"])
            for row in owner.store.db.execute(
                "PRAGMA table_info(v52_robinhood_position_lots)"
            ).fetchall()
        }
    assert {
        "v52_robinhood_positions",
        "v52_robinhood_position_lots",
        "v52_robinhood_position_events",
    } <= tables
    assert position_columns["remaining_token_raw"].upper() == "TEXT"
    assert lot_columns["entry_token_raw"].upper() == "TEXT"
    assert lot_columns["remaining_entry_cost_wei"].upper() == "TEXT"


def test_scale_requires_material_new_forward_strength(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "0x0000000000000000000000000000000000000011"
    market = "0x0000000000000000000000000000000000000022"
    base_profiles = {
        "entity_flow_accumulation": {
            "v52_authority": {
                "target_fraction": 0.04,
                "final_fraction": 0.01,
                "capture_stage": "starter",
            }
        }
    }

    monkeypatch.setattr(lifecycle, "_ensure_schema", lambda owner: None)
    monkeypatch.setattr(
        lifecycle,
        "_BASE_CHOOSE",
        lambda self, **kwargs: (
            "entity_flow_accumulation",
            0.01,
            base_profiles,
        ),
    )
    owner = SimpleNamespace(
        _roi_v52_candidate_token=token,
        _roi_v52_candidate_market=market,
    )
    kwargs = {
        "entity": "entity-a",
        "role": "independent_entity",
        "venue": "UNISWAP_V3_DIRECT",
        "lifecycle": "new_weth_pool",
        "regime": "neutral",
        "risk_signature": "clean",
        "risk_severity": 0.20,
        "flow_state": "pre_fomo",
        "lanes": ["entity_flow_accumulation"],
    }
    payload = lifecycle._evidence_payload(kwargs, "entity_flow_accumulation")
    position = {
        "status": "entered",
        "market": market,
        "remaining_fraction": 0.01,
        "last_evidence_fingerprint": lifecycle._fingerprint(payload),
        "last_lane": "entity_flow_accumulation",
        "lifecycle": "new_weth_pool",
        "last_trigger_entity": "entity-a",
        "last_flow_state": "pre_fomo",
        "last_risk_severity": 0.20,
    }
    monkeypatch.setattr(lifecycle, "_open_position", lambda owner, token: dict(position))
    monkeypatch.setattr(lifecycle, "_last_closed_position", lambda owner, token: None)

    lane, fraction, profiles = lifecycle._choose_with_lifecycle(owner, **kwargs)
    assert lane is None
    assert fraction == 0.0
    assert profiles["entity_flow_accumulation"]["v52_authority"]["reason"] == "scale_blocked_no_new_forward_evidence"

    strengthened = {**kwargs, "flow_state": "active_fomo"}
    lane, fraction, profiles = lifecycle._choose_with_lifecycle(owner, **strengthened)
    assert lane == "entity_flow_accumulation"
    assert fraction == pytest.approx(0.01)
    authority = profiles[lane]["v52_authority"]
    assert authority["capture_stage"] == "scale"
    assert authority["reason"] == "v52_scale_new_forward_strength"
    assert authority["aggregate_position_exitability_required_before_add"] is True
    assert authority["stressed_exit_capacity_required_before_entry_or_add"] is True
    assert authority["averaging_down_allowed"] is False


def test_second_leg_reentry_requires_new_impulse_acceleration_and_buyer(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "0x0000000000000000000000000000000000000033"
    market = "0x0000000000000000000000000000000000000044"
    monkeypatch.setattr(lifecycle, "_ensure_schema", lambda owner: None)
    monkeypatch.setattr(
        lifecycle,
        "_BASE_CHOOSE",
        lambda self, **kwargs: (
            "fomo_continuation",
            0.01,
            {"fomo_continuation": {"v52_authority": {"target_fraction": 0.04}}},
        ),
    )
    owner = SimpleNamespace(
        _roi_v52_candidate_token=token,
        _roi_v52_candidate_market=market,
    )
    closed = {
        "status": "closed",
        "market": market,
        "last_evidence_fingerprint": "old-impulse",
        "last_trigger_entity": "entity-a",
        "closed_at": "2020-01-01T00:00:00+00:00",
    }
    monkeypatch.setattr(lifecycle, "_open_position", lambda owner, token: None)
    monkeypatch.setattr(lifecycle, "_last_closed_position", lambda owner, token: dict(closed))

    weak = {
        "entity": "entity-a",
        "role": "independent_entity",
        "venue": "UNISWAP_V3_DIRECT",
        "lifecycle": "new_weth_pool",
        "regime": "neutral",
        "risk_signature": "clean",
        "risk_severity": 0.10,
        "flow_state": "pre_fomo",
        "lanes": ["fomo_continuation"],
    }
    lane, fraction, profiles = lifecycle._choose_with_lifecycle(owner, **weak)
    assert lane is None
    assert fraction == 0.0
    assert profiles["fomo_continuation"]["v52_authority"]["reason"] == "reentry_blocked_renewed_acceleration_missing"

    fresh = {**weak, "entity": "entity-b", "flow_state": "active_fomo"}
    lane, fraction, profiles = lifecycle._choose_with_lifecycle(owner, **fresh)
    assert lane == "fomo_continuation"
    assert fraction == pytest.approx(0.01)
    assert profiles[lane]["v52_authority"]["capture_stage"] == "reentry"
    assert profiles[lane]["v52_authority"]["reason"] == "v52_reentry_new_impulse"


def test_lifecycle_status_preserves_non_live_authority_boundary() -> None:
    status = lifecycle.lifecycle_status()
    assert status["authority_id"] == AUTHORITY_ID
    assert status["strategy_version"] == STRATEGY_VERSION
    assert status["economic_freeze_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert status["durable_lot_inventory"] is True
    assert status["aggregate_exact_exitability_before_add"] is True
    assert status["stressed_exit_capacity_before_entry_or_add"] is True
    assert status["scale_requires_new_forward_strength"] is True
    assert status["averaging_down_allowed"] is False
    assert status["staged_derisk_runner_authority"] is True
    assert status["second_leg_reentry_requires_new_impulse"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
