from __future__ import annotations

import sqlite3
import threading
from types import SimpleNamespace

import pytest

from solana_roi.unified_strategy_status import (
    REGIMES,
    _probe_status,
    _record_regime_probe,
    build_unified_strategy_status,
    regime_execution_contract,
)


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    def append(self, *_args, **_kwargs) -> None:
        return None


def _create_v5_trial_table(store: _Store) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, regime TEXT NOT NULL, venue TEXT NOT NULL, "
            "lifecycle TEXT NOT NULL, lane TEXT NOT NULL, position_fraction REAL NOT NULL, "
            "entry_cost_sol REAL, immediate_exit_net_sol REAL, risk_json TEXT NOT NULL, "
            "chase_band TEXT NOT NULL, latency_band TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "decision TEXT NOT NULL, selected INTEGER NOT NULL, entry_executable INTEGER NOT NULL, "
            "exit_executable INTEGER NOT NULL)"
        )


def test_every_market_regime_has_nonzero_paper_execution_contract() -> None:
    contract = regime_execution_contract()
    assert set(contract) == set(REGIMES)
    assert set(REGIMES) == {
        "weak_or_deteriorating",
        "neutral",
        "high_speculation",
        "broad_mania",
    }
    for regime in REGIMES:
        assert contract[regime]["solana"]["paper_execution_enabled"] is True
        assert contract[regime]["fomo"]["paper_execution_enabled"] is True
        assert contract[regime]["robinhood"]["paper_execution_enabled"] is True
        assert contract[regime]["solana"]["minimum_bootstrap_fraction"] > 0.0
        assert contract[regime]["fomo"]["clean_bootstrap_fraction_before_other_caps"] > 0.0
        assert contract[regime]["fomo"]["hazard_bootstrap_fraction_before_other_caps"] > 0.0
        assert contract[regime]["robinhood"]["minimum_bootstrap_fraction"] > 0.0
        assert contract[regime]["solana"]["regime_label_is_entry_veto"] is False
        assert contract[regime]["fomo"]["regime_label_is_entry_veto"] is False
        assert contract[regime]["robinhood"]["regime_label_is_entry_veto"] is False


def test_one_real_executable_round_trip_probe_can_complete_in_every_solana_regime() -> None:
    store = _Store()
    _create_v5_trial_table(store)
    adapter = SimpleNamespace(store=store, release_commit="test-release")

    with store._lock, store.db:
        for regime in REGIMES:
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_trials("
                "release_commit,source_signature,regime,venue,lifecycle,lane,position_fraction,entry_cost_sol,"
                "immediate_exit_net_sol,risk_json,chase_band,latency_band,observed_at,decision,selected,"
                "entry_executable,exit_executable) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "test-release",
                    f"sig-{regime}",
                    regime,
                    "PUMP_AMM",
                    "pump_amm_established_continuation_2_5m",
                    "elite_wallet_continuation",
                    0.005,
                    1.0,
                    0.99,
                    '{"structurally_tradeable":true}',
                    "baseline_le_15pct",
                    "le_5s",
                    "2026-09-04T12:00:00+00:00",
                    "paper_observe",
                    0,
                    1,
                    1,
                ),
            )

    for regime in REGIMES:
        assert _record_regime_probe(adapter, f"sig-{regime}") is True

    status = _probe_status(store, "test-release")
    assert set(status) == set(REGIMES)
    assert all(status[regime]["completed"] is True for regime in REGIMES)
    assert all(status[regime]["net_return_pct"] == pytest.approx(-1.0) for regime in REGIMES)

    with store._lock:
        rows = store.db.execute(
            "SELECT regime,paper_only,live_money_authority,promotion_authority,portfolio_allocation_authority "
            "FROM regime_paper_e2e_probes ORDER BY regime"
        ).fetchall()
    assert len(rows) == len(REGIMES)
    for row in rows:
        assert row["paper_only"] == 1
        assert row["live_money_authority"] == 0
        assert row["promotion_authority"] == 0
        assert row["portfolio_allocation_authority"] == 0


def test_unified_status_exposes_all_three_planes_and_separates_capability_from_proof() -> None:
    store = _Store()
    runtime = SimpleNamespace(
        store=store,
        quote_handoff=SimpleNamespace(client=object(), simulator=object()),
        wallet_discovery=SimpleNamespace(status=lambda: {}),
    )
    base = {
        "paper_only": True,
        "live_money_authority": False,
        "data_plane": "direct-solana",
        "direct_solana": {"enabled": True, "continuity_ok": True},
        "wallet_discovery": {
            "profit_first_entity_strategy": {
                "release_commit": "test-release",
                "risk_conditioned_alpha_v5": {"paper_strategy_authority": True},
                "fomo_paper_strategy": {"paper_strategy_authority": True},
                "fomo_continuation_shadow": {"last_error": None},
            }
        },
    }
    robinhood = {
        "runtime_ready": True,
        "paper_trading_authority": True,
        "failed_closed": False,
        "paper_only": True,
        "live_money_authority": False,
    }

    payload = build_unified_strategy_status(base, runtime, robinhood)

    assert payload["solana"]["all_regimes_paper_capable"] is True
    assert payload["fomo"]["all_regimes_paper_capable"] is True
    assert payload["robinhood"]["all_regimes_paper_capable"] is True
    assert payload["overall"]["all_regime_software_contracts_paper_capable"] is True
    assert payload["overall"]["all_paper_planes_e2e_achievable"] is True

    # Capability is immediate; empirical proof remains false until real forward
    # candidates actually produce entries/outcomes (or Solana's round-trip probe).
    assert payload["solana"]["all_regimes_e2e_proven"] is False
    assert payload["fomo"]["all_regimes_e2e_proven"] is False
    assert payload["robinhood"]["all_regimes_e2e_proven"] is False
    assert payload["overall"]["all_regime_e2e_paths_empirically_proven"] is False

    for regime in REGIMES:
        assert payload["solana"]["regimes"][regime]["e2e_achievable"] is True
        assert payload["fomo"]["regimes"][regime]["e2e_achievable"] is True
        assert payload["robinhood"]["regimes"][regime]["e2e_achievable"] is True
