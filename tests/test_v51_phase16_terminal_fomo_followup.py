from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from solana_roi import v51_exact_exit_execution as exact
from solana_roi import v51_exit_execution_terminal_fomo_followup as followup


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class Adapter(SimpleNamespace):
    pass


def _adapter() -> Adapter:
    return Adapter(store=Store(), epoch_id="epoch-active", release_commit="b" * 40)


def _create_final_tables(adapter: Adapter) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE profit_first_final_trials("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,epoch_id TEXT,source_signature TEXT,trigger_wallet TEXT,lane TEXT,"
            "context_json TEXT,observed_at TEXT,signal_to_entry_seconds REAL,assigned_position_fraction REAL)"
        )
        adapter.store.db.execute(
            "CREATE TABLE profit_first_final_outcomes("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,epoch_id TEXT,release_commit TEXT,strategy_version TEXT,"
            "source_signature TEXT,exit_signature TEXT,token_mint TEXT,trigger_wallet TEXT,lane TEXT,context_json TEXT,"
            "entry_observed_at TEXT,exit_observed_at TEXT,signal_to_entry_seconds REAL,position_fraction REAL,"
            "entry_cost_sol REAL,exit_net_sol REAL,net_return REAL,evidence_phase TEXT,exit_reason TEXT,"
            "exit_features_json TEXT,created_at TEXT,UNIQUE(epoch_id,source_signature,lane))"
        )
        adapter.store.db.execute(
            "INSERT INTO profit_first_final_trials(epoch_id,source_signature,trigger_wallet,lane,context_json,observed_at,"
            "signal_to_entry_seconds,assigned_position_fraction) VALUES (?,?,?,?,?,?,?,?)",
            (
                adapter.epoch_id,
                "entry-terminal",
                "wallet-a",
                "unified_profit_maximizer",
                None,
                "2026-09-06T12:00:00+00:00",
                2.0,
                0.05,
            ),
        )


def test_followup_retry_schedule_is_elapsed_and_terminal_only_after_300_seconds() -> None:
    assert exact.EXIT_RETRY_ELAPSED_SECONDS == (0, 10, 30, 60, 120, 300)
    first = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    assert [
        int((exact._retry_at(first, attempt) - first).total_seconds())
        for attempt in range(1, 6)
    ] == [10, 30, 60, 120, 300]
    assert exact._retry_at(first, 6) is None
    assert exact.TERMINAL_LIQUIDATION_ASSUMPTION == "total_loss_after_300s_without_executable_exact_exit"


def test_terminal_unsellable_position_is_settled_as_total_loss_not_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    followup.install_terminal_fomo_followup()
    adapter = _adapter()
    _create_final_tables(adapter)
    exact._ensure_schema(adapter)
    due = "2026-09-06T12:00:00+00:00"
    exact._upsert_liquidation(
        adapter,
        {
            "position_scope": "final",
            "source_signature": "entry-terminal",
            "token_mint": "mint-terminal",
            "actual_position_raw": 777_000,
            "entry_cost_sol": 0.50,
            "position_fraction": 0.05,
            "exit_signal_signature": "sell-terminal",
            "exit_reason": "trigger_wallet_exit_baseline",
            "exit_features_json": "{}",
            "first_exit_due_at": due,
        },
    )
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE profit_first_final_exit_liquidations SET attempt_count=5 WHERE epoch_id=? AND position_scope='final' "
            "AND source_signature='entry-terminal'",
            (adapter.epoch_id,),
        )
        liquidation = dict(
            adapter.store.db.execute(
                "SELECT * FROM profit_first_final_exit_liquidations WHERE epoch_id=? AND position_scope='final' "
                "AND source_signature='entry-terminal'",
                (adapter.epoch_id,),
            ).fetchone()
        )

    async def failed_exit(_adapter: object, *, token_mint: str, actual_position_raw: int) -> dict[str, object]:
        return {
            "attempted_at": "2026-09-06T12:05:00+00:00",
            "quote_input_raw": actual_position_raw,
            "amount_match": True,
            "route_hops": [],
            "quote_age_ms": 1.0,
            "token_account_requirements": {},
            "transaction_built": False,
            "simulation_ok": False,
            "simulation_error_class": "token_restriction",
            "units_consumed": None,
            "simulation_slot": None,
            "logs_count": 1,
            "route_valid": False,
            "token_restriction": True,
            "account_failure": False,
            "transfer_failure": True,
            "signature_fee_lamports": 0,
            "prioritization_fee_lamports": 0,
            "rent_fee_lamports": 0,
            "total_fee_lamports": 0,
            "error": "account frozen",
            "exit_net_sol": None,
        }

    monkeypatch.setattr(exact, "observe_exact_exit_order", failed_exit)
    asyncio.run(exact._attempt_liquidation(adapter, liquidation))

    with adapter.store._lock:
        state = adapter.store.db.execute(
            "SELECT status,eventual_exit_net_sol,terminal_assumption FROM profit_first_final_exit_liquidations "
            "WHERE epoch_id=? AND position_scope='final' AND source_signature='entry-terminal'",
            (adapter.epoch_id,),
        ).fetchone()
        outcome = adapter.store.db.execute(
            "SELECT net_return,exit_net_sol,exit_reason FROM profit_first_final_outcomes "
            "WHERE epoch_id=? AND source_signature='entry-terminal' AND lane='unified_profit_maximizer'",
            (adapter.epoch_id,),
        ).fetchone()
        model = adapter.store.db.execute(
            "SELECT execution_model_epoch,position_scope FROM profit_first_final_outcome_execution_models "
            "WHERE epoch_id=? AND source_signature='entry-terminal' AND lane='unified_profit_maximizer'",
            (adapter.epoch_id,),
        ).fetchone()
    assert state["status"] == "paper_exit_terminal_unexitable"
    assert state["eventual_exit_net_sol"] == 0.0
    assert state["terminal_assumption"] == exact.TERMINAL_LIQUIDATION_ASSUMPTION
    assert outcome["net_return"] == -1.0
    assert outcome["exit_net_sol"] == 0.0
    assert "terminal_unexitable_total_loss" in outcome["exit_reason"]
    assert model["execution_model_epoch"] == followup.ACTIVE_EXECUTION_MODEL_EPOCH
    assert model["position_scope"] == "final"


def test_fomo_liquidation_uses_its_own_scaled_raw_position() -> None:
    adapter = _adapter()
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE fomo_paper_trials(release_commit TEXT,source_signature TEXT,decision TEXT,position_fraction REAL)"
        )
        adapter.store.db.execute(
            "INSERT INTO fomo_paper_trials VALUES (?,?,?,?)",
            (adapter.release_commit, "entry-fomo", "paper_enter_bootstrap_probe", 0.02),
        )
    final_trial = {
        "source_signature": "entry-fomo",
        "assigned_position_fraction": 0.10,
        "entry_token_raw": 10_000,
        "quote_input_lamports": 1_000_000_000,
        "entry_fee_lamports": 10_000,
    }
    payload = exact._fomo_liquidation_payload(
        adapter,
        final_trial,
        {
            "source_signature": "entry-fomo",
            "token_mint": "mint-fomo",
            "exit_signal_signature": "sell-fomo",
            "exit_reason": "trigger_wallet_exit_baseline",
            "exit_features_json": "{}",
            "first_exit_due_at": "2026-09-06T12:00:00+00:00",
        },
    )
    assert payload is not None
    assert payload["position_scope"] == "fomo"
    assert payload["actual_position_raw"] == 2_000
    assert payload["actual_position_raw"] != final_trial["entry_token_raw"]


def test_fomo_learning_rows_use_size_specific_paper_outcome_not_shadow_canonical_return() -> None:
    adapter = _adapter()
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE fomo_shadow_observations("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT,source_signature TEXT,venue TEXT,lifecycle TEXT,"
            "regime TEXT,state_json TEXT)"
        )
        adapter.store.db.execute(
            "CREATE TABLE fomo_shadow_outcomes("
            "release_commit TEXT,source_signature TEXT,net_return REAL)"
        )
        adapter.store.db.execute(
            "CREATE TABLE fomo_paper_outcomes("
            "release_commit TEXT,source_signature TEXT,trigger_wallet TEXT,net_return REAL)"
        )
        adapter.store.db.execute(
            "CREATE TABLE fomo_paper_outcome_execution_models("
            "release_commit TEXT,source_signature TEXT,execution_model_epoch TEXT)"
        )
        adapter.store.db.execute(
            "INSERT INTO fomo_shadow_observations(release_commit,source_signature,venue,lifecycle,regime,state_json) "
            "VALUES (?,?,?,?,?,?)",
            (adapter.release_commit, "entry-fomo", "PUMPSWAP", "early", "hot", '{"state":"active_fomo"}'),
        )
        adapter.store.db.execute(
            "INSERT INTO fomo_shadow_outcomes VALUES (?,?,?)",
            (adapter.release_commit, "entry-fomo", 4.0),
        )
        adapter.store.db.execute(
            "INSERT INTO fomo_paper_outcomes VALUES (?,?,?,?)",
            (adapter.release_commit, "entry-fomo", "wallet-fomo", -0.25),
        )
        adapter.store.db.execute(
            "INSERT INTO fomo_paper_outcome_execution_models VALUES (?,?,?)",
            (adapter.release_commit, "entry-fomo", followup.ACTIVE_EXECUTION_MODEL_EPOCH),
        )
    rows = followup._forward_fomo_rows_active(adapter)
    assert len(rows) == 1
    assert rows[0]["net_return"] == -0.25
    assert rows[0]["net_return"] != 4.0


def test_followup_has_no_internal_settlement_monkeypatch_bridges() -> None:
    assert not hasattr(followup, "_ORIGINAL_RECORD_OUTCOME_MODEL")
    assert not hasattr(followup, "_ORIGINAL_SETTLE_FOMO")
    assert not hasattr(followup, "_ORIGINAL_SYNC_V5")
    payload = followup.status()
    assert payload["single_active_exit_engine"] is True
    assert payload["internal_settlement_monkeypatches"] is False


def test_fresh_production_composition_activates_one_terminal_fomo_engine() -> None:
    script = r'''
import importlib
import solana_roi.production  # noqa: F401
from solana_roi import v51_exact_exit_execution as exact
from solana_roi import v51_exit_execution_integrity as previous_v2
from solana_roi import v51_exit_execution_terminal_fomo_followup as followup
from solana_roi import v51_measurement_integrity as measurement
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter

assert followup._INSTALLED is True
assert exact._INSTALLED is True
assert previous_v2._INSTALLED is False
assert exact.EXACT_EXIT_EXECUTION_MODEL_EPOCH == followup.ACTIVE_EXECUTION_MODEL_EPOCH
assert measurement.EXECUTION_MODEL_EPOCH == followup.ACTIVE_EXECUTION_MODEL_EPOCH
assert getattr(FinalProfitFirstResearchAdapter.observe, "_roi_fomo_runtime", False) is True

# Prove the exact-exit sell is reachable through the actual fresh production
# wrapper chain. Later strategy/learning wrappers are allowed, but every wrapper
# must delegate through its captured original until the one active exact-exit
# function is reached.
current = FinalProfitFirstResearchAdapter._sell
seen = set()
chain = []
found = False
for _ in range(16):
    if not callable(current) or id(current) in seen:
        break
    seen.add(id(current))
    chain.append(f"{current.__module__}.{current.__name__}")
    if getattr(current, "_roi_exact_exit_execution_v3_terminal_fomo", False):
        found = True
        break
    module = importlib.import_module(current.__module__)
    delegates = [
        value
        for name, value in vars(module).items()
        if name.startswith("_ORIGINAL") and "SELL" in name and callable(value)
        and value is not current and id(value) not in seen
    ]
    if len(delegates) != 1:
        break
    current = delegates[0]
assert found, "sell delegation chain did not reach active exact exit: " + " -> ".join(chain)
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        "fresh production composition did not activate one terminal/FOMO exact-exit engine\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
