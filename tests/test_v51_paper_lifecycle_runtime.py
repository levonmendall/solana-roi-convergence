from __future__ import annotations

import asyncio

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v51_exact_exit_execution as exact
from solana_roi import v51_paper_lifecycle_runtime as lifecycle
from solana_roi.v51_atomic_paper_capital import capital_reconciliation, lifecycle_events


RELEASE = "f" * 40


@pytest.fixture(autouse=True)
def _isolate_lifecycle_runtime_state():
    # Tests intentionally close temporary ObservationEventStore instances. The
    # production lifecycle process owns one long-lived adapter, but test processes
    # reuse this module across cases, so never let one closed test adapter leak into
    # the next status read.
    lifecycle._ACTIVE_ADAPTER = None
    yield
    lifecycle._ACTIVE_ADAPTER = None


class DummyAdapter:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.release_commit = RELEASE
        self.epoch_id = "paper-lifecycle-test-epoch"


def _schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "selected INTEGER NOT NULL,decision TEXT NOT NULL,decision_reason TEXT NOT NULL,position_fraction REAL NOT NULL,"
            "lane TEXT NOT NULL,token_mint TEXT NOT NULL,entry_token_raw INTEGER,entry_cost_sol REAL)"
        )
        store.db.execute(
            "CREATE TABLE fomo_paper_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "decision TEXT NOT NULL,decision_reason TEXT NOT NULL,position_fraction REAL NOT NULL,"
            "token_mint TEXT NOT NULL,entry_token_raw INTEGER,entry_cost_sol REAL)"
        )
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "exit_signature TEXT NOT NULL,net_return REAL NOT NULL,settled_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "exit_signature TEXT NOT NULL,net_return REAL NOT NULL,settled_at TEXT NOT NULL)"
        )


def test_selected_entry_becomes_exact_atomic_open_and_settles_once(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "lifecycle.sqlite3")
    _schema(store)
    adapter = DummyAdapter(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_trials("
            "release_commit,source_signature,selected,decision,decision_reason,position_fraction,lane,token_mint,entry_token_raw,entry_cost_sol) "
            "VALUES (?,?,1,'paper_enter','selected',0.40,'raydium_cross_venue_persistence','TOKEN',1000,1.25)",
            (RELEASE, "sol-entry"),
        )

    assert lifecycle.sync_entry_reservations(adapter, "sol-entry") == 1
    first = capital_reconciliation(store, release_commit=RELEASE)
    assert first["active_reserved_fraction"] == pytest.approx(0.40)
    assert first["settlement_count"] == 0
    rows = lifecycle_events(store, release_commit=RELEASE, candidate_id="sol-entry")
    assert [row["stage"] for row in rows] == ["OPEN"]

    assert lifecycle.sync_entry_reservations(adapter, "sol-entry") == 1
    rows = lifecycle_events(store, release_commit=RELEASE, candidate_id="sol-entry")
    assert [row["stage"] for row in rows] == ["OPEN"]
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_outcomes("
            "release_commit,source_signature,exit_signature,net_return,settled_at) VALUES (?,?,?,?,?)",
            (RELEASE, "sol-entry", "paper-exit-1", 0.25, "2026-09-07T15:00:00+00:00"),
        )

    assert lifecycle.sync_settlements(adapter) == 1
    assert lifecycle.sync_settlements(adapter) == 0
    final = capital_reconciliation(store, release_commit=RELEASE)
    assert final["active_reserved_fraction"] == pytest.approx(0.0)
    assert final["settlement_count"] == 1
    assert final["realized_return_contribution"] == pytest.approx(0.10)
    rows = lifecycle_events(store, release_commit=RELEASE, candidate_id="sol-entry")
    assert [row["stage"] for row in rows] == ["OPEN", "CLOSED"]
    status = lifecycle.status(store, RELEASE)
    assert status["lifecycle_proven"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    store.close()


def test_exact_amount_reservation_rejects_instead_of_silent_downsize(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "contention.sqlite3")
    _schema(store)
    adapter = DummyAdapter(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_trials("
            "release_commit,source_signature,selected,decision,decision_reason,position_fraction,lane,token_mint,entry_token_raw,entry_cost_sol) "
            "VALUES (?,?,1,'paper_enter','selected',0.60,'elite_wallet_continuation','TOKEN-A',600,0.60)",
            (RELEASE, "sol-a"),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_trials("
            "release_commit,source_signature,decision,decision_reason,position_fraction,token_mint,entry_token_raw,entry_cost_sol) "
            "VALUES (?,?,'paper_enter_clean_fomo_probe','selected',0.50,'TOKEN-B',500,0.50)",
            (RELEASE, "fomo-b"),
        )

    assert lifecycle.sync_entry_reservations(adapter, "sol-a") == 1
    assert lifecycle.sync_entry_reservations(adapter, "fomo-b") == 0
    with store._lock:
        reservations = [
            dict(row)
            for row in store.db.execute(
                "SELECT reservation_id,status,requested_fraction,reserved_fraction FROM v51_paper_capital_reservations "
                "WHERE release_commit=? ORDER BY id",
                (RELEASE,),
            ).fetchall()
        ]
        fomo = store.db.execute(
            "SELECT decision,decision_reason,position_fraction FROM fomo_paper_trials WHERE source_signature='fomo-b'"
        ).fetchone()
    assert reservations[0]["reservation_id"] == "solana:sol-a"
    assert reservations[0]["status"] == "active"
    assert reservations[0]["reserved_fraction"] == pytest.approx(0.60)
    assert reservations[1]["reservation_id"] == "fomo:fomo-b"
    assert reservations[1]["status"] == "rejected"
    assert reservations[1]["requested_fraction"] == pytest.approx(0.50)
    assert reservations[1]["reserved_fraction"] == pytest.approx(0.0)
    assert fomo["decision"] == "no_entry_shared_paper_capital_unavailable"
    assert fomo["decision_reason"] == "shared_atomic_paper_capital_unavailable"
    assert float(fomo["position_fraction"]) == 0.0
    store.close()


def test_lifecycle_tick_advances_exit_retry_without_new_observation(tmp_path, monkeypatch) -> None:
    store = ObservationEventStore(tmp_path / "tick.sqlite3")
    _schema(store)
    adapter = DummyAdapter(store)
    calls: list[str] = []

    async def retry_due(received: DummyAdapter) -> None:
        assert received is adapter
        calls.append("retry")

    monkeypatch.setattr(exact, "_retry_due", retry_due)
    before = lifecycle.status()["retry_tick_count"]
    result = asyncio.run(lifecycle.lifecycle_tick(adapter))
    assert calls == ["retry"]
    assert result == {"entry_sync": 0, "settlement_sync": 0}
    assert lifecycle.status()["retry_tick_count"] == before + 1
    with store._lock:
        state = store.db.execute(
            "SELECT worker_running,last_tick_at,paper_only,live_money_authority "
            "FROM v51_paper_lifecycle_runtime_state WHERE release_commit=?",
            (RELEASE,),
        ).fetchone()
    assert state is not None
    assert state["last_tick_at"] is not None
    assert int(state["paper_only"]) == 1
    assert int(state["live_money_authority"]) == 0
    store.close()


def test_shadow_balance_exception_is_narrow_and_preserves_real_failures() -> None:
    base = {
        "error": "InstructionError: InsufficientFunds",
        "simulation_error_class": "account_failure",
        "amount_match": True,
        "transaction_built": True,
        "route_valid": True,
        "expected_output_lamports": 2_000_000,
        "total_fee_lamports": 5_000,
        "token_restriction": False,
        "transfer_failure": False,
    }
    # A broad InsufficientFunds result is not sufficient. Production must also
    # independently prove that this exact virtual PAPER position exceeds the real
    # shadow wallet's observed balance for the same input mint.
    assert lifecycle._proven_paper_balance_artifact(base) is False
    proven = {
        **base,
        "shadow_wallet_balance_observed": True,
        "shadow_wallet_input_balance_raw": 0,
        "paper_position_exceeds_shadow_balance": True,
    }
    assert lifecycle._proven_paper_balance_artifact(proven) is True
    assert lifecycle._proven_paper_balance_artifact({**proven, "shadow_wallet_balance_observed": False}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "shadow_wallet_input_balance_raw": None}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "paper_position_exceeds_shadow_balance": False}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "amount_match": False}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "route_valid": False}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "token_restriction": True}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "transfer_failure": True}) is False
    assert lifecycle._proven_paper_balance_artifact({**proven, "error": "custom program error"}) is False


def test_production_authority_wires_lifecycle_after_exact_exit_installation() -> None:
    import inspect
    from solana_roi import v51_production_authority as authority

    source = inspect.getsource(authority.install_v51_production_authority)
    assert "install_measurement_integrity_hardening()" in source
    assert "install_paper_lifecycle_runtime()" in source
    assert source.index("install_measurement_integrity_hardening()") < source.index("install_paper_lifecycle_runtime()")
    status = authority.status()
    assert status["paper_lifecycle_runtime"]["paper_only"] is True
    assert status["paper_lifecycle_runtime"]["live_money_authority"] is False
