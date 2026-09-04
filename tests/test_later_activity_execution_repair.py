from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import solana_roi.later_activity_execution_repair as repair


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self.db:
            self.db.execute(
                "CREATE TABLE wallet_discovery_forward_observations ("
                "signature TEXT PRIMARY KEY, wallet TEXT, token_mint TEXT, side TEXT, "
                "received_at TEXT, source TEXT, copyable INTEGER, risk_complete INTEGER, tracking_transport TEXT)"
            )


class _Adapter:
    def __init__(self) -> None:
        self.store = _Store()
        self.release_commit = "test-release"


def _insert(
    adapter: _Adapter,
    *,
    signature: str,
    token: str = "TOKEN",
    wallet: str = "WALLET",
    side: str = "buy",
    received_at: str = "2026-09-04T10:00:00+00:00",
    source: str = "wallet-realtime:PUMP_AMM",
    copyable: int = 1,
    risk_complete: int = 1,
) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature,wallet,token_mint,side,received_at,source,copyable,risk_complete,tracking_transport) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                signature,
                wallet,
                token,
                side,
                received_at,
                source,
                copyable,
                risk_complete,
                "logsSubscribe",
            ),
        )


def test_pumpswap_later_touch_is_partitioned_beyond_five_minutes(monkeypatch) -> None:
    adapter = _Adapter()
    _insert(adapter, signature="first", received_at="2026-09-04T10:00:00+00:00")
    monkeypatch.setattr(repair, "_ORIGINAL_LIFECYCLE", lambda *_args, **_kwargs: "pump_amm_post_bonding_curve")

    row = {
        "token_mint": "TOKEN",
        "received_at": "2026-09-04T10:45:00+00:00",
    }
    lifecycle = repair._lifecycle_with_observed_age(adapter, row, "PUMP_AMM")

    assert lifecycle == "pump_amm_mature_intraday_15_60m"


def test_raydium_later_touch_preserves_base_context_and_adds_age(monkeypatch) -> None:
    adapter = _Adapter()
    _insert(
        adapter,
        signature="first-ray",
        received_at="2026-09-04T01:00:00+00:00",
        source="wallet-realtime:RAYDIUM",
    )
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_LIFECYCLE",
        lambda *_args, **_kwargs: "raydium_post_pump_migration_evidence",
    )

    row = {
        "token_mint": "TOKEN",
        "received_at": "2026-09-04T08:00:01+00:00",
    }
    lifecycle = repair._lifecycle_with_observed_age(adapter, row, "RAYDIUM")

    assert lifecycle == "raydium_post_pump_migration_evidence_observed_6h_plus"


def test_first_same_venue_touch_does_not_fabricate_age(monkeypatch) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_LIFECYCLE",
        lambda *_args, **_kwargs: "raydium_native_or_migration_unproven",
    )

    row = {
        "token_mint": "NEW",
        "received_at": "2026-09-04T08:00:00+00:00",
    }

    assert (
        repair._lifecycle_with_observed_age(adapter, row, "RAYDIUM")
        == "raydium_native_or_migration_unproven"
    )


def test_durable_handoff_keeps_burst_beyond_legacy_sixteen_task_cap() -> None:
    adapter = _Adapter()
    for index in range(40):
        signature = f"sig-{index:02d}"
        _insert(adapter, signature=signature)
        assert repair._enqueue_handoff(adapter, signature) is True

    with adapter.store._lock:
        pending = int(
            adapter.store.db.execute(
                "SELECT COUNT(*) FROM later_activity_strategy_handoff "
                "WHERE release_commit=? AND state='pending'",
                (adapter.release_commit,),
            ).fetchone()[0]
        )

    assert pending == 40
    assert pending > 16


def test_noncopyable_later_buy_gets_explicit_terminal_rejection() -> None:
    adapter = _Adapter()
    _insert(adapter, signature="not-copyable", copyable=0)

    terminal, error = repair._terminal_outcome(adapter, "not-copyable")

    assert terminal == "reject_not_copyable_at_observation"
    assert error is None


def test_processing_handoffs_requeue_after_process_restart() -> None:
    adapter = _Adapter()
    _insert(adapter, signature="restart-me")
    repair._enqueue_handoff(adapter, "restart-me")
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE later_activity_strategy_handoff SET state='processing' "
            "WHERE release_commit=? AND signature=?",
            (adapter.release_commit, "restart-me"),
        )

    repair._reset_orphaned_processing(adapter)

    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT state,last_error FROM later_activity_strategy_handoff "
            "WHERE release_commit=? AND signature=?",
            (adapter.release_commit, "restart-me"),
        ).fetchone()
    assert row is not None
    assert row["state"] == "pending"
    assert row["last_error"] == "process_restart_requeued"


def test_production_composition_installs_later_activity_repair() -> None:
    path = Path(__file__).parents[1] / "src" / "solana_roi" / "robinhood_runtime_install.py"
    source = path.read_text()

    assert "install_later_activity_execution_repair" in source
    assert "install_regime_roi_wallet_authority()" in source
    assert source.index("install_regime_roi_wallet_authority()") < source.index(
        "install_later_activity_execution_repair()"
    )


def test_repair_retains_paper_only_boundary() -> None:
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert repair.DURABLE_HANDOFF_WORKERS <= 4
    assert repair.MAX_HANDOFF_ATTEMPTS == 3
