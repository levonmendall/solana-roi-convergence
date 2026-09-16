from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from threading import RLock
from types import SimpleNamespace

from solana_roi.storage_retention_policy import POLICIES_BY_TABLE
from solana_roi.strategy_v52_authority import target_sizing_policy
from solana_roi.v52_wallet_forward_alpha import (
    STATUS_MATERIAL,
    STATUS_NO_VALUE,
    ValidationWindowResult,
    WalletForwardAlphaEngine,
    WalletForwardValidationReport,
)
from solana_roi.v52_wallet_forward_alpha_runtime import (
    _VALIDATION_INTERVAL_SECONDS,
    WalletForwardAlphaRuntime,
)
from solana_roi.v52_wallet_forward_retention import (
    REPLAY_HISTORY_LIMIT,
    REPLAY_PRUNE_BATCH,
    VALIDATION_HEARTBEAT_SECONDS,
    prune_replay_history,
    should_persist_validation,
)


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = RLock()
        self.appended: list[tuple[object, ...]] = []

    def append(self, *args: object) -> None:
        self.appended.append(args)


def _window(window: str, *, accepted: bool = True, observations: int | None = None) -> ValidationWindowResult:
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    return ValidationWindowResult(
        window=window,
        observations=minimum if observations is None else observations,
        baseline_mean_return=0.01,
        current_wallet_mean_return=0.02,
        forward_alpha_mean_return=0.03,
        forward_vs_baseline_mean=0.02,
        forward_vs_current_mean=0.01,
        forward_vs_baseline_lower_95=0.01,
        forward_vs_current_lower_95=0.0,
        baseline_max_drawdown=0.05,
        current_wallet_max_drawdown=0.04,
        forward_alpha_max_drawdown=0.03,
        leakage_failures=0,
        realism_failures=0,
        accepted=accepted,
    )


def _report(
    *,
    status: str = STATUS_MATERIAL,
    enabled: bool = True,
    scope: tuple[str, ...] = ("bounded_sizing",),
    reasons: tuple[str, ...] = (),
    accepted_24h: bool = True,
    observation_delta: int = 0,
) -> WalletForwardValidationReport:
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    return WalletForwardValidationReport(
        status=status,
        windows=(
            _window("24h", accepted=accepted_24h, observations=minimum + observation_delta),
            _window("7d", observations=minimum + observation_delta),
            _window("30d", observations=minimum + observation_delta),
        ),
        strategy_influence_enabled=enabled,
        influence_scope=scope,
        reasons=reasons,
    )


def test_replay_history_prunes_only_a_bounded_batch_then_drains_to_latest_five() -> None:
    store = _Store()
    runtime = WalletForwardAlphaRuntime(store, SimpleNamespace(), started_at=datetime.now(timezone.utc) - timedelta(days=31))
    with store._lock, store.db:
        for i in range(300):
            store.db.execute(
                "INSERT INTO v52_wallet_forward_replay_runs(evaluated_at,runtime_started_at,status,strategy_influence_enabled,report_json,paper_only,live_money_authority) VALUES (?,?,?,?,?,1,0)",
                (f"2026-09-01T00:{i % 60:02d}:00+00:00", runtime.started_at.isoformat(), "test", 0, "{}"),
            )
    assert prune_replay_history(store) == REPLAY_PRUNE_BATCH
    assert store.db.execute("SELECT COUNT(*) FROM v52_wallet_forward_replay_runs").fetchone()[0] == 300 - REPLAY_PRUNE_BATCH
    while store.db.execute("SELECT COUNT(*) FROM v52_wallet_forward_replay_runs").fetchone()[0] > REPLAY_HISTORY_LIMIT:
        prune_replay_history(store)
    ids = [row[0] for row in store.db.execute("SELECT id FROM v52_wallet_forward_replay_runs ORDER BY id")]
    assert len(ids) == REPLAY_HISTORY_LIMIT
    assert ids == [296, 297, 298, 299, 300]


def test_runtime_replay_writer_stays_bounded_and_status_returns_newest_report() -> None:
    store = _Store()
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    runtime = WalletForwardAlphaRuntime(store, SimpleNamespace(), started_at=start)
    last = None
    for minute in range(12):
        last = runtime.run_real_validation(as_of=datetime(2026, 9, 15, 12, minute, tzinfo=timezone.utc), persist_if_complete=False)
    count = store.db.execute("SELECT COUNT(*) FROM v52_wallet_forward_replay_runs").fetchone()[0]
    assert count == REPLAY_HISTORY_LIMIT
    status = runtime.status()
    assert status["real_three_way_replay"]["evaluated_at"] == last["evaluated_at"]


def test_unchanged_validation_ignores_drifting_metrics_until_hourly_milestone() -> None:
    store = _Store()
    engine = WalletForwardAlphaEngine(store)
    t0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)
    original = _report()
    assert should_persist_validation(store, original, t0)
    engine.persist_validation(original, evaluated_at=t0)

    metric_drift_only = _report(observation_delta=7)
    assert not should_persist_validation(store, metric_drift_only, t0 + timedelta(minutes=1))
    assert not should_persist_validation(
        store,
        metric_drift_only,
        t0 + timedelta(seconds=VALIDATION_HEARTBEAT_SECONDS - 1),
    )
    assert should_persist_validation(
        store,
        metric_drift_only,
        t0 + timedelta(seconds=VALIDATION_HEARTBEAT_SECONDS),
    )


def test_validation_semantic_transitions_persist_immediately() -> None:
    store = _Store()
    engine = WalletForwardAlphaEngine(store)
    t0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)
    original = _report()
    engine.persist_validation(original, evaluated_at=t0)

    transitions = (
        _report(status=STATUS_NO_VALUE, enabled=False, scope=()),
        _report(enabled=False),
        _report(scope=("candidate_ranking", "bounded_sizing")),
        _report(reasons=("new_governed_reason",)),
        _report(accepted_24h=False),
    )
    for changed in transitions:
        assert should_persist_validation(store, changed, t0 + timedelta(minutes=1))


def test_crossing_minimum_sample_boundary_is_semantic_even_when_acceptance_is_unchanged() -> None:
    store = _Store()
    engine = WalletForwardAlphaEngine(store)
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    t0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)
    below = WalletForwardValidationReport(
        status=STATUS_NO_VALUE,
        windows=(
            _window("24h", accepted=False, observations=max(0, minimum - 1)),
            _window("7d", accepted=False, observations=max(0, minimum - 1)),
            _window("30d", accepted=False, observations=max(0, minimum - 1)),
        ),
        strategy_influence_enabled=False,
        influence_scope=(),
        reasons=("insufficient_samples",),
    )
    engine.persist_validation(below, evaluated_at=t0)
    crossed = WalletForwardValidationReport(
        status=STATUS_NO_VALUE,
        windows=(
            _window("24h", accepted=False, observations=minimum),
            _window("7d", accepted=False, observations=minimum),
            _window("30d", accepted=False, observations=minimum),
        ),
        strategy_influence_enabled=False,
        influence_scope=(),
        reasons=("insufficient_samples",),
    )
    assert should_persist_validation(store, crossed, t0 + timedelta(minutes=1))


def test_validation_calculation_cadence_is_unchanged() -> None:
    assert _VALIDATION_INTERVAL_SECONDS == 60.0


def test_retention_registry_protects_unproven_and_canonical_evidence() -> None:
    assert POLICIES_BY_TABLE["v52_wallet_forward_replay_runs"].mode == "latest_n"
    assert POLICIES_BY_TABLE["v52_wallet_forward_replay_runs"].value == 5
    assert POLICIES_BY_TABLE["v52_wallet_forward_replay_runs"].enforcement == "writer"
    assert POLICIES_BY_TABLE["v52_wallet_forward_validation"].enforcement == "writer"

    for table in (
        "wallet_discovery_forward_observations",
        "wallet_intelligence_snapshots",
        "risk_evidence",
        "normalized_swaps",
    ):
        assert POLICIES_BY_TABLE[table].enforcement == "protected"

    for table in (
        "certification_replication_changes",
        "direct_solana_minute_receipts",
        "wallet_discovery_broad_samples",
        "wallet_discovery_candidates",
        "risk_refresh_measurements",
        "program_coverage_observations",
        "helius_webhook_inbox",
    ):
        assert POLICIES_BY_TABLE[table].enforcement == "policy_only"

    receipts = POLICIES_BY_TABLE["direct_solana_recent_receipts"]
    assert receipts.enforcement == "writer_and_maintenance"
    assert receipts.value == (
        "durably completed hydration plus canonical normalization/wallet cursor, "
        "or consumed raw cursor, and 120s; unresolved gap protected"
    )
