from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping

from .storage_retention_policy import POLICIES_BY_TABLE
from .strategy_v52_authority import target_sizing_policy
from .v52_wallet_forward_alpha import WalletForwardValidationReport

REPLAY_HISTORY_LIMIT = int(POLICIES_BY_TABLE["v52_wallet_forward_replay_runs"].value or 5)
REPLAY_PRUNE_BATCH = 128
VALIDATION_HEARTBEAT_SECONDS = float(POLICIES_BY_TABLE["v52_wallet_forward_validation"].value or 3600.0)


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _window_semantics(value: Mapping[str, Any], minimum_samples: int) -> tuple[Any, ...]:
    return (
        str(value.get("window") or ""),
        int(value.get("observations") or 0) >= minimum_samples,
        bool(value.get("accepted")),
        int(value.get("leakage_failures") or 0) > 0,
        int(value.get("realism_failures") or 0) > 0,
    )


def validation_semantics(report: WalletForwardValidationReport) -> tuple[Any, ...]:
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    windows = sorted(
        (_window_semantics(asdict(window), minimum) for window in report.windows),
        key=lambda item: item[0],
    )
    return (
        str(report.status),
        bool(report.strategy_influence_enabled),
        tuple(sorted(str(value) for value in report.influence_scope)),
        tuple(sorted(str(value) for value in report.reasons)),
        tuple(windows),
    )


def _stored_validation_semantics(row: Mapping[str, Any]) -> tuple[Any, ...]:
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    windows_raw = json.loads(str(row["windows_json"]))
    scope_raw = json.loads(str(row["influence_scope_json"]))
    reasons_raw = json.loads(str(row["reasons_json"]))
    windows = sorted(
        (_window_semantics(dict(value), minimum) for value in windows_raw),
        key=lambda item: item[0],
    )
    return (
        str(row["status"]),
        bool(row["strategy_influence_enabled"]),
        tuple(sorted(str(value) for value in scope_raw)),
        tuple(sorted(str(value) for value in reasons_raw)),
        tuple(windows),
    )


def should_persist_validation(
    store: Any,
    report: WalletForwardValidationReport,
    evaluated_at: datetime,
    *,
    heartbeat_seconds: float = VALIDATION_HEARTBEAT_SECONDS,
) -> bool:
    """Persist semantic transitions immediately and unchanged complete evidence hourly.

    Raw portfolio metrics and observation counts intentionally do not participate in
    the semantic fingerprint except for crossing the governed minimum-sample boundary.
    The validation calculation still runs at its existing cadence; this only governs
    durable diagnostic snapshots.
    """

    now = _utc(evaluated_at)
    with store._lock:
        row = store.db.execute(
            "SELECT evaluated_at,status,strategy_influence_enabled,influence_scope_json,reasons_json,windows_json "
            "FROM v52_wallet_forward_validation ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return True
    try:
        current = validation_semantics(report)
        previous = _stored_validation_semantics(dict(row))
        if current != previous:
            return True
        last = _utc(row["evaluated_at"])
        return (now - last).total_seconds() >= max(1.0, float(heartbeat_seconds))
    except Exception:
        # Preserve evidence rather than suppress a write if older/corrupt diagnostic
        # state cannot be interpreted safely.
        return True


def prune_replay_history(
    store: Any,
    *,
    history_limit: int = REPLAY_HISTORY_LIMIT,
    batch_size: int = REPLAY_PRUNE_BATCH,
) -> int:
    """Bound replay diagnostics without a history-scaled delete.

    At most ``batch_size`` stale rows are removed per invocation. A pre-existing
    backlog therefore drains incrementally while the newest ``history_limit`` rows
    are always protected.
    """

    keep = max(1, int(history_limit))
    batch = max(1, int(batch_size))
    with store._lock, store.db:
        cursor = store.db.execute(
            "DELETE FROM v52_wallet_forward_replay_runs WHERE id IN ("
            "SELECT id FROM v52_wallet_forward_replay_runs WHERE id < COALESCE(("
            "SELECT id FROM v52_wallet_forward_replay_runs ORDER BY id DESC LIMIT 1 OFFSET ?"
            "), -1) ORDER BY id ASC LIMIT ?)",
            (keep - 1, batch),
        )
    return max(0, int(cursor.rowcount))


__all__ = [
    "REPLAY_HISTORY_LIMIT",
    "REPLAY_PRUNE_BATCH",
    "VALIDATION_HEARTBEAT_SECONDS",
    "prune_replay_history",
    "should_persist_validation",
    "validation_semantics",
]
