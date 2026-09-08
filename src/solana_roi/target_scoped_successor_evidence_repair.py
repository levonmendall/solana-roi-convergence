from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from . import live_poll_redundancy as live_poll
from . import same_release_continuity_epoch_repair as successor
from . import strategy_relevant_continuity as continuity
from . import target_stream_fanout as fanout


REPAIR_VERSION = "target-scoped-successor-evidence-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_FRESH_WEBSOCKET_EVIDENCE: Callable[[Any, str], tuple[bool, str | None]] | None = None
_INSTALLED = False


def _latest(value_a: str | None, value_b: str | None) -> str | None:
    values = [value for value in (value_a, value_b) if value]
    if not values:
        return None
    return max(values, key=lambda value: datetime.fromisoformat(value))


def _gap_floors(self: Any, epoch: Any, target_keys: set[str]) -> tuple[dict[str, str], str | None]:
    """Return latest real gap boundary per scout plus any non-target/global boundary.

    A failed strategy epoch can accumulate later gaps while it is waiting for a
    successor. Those gaps are still authoritative, but a gap on scout A must not
    force an unrelated scout B that stayed continuously covered to disconnect and
    reconnect merely to manufacture a newer ``last_change_at`` timestamp.
    """

    store = successor._store(self)
    if store is None:
        raise RuntimeError("strategy continuity store unavailable")
    successor._ensure_schema(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT target_key,gap_started_at FROM direct_solana_strategy_continuity_gap_event "
            "WHERE release_commit=? AND epoch_id=? ORDER BY id",
            (successor._release_commit(), int(epoch["epoch_id"])),
        ).fetchall()

    per_target: dict[str, str] = {}
    global_floor: str | None = None
    for row in rows:
        key = str(row["target_key"] or "")
        boundary = successor._iso(row["gap_started_at"])
        if not boundary:
            continue
        if key in target_keys:
            per_target[key] = _latest(per_target.get(key), boundary) or boundary
        else:
            # Legacy/untracked global gap evidence is intentionally conservative:
            # every scout must provide a post-boundary reconnect before recovery.
            global_floor = _latest(global_floor, boundary)
    return per_target, global_floor


def _target_scoped_fresh_websocket_evidence(
    self: Any,
    boundary: str,
) -> tuple[bool, str | None]:
    if _ORIGINAL_FRESH_WEBSOCKET_EVIDENCE is None:
        return False, None

    epoch = successor._latest_epoch(self)
    if epoch is None or str(epoch["state"]) != "failed":
        return _ORIGINAL_FRESH_WEBSOCKET_EVIDENCE(self, boundary)

    strategy_targets = tuple(
        target for target in (getattr(self, "watch_targets", ()) or ()) if target.kind == "scout"
    )
    if not strategy_targets:
        return False, None
    target_keys = {continuity._target_key(target) for target in strategy_targets}

    try:
        per_target_floor, global_floor = _gap_floors(self, epoch, target_keys)
        _lock, provider_targets, _events, states = fanout._state_maps(self)
        satisfied_at: list[str] = []

        for target in strategy_targets:
            key = continuity._target_key(target)
            target_floor = _latest(per_target_floor.get(key), global_floor)
            connected_times: list[str] = []
            fresh_times: list[str] = []

            for provider, live_targets in provider_targets.items():
                if provider == live_poll.POLL_PROVIDER_NAME or key not in set(live_targets):
                    continue
                provider_state = states.get(provider, {})
                row = provider_state.get(key) if isinstance(provider_state, dict) else None
                if not isinstance(row, dict) or not bool(row.get("connected")):
                    continue
                changed = successor._iso(row.get("last_change_at"))
                if not changed:
                    continue
                connected_times.append(changed)
                if target_floor is None or successor._after(changed, target_floor):
                    fresh_times.append(changed)

            if not connected_times:
                return False, None
            if target_floor is not None and not fresh_times:
                # This scout actually lost all real WS coverage (or belongs to a
                # legacy/global gap) and therefore must prove a post-gap reconnect.
                return False, None

            # A scout with no gap event during this failed episode may keep its
            # pre-failure connection: the absence of a generation loss plus current
            # connected state is continuity evidence, not stale evidence.
            evidence_times = fresh_times or connected_times
            satisfied_at.append(max(evidence_times, key=lambda value: datetime.fromisoformat(value)))

        return True, max(satisfied_at, key=lambda value: datetime.fromisoformat(value))
    except Exception:
        # Any inability to prove target-scoped history falls back to the older,
        # stricter all-target post-gap rule. Never fail open on diagnostic failure.
        return _ORIGINAL_FRESH_WEBSOCKET_EVIDENCE(self, boundary)


def install_target_scoped_successor_evidence_repair() -> None:
    global _ORIGINAL_FRESH_WEBSOCKET_EVIDENCE, _INSTALLED
    if _INSTALLED:
        return

    successor.install_same_release_continuity_epoch_repair()
    current = successor._fresh_websocket_evidence
    if not bool(getattr(current, "_roi_target_scoped_successor_evidence", False)):
        _ORIGINAL_FRESH_WEBSOCKET_EVIDENCE = current
        setattr(
            _target_scoped_fresh_websocket_evidence,
            "_roi_target_scoped_successor_evidence",
            True,
        )
        successor._fresh_websocket_evidence = _target_scoped_fresh_websocket_evidence
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "install_target_scoped_successor_evidence_repair",
]
