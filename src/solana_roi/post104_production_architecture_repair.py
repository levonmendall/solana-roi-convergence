from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from . import candidate_certification_hotpath_repair as candidate_hotpath
from . import continuity_exact_durable_signature_repair as exact
from . import continuity_high_volume_poll_affinity_repair as affinity
from . import full_scope_dispatch_capacity_repair as full_scope
from . import launch_coverage_bridge as launch_bridge
from . import production_capacity_repair as capacity
from .direct_solana import DirectSolanaIngestionPlane
from .observation import TimedRiskCollectors
from .risk import RiskDimension


CANDIDATE_ENTRY_WINDOW_SECONDS = 20.0
CANDIDATE_CONTEXT_RESERVE_SECONDS = 0.15
CANDIDATE_CREATED_AT_MAX_SECONDS = 2.5
LIVE_POLL_PROVIDER_NAMES = frozenset({"rpc-live-poll"})

_ORIGINAL_FULL_SCOPE_BATCH: Callable[..., int] | None = None
_ORIGINAL_PREFILL: Callable[..., Any] | None = None
_ORIGINAL_RISK_REFRESH: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_post104_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _actual_ws_high_volume_rows(items: list[Any]) -> list[dict[str, Any]]:
    """Return high-volume receipts that actually arrived through a WebSocket.

    The full-scope dispatcher is also reachable by compatibility paths, so the
    transport name is checked explicitly. Synthetic live-poll rows can never create
    an exact WebSocket frontier.
    """

    rows: list[dict[str, Any]] = []
    for item in items:
        try:
            _priority, _mono, _sequence, received_at, provider, _targets, _message = (
                capacity._parse_dispatch_item(item)
            )
            fields = capacity._dispatch_fields(item)
        except Exception:
            continue
        if fields is None or str(provider) in LIVE_POLL_PROVIDER_NAMES:
            continue
        target, slot, signature, _failed, source = fields
        source_key = str(source or "")
        if source_key not in affinity.HIGH_VOLUME_ROUTINE_SOURCES:
            continue
        try:
            parsed_slot = int(slot)
        except (TypeError, ValueError):
            continue
        signature = str(signature or "")
        if parsed_slot <= 0 or not signature:
            continue
        rows.append(
            {
                "signature": signature,
                "source_key": source_key,
                "slot": parsed_slot,
                "received_at": received_at,
                "provider": str(provider),
                "target_kind": str(getattr(target, "kind", "") or ""),
            }
        )
    return rows


def _durable_keys(store: Any, rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    by_source: dict[str, list[str]] = {}
    for row in rows:
        by_source.setdefault(str(row["source_key"]), []).append(str(row["signature"]))
    found: set[tuple[str, str]] = set()
    try:
        with store._lock:
            for source, signatures in by_source.items():
                unique = list(dict.fromkeys(signatures))
                for start in range(0, len(unique), 300):
                    chunk = unique[start : start + 300]
                    placeholders = ",".join("?" for _ in chunk)
                    result = store.db.execute(
                        "SELECT signature,source_key FROM direct_solana_recent_receipts "
                        f"WHERE source_key=? AND signature IN ({placeholders})",
                        (source, *chunk),
                    ).fetchall()
                    found.update((str(item["signature"]), str(item["source_key"])) for item in result)
    except Exception:
        return set()
    return found


def _publish_exact_durable_frontier(self: Any, rows: list[dict[str, Any]]) -> None:
    """Publish only receipts proven durable after the final batch transaction.

    PR #102 wrapped DirectSolanaJournal.record_receipt, but PR #67's final set-based
    writer intentionally bypasses that method. Production therefore had millions of
    durable receipts while the exact-boundary frontier stayed empty. This helper is
    attached to the final writer instead of reopening per-receipt transactions.
    """

    if not rows:
        return
    durable = _durable_keys(self.store, rows)
    if not durable:
        _inc(self, "durable_frontier_no_verified_rows")
        return

    journal = self.journal
    frontiers = exact._journal_frontiers(journal)
    # Publishing every verified receipt preserves the original PR #102 ordering
    # rule: the greatest slot wins; same-slot order remains non-authoritative and
    # omitted same-slot signatures are separately proven durable during recovery.
    published = 0
    for item in rows:
        key = (str(item["signature"]), str(item["source_key"]))
        if key not in durable:
            continue
        committed = time.monotonic()
        row = {
            "signature": str(item["signature"]),
            "slot": int(item["slot"]),
            "source_key": str(item["source_key"]),
            "received_at": item["received_at"].isoformat(),
            "committed_monotonic": committed,
            "durable": True,
            "transport": "websocket",
            "final_batch_commit": True,
        }
        current = frontiers.get(str(item["source_key"]))
        if not isinstance(current, dict) or (
            int(row["slot"]), float(row["committed_monotonic"])
        ) >= (
            int(current.get("slot", 0) or 0),
            float(current.get("committed_monotonic", 0.0) or 0.0),
        ):
            frontiers[str(item["source_key"])] = row
            published += 1

    if published:
        setattr(
            journal,
            "_roi_exact_durable_ws_frontier_updates",
            int(getattr(journal, "_roi_exact_durable_ws_frontier_updates", 0) or 0)
            + published,
        )
        _inc(self, "durable_frontier_updates", published)


def _full_scope_batch_with_exact_frontier(self: Any, items: list[Any]) -> int:
    if _ORIGINAL_FULL_SCOPE_BATCH is None:
        raise RuntimeError("post-104 final durable integration is not installed")
    ws_rows = _actual_ws_high_volume_rows(items)
    result = int(_ORIGINAL_FULL_SCOPE_BATCH(self, items))
    # The delegated call has exited `with store.db`, so SQLite durability precedes
    # frontier publication. A gap beginning before this point will reject this row
    # as post-gap, preserving the fail-closed generation boundary.
    _publish_exact_durable_frontier(self, ws_rows)
    return result


setattr(_full_scope_batch_with_exact_frontier, "_roi_post104_exact_final_batch", True)


def _candidate_source(candidate: Any) -> str:
    raw = str(getattr(candidate, "source", "") or "").upper()
    for source in ("PUMP_FUN", "PUMP_AMM", "RAYDIUM"):
        if source in raw:
            return source
    return raw or "SCOUT_CANDIDATE"


async def _candidate_targeted_context_prefill(self: Any, candidate: Any) -> bool:
    """Acquire only mint-specific launch context when a scout arrives before attestation.

    The existing hot-path repair correctly removed the old source-wide 600-signature
    fanout. Its fail-closed fallback, however, left a new scout with no mechanism to
    obtain the three early buyers/flow/funding observations that six-dimension risk
    needs. Use the already-existing mint-address launch bridge, bounded by the same
    20-second candidate window, without reopening source-wide history.
    """

    if _ORIGINAL_PREFILL is None:
        raise RuntimeError("post-104 candidate context repair is not installed")
    existing = bool(await _ORIGINAL_PREFILL(self, candidate))
    if existing or not candidate_hotpath._is_frozen_scout_buy(self, candidate):
        return existing

    observed_at = getattr(candidate, "observed_at", None)
    if not isinstance(observed_at, datetime):
        return False
    now = _utcnow()
    remaining = (
        CANDIDATE_ENTRY_WINDOW_SECONDS
        - max(0.0, (now - observed_at).total_seconds())
        - CANDIDATE_CONTEXT_RESERVE_SECONDS
    )
    if remaining <= 0.0:
        _inc(self, "candidate_context_skipped_after_entry_window")
        return False

    mint = str(getattr(candidate, "token_mint", "") or "")
    if not mint:
        return False
    raw_collectors = launch_bridge._raw_collectors(self)
    launch = getattr(raw_collectors, "launch", None)
    created_fn = getattr(launch, "_created_at", None)
    if not callable(created_fn):
        _inc(self, "candidate_context_launch_collector_unavailable")
        return False

    _inc(self, "candidate_context_attempted")
    try:
        created_at = await asyncio.wait_for(
            created_fn(mint),
            timeout=max(0.05, min(CANDIDATE_CREATED_AT_MAX_SECONDS, remaining)),
        )
    except asyncio.TimeoutError:
        _inc(self, "candidate_context_created_at_timeout")
        return False
    except asyncio.CancelledError:
        raise
    except Exception:
        _inc(self, "candidate_context_created_at_error")
        return False
    if not isinstance(created_at, datetime):
        _inc(self, "candidate_context_created_at_missing")
        return False

    launch_bridge._seed_launch_created_at(self, mint, created_at)
    remaining = (
        CANDIDATE_ENTRY_WINDOW_SECONDS
        - max(0.0, (_utcnow() - observed_at).total_seconds())
        - CANDIDATE_CONTEXT_RESERVE_SECONDS
    )
    if remaining <= 0.0:
        _inc(self, "candidate_context_skipped_after_created_at")
        return False

    try:
        count, complete, candidate_count = await asyncio.wait_for(
            launch_bridge._hydrate_mint_launch_context(
                self,
                mint=mint,
                source=_candidate_source(candidate),
                launch_signature="",
                created_at=created_at,
            ),
            timeout=remaining,
        )
    except asyncio.TimeoutError:
        _inc(self, "candidate_context_entry_window_timeout")
        return False
    except asyncio.CancelledError:
        raise
    except Exception:
        _inc(self, "candidate_context_error")
        return False

    _inc(self, "candidate_context_rows", int(count))
    _inc(self, "candidate_context_candidates", int(candidate_count))
    if bool(complete):
        _inc(self, "candidate_context_complete")
    else:
        _inc(self, "candidate_context_incomplete")
    return bool(complete and candidate_count > 0)


setattr(_candidate_targeted_context_prefill, "_roi_post104_candidate_context", True)


def _dimension_counts(obj: Any, name: str) -> Counter[str]:
    attr = f"_roi_post104_risk_{name}_dimensions"
    value = getattr(obj, attr, None)
    if isinstance(value, Counter):
        return value
    value = Counter()
    setattr(obj, attr, value)
    return value


async def _risk_refresh_with_dimension_accounting(
    self: TimedRiskCollectors,
    mint: str,
    at: datetime,
    *,
    current_swap: Any = None,
) -> None:
    if _ORIGINAL_RISK_REFRESH is None:
        raise RuntimeError("post-104 risk accounting is not installed")
    await _ORIGINAL_RISK_REFRESH(self, mint, at, current_swap=current_swap)

    try:
        eligible = current_swap is not None and self._eligible_candidate(current_swap)
    except Exception:
        eligible = False
    if not eligible:
        return
    assessed_at = self.now_fn()
    raw = self.risk.readiness(mint, as_of=assessed_at)
    if not isinstance(raw, dict):
        return
    present = raw.get("present") if isinstance(raw.get("present"), dict) else {}
    fresh = raw.get("fresh_dimensions") if isinstance(raw.get("fresh_dimensions"), dict) else {}
    missing = [d.value for d in RiskDimension if not bool(present.get(d.value))]
    stale = [
        d.value
        for d in RiskDimension
        if bool(present.get(d.value)) and not bool(fresh.get(d.value))
    ]
    if missing or stale:
        _inc(self, "risk_incomplete_measurements")
        _dimension_counts(self, "missing").update(missing)
        _dimension_counts(self, "stale").update(stale)
    else:
        _inc(self, "risk_complete_measurements")
    setattr(
        self,
        "_roi_post104_risk_last_readiness",
        {
            "token_mint": str(mint),
            "assessed_at": assessed_at.isoformat(),
            "complete": bool(raw.get("complete")),
            "fresh": bool(raw.get("fresh")),
            "missing_dimensions": missing,
            "stale_dimensions": stale,
            "present": {d.value: bool(present.get(d.value)) for d in RiskDimension},
            "fresh_dimensions": {d.value: bool(fresh.get(d.value)) for d in RiskDimension},
        },
    )


setattr(_risk_refresh_with_dimension_accounting, "_roi_post104_risk_accounting", True)


def _status_with_post104_architecture(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("post-104 architecture status is not installed")
    payload = _ORIGINAL_STATUS(self)
    collectors = getattr(getattr(self, "service", None), "collectors", None)
    payload["post104_architecture_repair"] = {
        "installed": True,
        "exact_durable_frontier_from_final_batch_commit": True,
        "exact_durable_frontier_updates_session": int(
            getattr(self, "_roi_post104_durable_frontier_updates", 0) or 0
        ),
        "durable_frontier_no_verified_rows_session": int(
            getattr(self, "_roi_post104_durable_frontier_no_verified_rows", 0) or 0
        ),
        "candidate_context_scope": "mint-address-only",
        "candidate_source_wide_history_reopened": False,
        "candidate_context_attempted_session": int(
            getattr(self, "_roi_post104_candidate_context_attempted", 0) or 0
        ),
        "candidate_context_complete_session": int(
            getattr(self, "_roi_post104_candidate_context_complete", 0) or 0
        ),
        "candidate_context_incomplete_session": int(
            getattr(self, "_roi_post104_candidate_context_incomplete", 0) or 0
        ),
        "candidate_context_rows_session": int(
            getattr(self, "_roi_post104_candidate_context_rows", 0) or 0
        ),
        "candidate_context_entry_window_timeout_session": int(
            getattr(self, "_roi_post104_candidate_context_entry_window_timeout", 0) or 0
        ),
        "risk_incomplete_measurements_session": int(
            getattr(collectors, "_roi_post104_risk_incomplete_measurements", 0) or 0
        ),
        "risk_complete_measurements_session": int(
            getattr(collectors, "_roi_post104_risk_complete_measurements", 0) or 0
        ),
        "risk_missing_dimensions_session": dict(
            _dimension_counts(collectors, "missing") if collectors is not None else {}
        ),
        "risk_stale_dimensions_session": dict(
            _dimension_counts(collectors, "stale") if collectors is not None else {}
        ),
        "last_risk_readiness": getattr(collectors, "_roi_post104_risk_last_readiness", None),
        "candidate_processing_target_seconds_unchanged": 5.0,
        "candidate_entry_window_seconds_unchanged": CANDIDATE_ENTRY_WINDOW_SECONDS,
        "recoverability_lease_seconds_unchanged": 12.0,
        "hard_recovery_bound_unchanged": "3x1000",
        "risk_thresholds_unchanged": True,
        "full_market_scope_unchanged": True,
        "historical_promotion_authority": False,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
        "live_money_authority": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "exact_durable_frontier_published_from_final_set_based_commit": True,
                "candidate_missing_launch_context_uses_mint_specific_bridge": True,
                "candidate_source_wide_history_reopened": False,
                "candidate_risk_dimension_accounting": True,
                "candidate_latency_threshold_unchanged": True,
                "candidate_entry_window_unchanged": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_post104_architecture, "_roi_post104_architecture", True)


def install_post104_production_architecture_repair() -> None:
    global _ORIGINAL_FULL_SCOPE_BATCH, _ORIGINAL_PREFILL, _ORIGINAL_RISK_REFRESH, _ORIGINAL_STATUS

    current_batch = full_scope._persist_full_scope_batch
    if not bool(getattr(current_batch, "_roi_post104_exact_final_batch", False)):
        _ORIGINAL_FULL_SCOPE_BATCH = current_batch
        try:
            _full_scope_batch_with_exact_frontier.__dict__.update(getattr(current_batch, "__dict__", {}))
        except Exception:
            pass
        setattr(_full_scope_batch_with_exact_frontier, "_roi_post104_exact_final_batch", True)
        full_scope._persist_full_scope_batch = _full_scope_batch_with_exact_frontier  # type: ignore[assignment]

    current_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current_prefill, "_roi_post104_candidate_context", False)):
        _ORIGINAL_PREFILL = current_prefill
        try:
            _candidate_targeted_context_prefill.__dict__.update(getattr(current_prefill, "__dict__", {}))
        except Exception:
            pass
        setattr(_candidate_targeted_context_prefill, "_roi_post104_candidate_context", True)
        DirectSolanaIngestionPlane._prefill_launch_context = _candidate_targeted_context_prefill  # type: ignore[method-assign]

    current_refresh = TimedRiskCollectors.refresh
    if not bool(getattr(current_refresh, "_roi_post104_risk_accounting", False)):
        _ORIGINAL_RISK_REFRESH = current_refresh
        try:
            _risk_refresh_with_dimension_accounting.__dict__.update(getattr(current_refresh, "__dict__", {}))
        except Exception:
            pass
        setattr(_risk_refresh_with_dimension_accounting, "_roi_post104_risk_accounting", True)
        TimedRiskCollectors.refresh = _risk_refresh_with_dimension_accounting  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_post104_architecture", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_post104_architecture.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_post104_architecture, "_roi_post104_architecture", True)
        DirectSolanaIngestionPlane.status = _status_with_post104_architecture  # type: ignore[method-assign]


__all__ = [
    "CANDIDATE_ENTRY_WINDOW_SECONDS",
    "_actual_ws_high_volume_rows",
    "_candidate_targeted_context_prefill",
    "_full_scope_batch_with_exact_frontier",
    "_publish_exact_durable_frontier",
    "_risk_refresh_with_dimension_accounting",
    "install_post104_production_architecture_repair",
]
