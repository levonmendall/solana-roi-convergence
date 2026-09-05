from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

from . import robinhood_entity_universe as universe
from . import robinhood_pumpfun_wallet_intelligence as intelligence
from . import robinhood_pumpfun_wallet_intelligence_integration as integration
from . import robinhood_pumpfun_wallet_selection as selection
from . import robinhood_wallet_intelligence_v51_alignment as v51_alignment


REPAIR_VERSION = "robinhood-wallet-selection-authority-boundary-v1"
WRONG_INTELLIGENCE_REJECTION_PREFIX = "Pump.fun-equivalent copyable forward evidence failed:"

_ORIGINAL_POLL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(db: Any, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _columns(db: Any, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _row_count(db: Any, table: str) -> int:
    if not _table_exists(db, table):
        return 0
    row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] if row is not None else 0)


def _candidate_actor_set(db: Any) -> set[str]:
    if not _table_exists(db, "robinhood_wallet_selection_candidates"):
        return set()
    return {
        str(row[0])
        for row in db.execute(
            "SELECT actor FROM robinhood_wallet_selection_candidates ORDER BY actor"
        ).fetchall()
    }


def _active_actor_set(db: Any) -> set[str]:
    if not _table_exists(db, "robinhood_wallet_selection_candidates"):
        return set()
    return {
        str(row[0])
        for row in db.execute(
            "SELECT actor FROM robinhood_wallet_selection_candidates "
            "WHERE state IN ('seed_tracking','tracking') ORDER BY actor"
        ).fetchall()
    }


def _ensure_audit_schema(db: Any) -> None:
    db.execute(
        "CREATE TABLE IF NOT EXISTS robinhood_wallet_authority_boundary_audit ("
        "repair_version TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, "
        "prior_state TEXT NOT NULL, restored_state TEXT, seed_label TEXT, prior_last_error TEXT, "
        "repaired_at TEXT NOT NULL, PRIMARY KEY(repair_version,actor,action))"
    )


def _legacy_immediate_copy_case() -> str:
    return (
        "CASE WHEN copyable_price_eth IS NOT NULL AND copyable_price_eth>0 "
        "AND chase_fraction IS NOT NULL AND chase_fraction>=0 "
        "AND observation_lag_ms IS NOT NULL AND observation_lag_ms>=0 "
        "AND chase_fraction<=? AND observation_lag_ms<=? THEN 1 ELSE 0 END"
    )


def _restore_diagnostic_copy_semantics(db: Any, swap_ids: Sequence[int] | None = None) -> None:
    """Keep 15%/20s only as a legacy immediate-copy diagnostic.

    PR #169 temporarily rewrote `copyable` to mean strategy-observable. Wallet
    selection no longer consumes this table at all, so restore the durable field to
    its original immediate-copy meaning for clean telemetry and audit continuity.
    """

    if not _table_exists(db, "robinhood_wallet_intelligence_forward"):
        return
    columns = _columns(db, "robinhood_wallet_intelligence_forward")
    where = ""
    params: list[Any] = [
        float(intelligence.MAX_CHASE_FRACTION),
        float(intelligence.MAX_OBSERVATION_LAG_SECONDS) * 1000.0,
    ]
    if swap_ids is not None:
        ids = sorted({int(value) for value in swap_ids})
        if not ids:
            return
        where = " WHERE swap_id IN (" + ",".join("?" for _ in ids) + ")"
        params.extend(ids)

    case = _legacy_immediate_copy_case()
    assignments = [f"copyable={case}"]
    # `immediate_copyable` was added by PR #169. If it exists on a persisted disk,
    # keep it as an exact compatibility mirror rather than leaving mixed semantics.
    if "immediate_copyable" in columns:
        assignments.append("immediate_copyable=copyable")

    db.execute(
        "UPDATE robinhood_wallet_intelligence_forward SET " + ",".join(assignments) + where,
        tuple(params),
    )
    if "immediate_copyable" in columns:
        # SQLite evaluates assignments from the original row, so mirror in a second
        # bounded update after `copyable` has been restored.
        db.execute(
            "UPDATE robinhood_wallet_intelligence_forward SET immediate_copyable=copyable" + where,
            tuple(params[2:]) if swap_ids is not None else (),
        )


def _repair_persisted_authority_state(db: Any) -> dict[str, Any]:
    """Undo only state changes produced by the misplaced intelligence authority.

    No candidate, swap, broad-sample, forward-observation or intelligence row is
    deleted. Legitimate `mature forward geometric value nonpositive` rejections from
    the wallet-selection layer remain untouched.
    """

    _ensure_audit_schema(db)
    tables = (
        "robinhood_wallet_selection_candidates",
        "robinhood_wallet_selection_broad_samples",
        "robinhood_wallet_selection_forward",
        "robinhood_wallet_intelligence_forward",
    )
    before_counts = {table: _row_count(db, table) for table in tables}
    before_candidates = _candidate_actor_set(db)
    active_before = _active_actor_set(db)
    now = _utcnow()

    if _table_exists(db, "robinhood_wallet_selection_candidates"):
        rows = db.execute(
            "SELECT actor,state,seed_label,last_error FROM robinhood_wallet_selection_candidates "
            "WHERE state IN ('seed_tracking','tracking') ORDER BY actor"
        ).fetchall()
        for row in rows:
            db.execute(
                "INSERT OR IGNORE INTO robinhood_wallet_authority_boundary_audit("
                "repair_version,actor,action,prior_state,restored_state,seed_label,prior_last_error,repaired_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    REPAIR_VERSION,
                    str(row[0]),
                    "preserved_active_tracking",
                    str(row[1]),
                    str(row[1]),
                    row[2],
                    row[3],
                    now,
                ),
            )

        wrong = db.execute(
            "SELECT actor,state,seed_label,last_error FROM robinhood_wallet_selection_candidates "
            "WHERE state='forward_rejected' AND last_error LIKE ? ORDER BY actor",
            (WRONG_INTELLIGENCE_REJECTION_PREFIX + "%",),
        ).fetchall()
        for row in wrong:
            actor = str(row[0])
            restored = "seed_tracking" if row[2] else "tracking"
            db.execute(
                "INSERT OR IGNORE INTO robinhood_wallet_authority_boundary_audit("
                "repair_version,actor,action,prior_state,restored_state,seed_label,prior_last_error,repaired_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    REPAIR_VERSION,
                    actor,
                    "restored_misplaced_intelligence_rejection",
                    str(row[1]),
                    restored,
                    row[2],
                    row[3],
                    now,
                ),
            )
            db.execute(
                "UPDATE robinhood_wallet_selection_candidates SET state=?,last_error=NULL WHERE actor=?",
                (restored, actor),
            )

    _restore_diagnostic_copy_semantics(db)

    after_counts = {table: _row_count(db, table) for table in tables}
    after_candidates = _candidate_actor_set(db)
    active_after = _active_actor_set(db)

    if before_counts != after_counts:
        raise RuntimeError("wallet authority repair changed durable row counts")
    if before_candidates != after_candidates:
        raise RuntimeError("wallet authority repair changed candidate identities")
    if not active_before.issubset(active_after):
        raise RuntimeError("wallet authority repair removed an active tracked wallet")

    restored_rows = db.execute(
        "SELECT actor,restored_state FROM robinhood_wallet_authority_boundary_audit "
        "WHERE repair_version=? AND action='restored_misplaced_intelligence_rejection' ORDER BY actor",
        (REPAIR_VERSION,),
    ).fetchall()
    preserved_rows = db.execute(
        "SELECT actor FROM robinhood_wallet_authority_boundary_audit "
        "WHERE repair_version=? AND action='preserved_active_tracking' ORDER BY actor",
        (REPAIR_VERSION,),
    ).fetchall()
    return {
        "repair_version": REPAIR_VERSION,
        "row_counts_preserved": True,
        "candidate_identity_set_preserved": True,
        "active_tracking_set_not_reduced_by_repair": True,
        "preserved_active_tracking_count": len(preserved_rows),
        "preserved_active_tracking_actors": [str(row[0]) for row in preserved_rows],
        "restored_misplaced_intelligence_rejection_count": len(restored_rows),
        "restored_misplaced_intelligence_rejections": [
            {"actor": str(row[0]), "restored_state": str(row[1])} for row in restored_rows
        ],
        "durable_row_counts": after_counts,
    }


def _repair_instance_once(self: Any) -> dict[str, Any]:
    existing = getattr(self, "_roi_robinhood_wallet_authority_boundary_repair", None)
    if isinstance(existing, dict):
        return existing
    selection._ensure_schema(self)
    intelligence._ensure_schema(self)
    with self.store._lock, self.store.db:
        result = _repair_persisted_authority_state(self.store.db)
    setattr(self, "_roi_robinhood_wallet_authority_boundary_repair", result)
    return result


def _pending_intelligence_swap_ids(self: Any, limit: int = 250) -> list[int]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT f.swap_id FROM robinhood_wallet_selection_forward f "
            "LEFT JOIN robinhood_wallet_intelligence_forward i ON i.swap_id=f.swap_id "
            "WHERE i.swap_id IS NULL ORDER BY f.swap_id LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
    return [int(row[0]) for row in rows]


def _diagnostic_status(self: Any) -> dict[str, Any]:
    selection._ensure_schema(self)
    intelligence._ensure_schema(self)
    with self.store._lock:
        total = int(
            self.store.db.execute(
                "SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward"
            ).fetchone()[0]
        )
        immediate = int(
            self.store.db.execute(
                "SELECT COUNT(*) FROM robinhood_wallet_intelligence_forward WHERE copyable=1"
            ).fetchone()[0]
        )
        active = [
            str(row[0])
            for row in self.store.db.execute(
                "SELECT actor FROM robinhood_wallet_selection_candidates "
                "WHERE state IN ('seed_tracking','tracking') ORDER BY actor LIMIT 50"
            ).fetchall()
        ]
        restored = [
            {"actor": str(row[0]), "restored_state": str(row[1])}
            for row in self.store.db.execute(
                "SELECT actor,restored_state FROM robinhood_wallet_authority_boundary_audit "
                "WHERE repair_version=? AND action='restored_misplaced_intelligence_rejection' ORDER BY actor",
                (REPAIR_VERSION,),
            ).fetchall()
        ] if _table_exists(self.store.db, "robinhood_wallet_authority_boundary_audit") else []
    repair = getattr(self, "_roi_robinhood_wallet_authority_boundary_repair", None)
    return {
        "repair_version": REPAIR_VERSION,
        "wallet_selection_authority": "wallet_quality_and_forward_followthrough_only",
        "copyability_intelligence_mode": "diagnostic_only",
        "copyability_has_wallet_selection_authority": False,
        "copyability_has_candidate_demotion_authority": False,
        "copyability_has_paper_entry_authority": False,
        "chase_has_wallet_selection_authority": False,
        "latency_has_wallet_selection_authority": False,
        "legacy_immediate_copy_reference": {
            "max_chase_fraction": float(intelligence.MAX_CHASE_FRACTION),
            "max_observation_lag_seconds": float(intelligence.MAX_OBSERVATION_LAG_SECONDS),
            "purpose": "diagnostic_only",
            "promotion_authority": False,
        },
        "strategy_context_authority": "downstream_v5_1_opportunity_and_shadow_learning",
        "forward_diagnostic_observations": total,
        "legacy_immediate_copy_observations": immediate,
        "legacy_immediate_copy_fraction": immediate / total if total else 0.0,
        "currently_active_tracking_candidates": active,
        "restored_misplaced_intelligence_rejections": restored,
        "state_integrity_repair": repair,
        "provider_requests_added": 0,
        "paper_only": True,
        "live_money_authority": False,
    }


async def _poll_once_with_diagnostic_intelligence(self: Any) -> None:
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Robinhood wallet authority boundary is not installed")
    _repair_instance_once(self)
    await _ORIGINAL_POLL(self)
    try:
        pending = _pending_intelligence_swap_ids(self)
        enriched = int(intelligence._enrich_forward_observations(self))
        if pending:
            with self.store._lock, self.store.db:
                _restore_diagnostic_copy_semantics(self.store.db, pending)
        setattr(
            self,
            "_roi_robinhood_wallet_diagnostic_last_cycle",
            {
                "forward_diagnostics_enriched": enriched,
                "candidate_demotions_from_intelligence": 0,
                "wallet_selection_authority": False,
                "provider_requests_added": 0,
            },
        )
        setattr(self, "_roi_robinhood_wallet_diagnostic_last_error", None)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Diagnostic enrichment cannot demote a wallet or block the canonical selection
        # cycle. Surface the failure while leaving wallet authority untouched.
        setattr(
            self,
            "_roi_robinhood_wallet_diagnostic_last_error",
            f"{type(exc).__name__}: wallet diagnostic enrichment unavailable",
        )


def _status_with_authority_boundary(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood wallet authority boundary status is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    try:
        payload["robinhood_wallet_selection_authority_boundary"] = _diagnostic_status(self)
        # Keep the old key for external readers, but make its non-authoritative role
        # explicit instead of exposing a promotion policy that no longer exists.
        payload["pumpfun_wallet_intelligence_parity"] = {
            "deprecated_as_wallet_selection_authority": True,
            "mode": "diagnostic_only",
            "replacement_status_key": "robinhood_wallet_selection_authority_boundary",
            "wallet_selection_authority": False,
            "candidate_demotion_authority": False,
            "paper_entry_authority": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    except Exception as exc:
        payload["robinhood_wallet_selection_authority_boundary"] = {
            "repair_version": REPAIR_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: wallet authority boundary status unavailable",
            "wallet_selection_authority": "wallet_quality_and_forward_followthrough_only",
            "copyability_has_wallet_selection_authority": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def _retract_misplaced_authority(plane_cls: type[Any]) -> None:
    # Restore the production universe payload that existed immediately before the
    # intelligence integration replaced it. Because the selection installer had
    # already wrapped `universe.build_entity_universe`, this returns authority to the
    # quality/120s-forward selector rather than to the older generic universe.
    if integration._ORIGINAL_UNIVERSE_PAYLOAD is not None:
        universe._payload = integration._ORIGINAL_UNIVERSE_PAYLOAD

    # Restore the wallet-layer mature-negative rule. It uses the wallet's own fresh
    # 120-second follow-through geometric value and contains no chase/latency test.
    if intelligence._ORIGINAL_DEMOTE is not None:
        selection._demote_mature_negative_candidates = intelligence._ORIGINAL_DEMOTE

    # Remove the authority-bearing intelligence poll/status wrappers before attaching
    # the diagnostic-only versions below.
    if intelligence._ORIGINAL_POLL is not None:
        plane_cls._poll_once = intelligence._ORIGINAL_POLL
    if intelligence._ORIGINAL_STATUS is not None:
        plane_cls.status = intelligence._ORIGINAL_STATUS

    # PR #169 monkey-patched intelligence helpers so `copyable` became a strategy
    # observability alias. Restore their pre-169 implementations; this subsystem is
    # now diagnostic only and its legacy immediate-copy fields keep their old meaning.
    if v51_alignment._ORIGINAL_ENSURE_SCHEMA is not None:
        intelligence._ensure_schema = v51_alignment._ORIGINAL_ENSURE_SCHEMA  # type: ignore[assignment]
    if v51_alignment._ORIGINAL_ENRICH is not None:
        intelligence._enrich_forward_observations = v51_alignment._ORIGINAL_ENRICH  # type: ignore[assignment]
    if v51_alignment._ORIGINAL_CANDIDATE_PROFILE is not None:
        intelligence._candidate_profile = v51_alignment._ORIGINAL_CANDIDATE_PROFILE  # type: ignore[assignment]
    if v51_alignment._ORIGINAL_BUILD_UNIVERSE is not None:
        intelligence.build_intelligence_entity_universe = v51_alignment._ORIGINAL_BUILD_UNIVERSE  # type: ignore[assignment]
    if v51_alignment._ORIGINAL_INTELLIGENCE_STATUS is not None:
        intelligence._intelligence_status = v51_alignment._ORIGINAL_INTELLIGENCE_STATUS  # type: ignore[assignment]


def install_robinhood_wallet_selection_authority_boundary(plane_cls: type[Any]) -> None:
    global _ORIGINAL_POLL, _ORIGINAL_STATUS, _INSTALLED
    if _INSTALLED:
        return

    _retract_misplaced_authority(plane_cls)

    _ORIGINAL_POLL = plane_cls._poll_once
    setattr(_poll_once_with_diagnostic_intelligence, "_roi_robinhood_wallet_selection_authority_boundary", True)
    plane_cls._poll_once = _poll_once_with_diagnostic_intelligence  # type: ignore[method-assign]

    _ORIGINAL_STATUS = plane_cls.status
    setattr(_status_with_authority_boundary, "_roi_robinhood_wallet_selection_authority_boundary", True)
    plane_cls.status = _status_with_authority_boundary  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_wallet_selection_authority_boundary_installed", True)
    setattr(plane_cls, "_roi_robinhood_wallet_selection_authority_boundary_version", REPAIR_VERSION)
    setattr(plane_cls, "_roi_robinhood_pumpfun_wallet_intelligence_authority_active", False)
    setattr(plane_cls, "_roi_robinhood_wallet_intelligence_v51_alignment_authority_active", False)
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "WRONG_INTELLIGENCE_REJECTION_PREFIX",
    "_repair_persisted_authority_state",
    "_restore_diagnostic_copy_semantics",
    "install_robinhood_wallet_selection_authority_boundary",
]
