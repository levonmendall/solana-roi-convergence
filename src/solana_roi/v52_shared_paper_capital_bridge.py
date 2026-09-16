from __future__ import annotations

"""Bridge Robinhood v5.2 lifecycle accounting into the existing shared paper ledger.

The canonical v51 atomic paper-capital tables remain the one buying-power
authority. Robinhood keeps its isolated market/evidence database, but it may not
activate a position until the canonical ledger has reserved the exact fraction.
Partial exits reduce the same canonical reservation in-place, and realized
contributions are appended to the existing canonical lifecycle-event table.

No live-money, signing, submission, strategy-threshold, or qualification
authority is introduced here.
"""

import json
import math
from datetime import datetime, timezone
from typing import Any

from . import v51_atomic_paper_capital as capital
from . import v51_paper_lifecycle_runtime as paper_lifecycle
from . import v52_robinhood_position_lifecycle as robinhood_lifecycle
from .robinhood_chain_core import _clean_address


BRIDGE_VERSION = "v52-shared-paper-capital-robinhood-bridge-1"
ORPHAN_GRACE_SECONDS = 120.0
_INSTALLED = False
_BASE_PERSIST_LOT: Any | None = None
_BASE_APPLY_EXIT: Any | None = None
_BASE_OPEN_EXPOSURE: Any | None = None
_BASE_PAPER_NAV: Any | None = None
_BASE_CAPITAL_RECONCILIATION: Any | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite_fraction(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(result):
        return 0.0
    return max(0.0, result)


def _canonical_adapter(owner: Any | None = None) -> Any | None:
    adapter = getattr(paper_lifecycle, "_ACTIVE_ADAPTER", None)
    if adapter is None or getattr(adapter, "store", None) is None:
        return None
    if owner is not None:
        owner_release = str(getattr(owner, "release_commit", "") or "")
        adapter_release = str(getattr(adapter, "release_commit", "") or "")
        if not owner_release or owner_release != adapter_release:
            return None
    return adapter


def _reservation_id(token: str, evidence_fingerprint: str) -> str:
    token = _clean_address(token)
    return f"robinhood:{token}:{evidence_fingerprint}"


def _candidate_id(token: str, evidence_fingerprint: str) -> str:
    return f"{_clean_address(token)}:{evidence_fingerprint}"


def reserve_robinhood_capital(
    owner: Any,
    *,
    token: str,
    evidence_fingerprint: str,
    lane: str,
    requested_fraction: float,
) -> dict[str, Any]:
    """Reserve against the same canonical capacity used by Solana and FOMO."""
    adapter = _canonical_adapter(owner)
    requested = _finite_fraction(requested_fraction)
    if adapter is None or requested <= 0.0:
        return {
            "status": "unavailable",
            "reason": "canonical_shared_paper_capital_unavailable",
            "reserved_fraction": 0.0,
            "idempotent_replay": False,
        }
    return capital.reserve_paper_capital(
        adapter.store,
        release_commit=str(owner.release_commit),
        reservation_id=_reservation_id(token, evidence_fingerprint),
        lane=f"ROBINHOOD:{lane or 'unknown'}",
        candidate_id=_candidate_id(token, evidence_fingerprint),
        requested_fraction=requested,
        allow_downsize=False,
        minimum_fraction=requested,
    )


def _local_lots(owner: Any) -> list[dict[str, Any]]:
    robinhood_lifecycle._ensure_schema(owner)
    with owner.store._lock:
        rows = owner.store.db.execute(
            "SELECT l.id,l.trial_id,l.position_id,l.entry_fraction,l.remaining_fraction,"
            "l.evidence_fingerprint,l.capital_reservation_id,p.token,c.lane "
            "FROM v52_robinhood_position_lots l "
            "JOIN v52_robinhood_positions p ON p.id=l.position_id "
            "LEFT JOIN robinhood_v5_trial_context c ON c.trial_id=l.trial_id "
            "ORDER BY l.id"
        ).fetchall()
    return [dict(row) for row in rows]


def _attach_local_reservation(owner: Any, lot: dict[str, Any], reservation_id: str) -> None:
    existing = str(lot.get("capital_reservation_id") or "")
    if existing and existing != reservation_id:
        raise RuntimeError("robinhood_capital_reservation_identity_mismatch")
    with owner.store._lock, owner.store.db:
        owner.store.db.execute(
            "UPDATE v52_robinhood_position_lots SET capital_reservation_id=? WHERE id=? "
            "AND (capital_reservation_id IS NULL OR capital_reservation_id='')",
            (reservation_id, int(lot["id"])),
        )
        owner.store.db.execute(
            "UPDATE robinhood_paper_trials SET capital_reservation_id=? WHERE id=? "
            "AND (capital_reservation_id IS NULL OR capital_reservation_id='')",
            (reservation_id, int(lot["trial_id"])),
        )


def _reservation_row(store: Any, release_commit: str, reservation_id: str) -> dict[str, Any] | None:
    capital.ensure_atomic_capital_schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT * FROM v51_paper_capital_reservations "
            "WHERE release_commit=? AND reservation_id=? LIMIT 1",
            (release_commit, reservation_id),
        ).fetchone()
    return dict(row) if row is not None else None


def _ensure_open_lot_reservations(owner: Any, adapter: Any, lots: list[dict[str, Any]]) -> set[str]:
    """Attach/recover deterministic reservation ids for every still-open lot."""
    release = str(owner.release_commit)
    local_ids: set[str] = set()
    for lot in lots:
        token = str(lot.get("token") or "")
        evidence = str(lot.get("evidence_fingerprint") or "")
        expected = _reservation_id(token, evidence)
        local_ids.add(expected)
        remaining = _finite_fraction(lot.get("remaining_fraction"))
        row = _reservation_row(adapter.store, release, expected)
        if row is None and remaining > 1e-12:
            reservation = reserve_robinhood_capital(
                owner,
                token=token,
                evidence_fingerprint=evidence,
                lane=str(lot.get("lane") or "unknown"),
                requested_fraction=remaining,
            )
            if str(reservation.get("status") or "") != "active":
                raise RuntimeError("open_robinhood_lot_cannot_recover_shared_capital")
        if remaining > 1e-12:
            _attach_local_reservation(owner, lot, expected)
    return local_ids


def _record_realization_rows(owner: Any, store: Any) -> int:
    with owner.store._lock:
        events = owner.store.db.execute(
            "SELECT id,position_id,position_fraction,net_return,created_at "
            "FROM v52_robinhood_position_events WHERE net_return IS NOT NULL ORDER BY id"
        ).fetchall()
    inserted = 0
    for raw in events:
        event = dict(raw)
        fraction = _finite_fraction(event.get("position_fraction"))
        try:
            net_return = float(event.get("net_return"))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(net_return) or fraction <= 0.0:
            continue
        contribution = fraction * net_return
        payload = json.dumps(
            {
                "surface": "ROBINHOOD",
                "position_id": int(event["position_id"]),
                "position_event_id": int(event["id"]),
                "released_fraction": fraction,
                "net_return": net_return,
                "realized_contribution": contribution,
                "paper_only": True,
                "live_money_authority": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO v51_paper_lifecycle_events("
            "release_commit,candidate_id,event_key,stage,payload_json,created_at,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,1,0)",
            (
                str(owner.release_commit),
                f"robinhood-position:{int(event['position_id'])}",
                f"ROBINHOOD:REALIZED:{int(event['id'])}",
                "ROBINHOOD_REALIZED",
                payload,
                str(event.get("created_at") or _utcnow()),
            ),
        )
        inserted += int(cursor.rowcount or 0)
    return inserted


def _settle_closed_reservation(store: Any, release: str, reservation_id: str, now: str) -> None:
    store.db.execute(
        "INSERT OR IGNORE INTO v51_paper_capital_settlements("
        "release_commit,settlement_id,reservation_id,net_return,realized_contribution,settled_at,"
        "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0)",
        (release, f"robinhood:{reservation_id}:closed", reservation_id, 0.0, 0.0, now),
    )
    store.db.execute(
        "UPDATE v51_paper_capital_reservations SET reserved_fraction=0,status='settled',"
        "reason='robinhood_lifecycle_settled',net_return=0,realized_contribution=0,updated_at=? "
        "WHERE release_commit=? AND reservation_id=? AND status='active'",
        (now, release, reservation_id),
    )


def reconcile_robinhood_capital(owner: Any, *, cleanup_stale: bool = True) -> dict[str, Any]:
    """Converge isolated Robinhood lifecycle state into canonical capital state."""
    adapter = _canonical_adapter(owner)
    if adapter is None:
        return {"ok": False, "reason": "canonical_shared_paper_capital_unavailable"}
    capital.ensure_atomic_capital_schema(adapter.store)
    lots = _local_lots(owner)
    local_ids = _ensure_open_lot_reservations(owner, adapter, lots)
    # Refresh after recovery/attachment so the local id surface is canonical.
    lots = _local_lots(owner)
    release = str(owner.release_commit)
    now = _utcnow()
    updated = 0
    settled = 0
    cancelled_orphans = 0
    with adapter.store._lock:
        adapter.store.db.execute("BEGIN IMMEDIATE")
        try:
            _record_realization_rows(owner, adapter.store)
            for lot in lots:
                reservation_id = str(lot.get("capital_reservation_id") or "")
                remaining = _finite_fraction(lot.get("remaining_fraction"))
                if not reservation_id:
                    if remaining > 1e-12:
                        raise RuntimeError("open_robinhood_lot_missing_shared_reservation")
                    continue
                row = adapter.store.db.execute(
                    "SELECT status,reserved_fraction FROM v51_paper_capital_reservations "
                    "WHERE release_commit=? AND reservation_id=? LIMIT 1",
                    (release, reservation_id),
                ).fetchone()
                if row is None:
                    if remaining > 1e-12:
                        raise RuntimeError("open_robinhood_lot_reservation_missing")
                    continue
                status = str(row["status"] or "")
                reserved = _finite_fraction(row["reserved_fraction"])
                if status == "active":
                    if remaining > reserved + 1e-12:
                        raise RuntimeError("robinhood_shared_capital_under_reserved")
                    if remaining <= 1e-12:
                        _settle_closed_reservation(adapter.store, release, reservation_id, now)
                        settled += 1
                    elif remaining + 1e-12 < reserved:
                        adapter.store.db.execute(
                            "UPDATE v51_paper_capital_reservations SET reserved_fraction=?,"
                            "reason='robinhood_partial_exit_released',updated_at=? "
                            "WHERE release_commit=? AND reservation_id=? AND status='active'",
                            (remaining, now, release, reservation_id),
                        )
                        updated += 1
                elif status == "settled" and remaining > 1e-12:
                    raise RuntimeError("settled_robinhood_reservation_has_open_inventory")
                elif status in {"cancelled", "rejected"} and remaining > 1e-12:
                    raise RuntimeError("open_robinhood_inventory_has_inactive_reservation")

            if cleanup_stale:
                active = adapter.store.db.execute(
                    "SELECT reservation_id,created_at FROM v51_paper_capital_reservations "
                    "WHERE release_commit=? AND status='active' AND lane LIKE 'ROBINHOOD:%'",
                    (release,),
                ).fetchall()
                current = set(local_ids)
                for raw in active:
                    reservation_id = str(raw["reservation_id"] or "")
                    if reservation_id in current:
                        continue
                    try:
                        created = datetime.fromisoformat(str(raw["created_at"]))
                        if created.tzinfo is None:
                            created = created.replace(tzinfo=timezone.utc)
                        age = (datetime.now(timezone.utc) - created.astimezone(timezone.utc)).total_seconds()
                    except (TypeError, ValueError):
                        age = 0.0
                    if age < ORPHAN_GRACE_SECONDS:
                        continue
                    cursor = adapter.store.db.execute(
                        "UPDATE v51_paper_capital_reservations SET status='cancelled',"
                        "reason='robinhood_orphan_recovered',updated_at=? "
                        "WHERE release_commit=? AND reservation_id=? AND status='active'",
                        (now, release, reservation_id),
                    )
                    cancelled_orphans += int(cursor.rowcount or 0)
            adapter.store.db.commit()
        except Exception:
            if adapter.store.db.in_transaction:
                adapter.store.db.rollback()
            raise
    return {
        "ok": True,
        "updated_partial_reservations": updated,
        "settled_reservations": settled,
        "cancelled_stale_orphans": cancelled_orphans,
    }


def _matching_local_lot(owner: Any, token: str, evidence: str) -> dict[str, Any] | None:
    robinhood_lifecycle._ensure_schema(owner)
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT l.*,p.token FROM v52_robinhood_position_lots l "
            "JOIN v52_robinhood_positions p ON p.id=l.position_id "
            "WHERE p.token=? AND l.evidence_fingerprint=? ORDER BY l.id DESC LIMIT 1",
            (_clean_address(token), evidence),
        ).fetchone()
    return dict(row) if row is not None else None


def _persist_lot_with_shared_capital(owner: Any, payload: dict[str, Any], *, pending: dict[str, Any]) -> bool:
    if _BASE_PERSIST_LOT is None:
        raise RuntimeError("shared_paper_capital_bridge_missing_persist_base")
    token = _clean_address(payload.get("token"))
    evidence = str(pending.get("evidence_fingerprint") or "")
    requested = _finite_fraction(payload.get("fraction"))
    lane = str(payload.get("lane") or "unknown")
    if not token or not evidence or requested <= 0.0:
        return False
    reservation_id = _reservation_id(token, evidence)
    existing = _matching_local_lot(owner, token, evidence)
    if existing is not None:
        reconcile_robinhood_capital(owner, cleanup_stale=False)
        return True

    reservation = reserve_robinhood_capital(
        owner,
        token=token,
        evidence_fingerprint=evidence,
        lane=lane,
        requested_fraction=requested,
    )
    if str(reservation.get("status") or "") != "active":
        return False
    try:
        committed = bool(_BASE_PERSIST_LOT(owner, payload, pending=pending))
    except Exception:
        if _matching_local_lot(owner, token, evidence) is None:
            adapter = _canonical_adapter(owner)
            if adapter is not None:
                capital.cancel_paper_capital(
                    adapter.store,
                    release_commit=str(owner.release_commit),
                    reservation_id=reservation_id,
                    reason="robinhood_entry_failed_before_local_commit",
                )
        raise
    if not committed:
        existing = _matching_local_lot(owner, token, evidence)
        if existing is None:
            adapter = _canonical_adapter(owner)
            if adapter is not None:
                capital.cancel_paper_capital(
                    adapter.store,
                    release_commit=str(owner.release_commit),
                    reservation_id=reservation_id,
                    reason="robinhood_entry_not_persisted",
                )
            return False
    reconcile_robinhood_capital(owner, cleanup_stale=False)
    attached = _matching_local_lot(owner, token, evidence)
    if attached is None or str(attached.get("capital_reservation_id") or "") != reservation_id:
        raise RuntimeError("robinhood_entry_committed_without_shared_reservation_lineage")
    return True


def _apply_exit_with_shared_capital(owner: Any, *args: Any, **kwargs: Any) -> None:
    if _BASE_APPLY_EXIT is None:
        raise RuntimeError("shared_paper_capital_bridge_missing_exit_base")
    _BASE_APPLY_EXIT(owner, *args, **kwargs)
    reconcile_robinhood_capital(owner, cleanup_stale=False)


def _open_exposure_with_shared_capital(owner: Any) -> float:
    if _BASE_OPEN_EXPOSURE is None:
        raise RuntimeError("shared_paper_capital_bridge_missing_exposure_base")
    try:
        result = reconcile_robinhood_capital(owner, cleanup_stale=True)
        if not result.get("ok"):
            return 1.0
    except Exception:
        # Fail closed for new allocation if canonical buying power cannot be
        # reconciled. Existing position-management paths remain reachable.
        return 1.0
    return float(_BASE_OPEN_EXPOSURE(owner))


def _canonical_paper_nav(owner: Any) -> float:
    adapter = _canonical_adapter(owner)
    if adapter is None:
        if _BASE_PAPER_NAV is None:
            return float(getattr(owner, "starting_nav_usd", 500.0))
        return float(_BASE_PAPER_NAV(owner))
    reconcile_robinhood_capital(owner, cleanup_stale=False)
    result = capital.capital_reconciliation(
        adapter.store,
        release_commit=str(owner.release_commit),
    )
    return float(getattr(owner, "starting_nav_usd", 500.0)) * float(result["paper_nav_multiplier"])


def _reconciliation_with_robinhood(
    store: Any,
    *,
    release_commit: str,
    capacity_fraction: float = capital.DEFAULT_CAPACITY_FRACTION,
) -> dict[str, Any]:
    if _BASE_CAPITAL_RECONCILIATION is None:
        raise RuntimeError("shared_paper_capital_bridge_missing_reconciliation_base")
    result = dict(
        _BASE_CAPITAL_RECONCILIATION(
            store,
            release_commit=release_commit,
            capacity_fraction=capacity_fraction,
        )
    )
    with store._lock:
        rows = store.db.execute(
            "SELECT payload_json FROM v51_paper_lifecycle_events "
            "WHERE release_commit=? AND stage='ROBINHOOD_REALIZED' ORDER BY id",
            (release_commit,),
        ).fetchall()
    robinhood_contribution = 0.0
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
            value = float(payload.get("realized_contribution") or 0.0)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if math.isfinite(value):
            robinhood_contribution += value
    base_contribution = float(result.get("realized_return_contribution") or 0.0)
    total = base_contribution + robinhood_contribution
    result["robinhood_realized_return_contribution"] = robinhood_contribution
    result["realized_return_contribution"] = total
    result["paper_nav_multiplier"] = max(0.0, 1.0 + total)
    result["shared_robinhood_capital_bridge"] = True
    return result


def install_v52_shared_paper_capital_bridge() -> None:
    global _INSTALLED, _BASE_PERSIST_LOT, _BASE_APPLY_EXIT, _BASE_OPEN_EXPOSURE
    global _BASE_PAPER_NAV, _BASE_CAPITAL_RECONCILIATION
    if _INSTALLED:
        return
    _BASE_PERSIST_LOT = robinhood_lifecycle._persist_lot
    _BASE_APPLY_EXIT = robinhood_lifecycle._apply_exit
    _BASE_OPEN_EXPOSURE = robinhood_lifecycle._open_exposure_with_lifecycle
    _BASE_PAPER_NAV = robinhood_lifecycle._paper_nav_with_lifecycle
    _BASE_CAPITAL_RECONCILIATION = capital.capital_reconciliation

    setattr(_persist_lot_with_shared_capital, "__wrapped__", _BASE_PERSIST_LOT)
    setattr(_persist_lot_with_shared_capital, "_roi_v52_shared_paper_capital", True)
    robinhood_lifecycle._persist_lot = _persist_lot_with_shared_capital

    setattr(_apply_exit_with_shared_capital, "__wrapped__", _BASE_APPLY_EXIT)
    setattr(_apply_exit_with_shared_capital, "_roi_v52_shared_paper_capital", True)
    robinhood_lifecycle._apply_exit = _apply_exit_with_shared_capital

    setattr(_open_exposure_with_shared_capital, "__wrapped__", _BASE_OPEN_EXPOSURE)
    setattr(_open_exposure_with_shared_capital, "_roi_v52_shared_paper_capital", True)
    robinhood_lifecycle._open_exposure_with_lifecycle = _open_exposure_with_shared_capital

    setattr(_canonical_paper_nav, "__wrapped__", _BASE_PAPER_NAV)
    setattr(_canonical_paper_nav, "_roi_v52_staged_nav_reconciliation", True)
    setattr(_canonical_paper_nav, "_roi_v52_shared_paper_capital", True)
    robinhood_lifecycle._paper_nav_with_lifecycle = _canonical_paper_nav

    capital.capital_reconciliation = _reconciliation_with_robinhood
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": BRIDGE_VERSION,
        "installed": _INSTALLED,
        "single_buying_power_authority": "v51_paper_capital_reservations",
        "robinhood_market_store_remains_isolated": True,
        "cross_lane_capacity_shared": True,
        "exact_reservation_before_robinhood_position": True,
        "partial_exit_releases_same_reservation": True,
        "duplicate_realizations_idempotent": True,
        "stale_precommit_orphans_recoverable": True,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "BRIDGE_VERSION",
    "install_v52_shared_paper_capital_bridge",
    "reconcile_robinhood_capital",
    "reserve_robinhood_capital",
    "status",
]
