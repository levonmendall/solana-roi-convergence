from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from . import v52_robinhood_position_lifecycle as lifecycle
from .robinhood_chain_core import _clean_address
from .v51_atomic_paper_capital import (
    DEFAULT_CAPACITY_FRACTION,
    cancel_paper_capital,
    capital_reconciliation,
    ensure_atomic_capital_schema,
    reserve_paper_capital,
)

REPAIR_VERSION = "v52-robinhood-shared-paper-capital-v2"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_BASE_VALIDATE: Callable[..., Awaitable[bool]] | None = None
_BASE_APPLY_EXIT: Callable[..., Any] | None = None
_BASE_NAV: Callable[..., float] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_schema(owner: Any) -> None:
    ensure_atomic_capital_schema(owner.store)
    with owner.store._lock, owner.store.db:
        owner.store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_robinhood_capital_lineage ("
            "trial_id INTEGER PRIMARY KEY, release_commit TEXT NOT NULL, "
            "original_reservation_id TEXT NOT NULL, current_reservation_id TEXT NOT NULL, "
            "original_fraction REAL NOT NULL, accounted_remaining_fraction REAL NOT NULL, "
            "accounted_realized_exit_net_wei TEXT NOT NULL, accounted_sold_cost_wei TEXT NOT NULL, "
            "settlement_sequence INTEGER NOT NULL, updated_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        owner.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v52_robinhood_capital_lineage_release "
            "ON v52_robinhood_capital_lineage(release_commit,current_reservation_id)"
        )


def _entry_reservation_id(owner: Any, payload: dict[str, Any], evidence: str) -> str:
    token = _clean_address(payload.get("token"))
    market = _clean_address(payload.get("market"))
    release = str(getattr(owner, "release_commit", "") or "")
    if not token or not market or not release or not evidence:
        raise ValueError("v52_robinhood_shared_capital_identity_incomplete")
    return f"robinhood-v52:{token}:{market}:{evidence}"


def _matching_lot(owner: Any, *, payload: dict[str, Any], evidence: str) -> dict[str, Any] | None:
    token = _clean_address(payload.get("token"))
    market = _clean_address(payload.get("market"))
    release = str(getattr(owner, "release_commit", "") or "")
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT l.*,t.release_commit,t.token,t.market,t.capital_reservation_id AS trial_reservation_id "
            "FROM v52_robinhood_position_lots l "
            "JOIN robinhood_paper_trials t ON t.id=l.trial_id "
            "WHERE t.release_commit=? AND t.token=? AND t.market=? AND l.evidence_fingerprint=? "
            "ORDER BY l.id DESC LIMIT 1",
            (release, token, market, evidence),
        ).fetchone()
    return dict(row) if row is not None else None


def _lineage(owner: Any, trial_id: int) -> dict[str, Any] | None:
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT * FROM v52_robinhood_capital_lineage WHERE trial_id=? LIMIT 1",
            (int(trial_id),),
        ).fetchone()
    return dict(row) if row is not None else None


def _link_reservation(
    owner: Any,
    *,
    lot: dict[str, Any],
    reservation_id: str,
) -> None:
    trial_id = int(lot["trial_id"])
    release = str(lot["release_commit"])
    entry_fraction = max(0.0, float(lot["entry_fraction"] or 0.0))
    remaining_fraction = max(0.0, float(lot["remaining_fraction"] or 0.0))
    total_cost = max(0, int(lot["entry_total_cost_wei"] or 0))
    remaining_cost = max(0, int(lot["remaining_entry_cost_wei"] or 0))
    realized_net = max(0, int(lot["realized_exit_net_wei"] or 0))
    sold_cost = max(0, total_cost - remaining_cost)
    now = _utcnow()
    with owner.store._lock:
        owner.store.db.execute("BEGIN IMMEDIATE")
        try:
            reservation = owner.store.db.execute(
                "SELECT status,reserved_fraction FROM v51_paper_capital_reservations "
                "WHERE release_commit=? AND reservation_id=? LIMIT 1",
                (release, reservation_id),
            ).fetchone()
            if reservation is None or str(reservation["status"]) != "active":
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_shared_capital_reservation_not_active")
            if float(reservation["reserved_fraction"] or 0.0) + 1e-12 < remaining_fraction:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_shared_capital_reservation_too_small")
            owner.store.db.execute(
                "UPDATE v52_robinhood_position_lots SET capital_reservation_id=? WHERE trial_id=?",
                (reservation_id, trial_id),
            )
            owner.store.db.execute(
                "UPDATE robinhood_paper_trials SET capital_reservation_id=? WHERE id=?",
                (reservation_id, trial_id),
            )
            owner.store.db.execute(
                "INSERT INTO v52_robinhood_capital_lineage("
                "trial_id,release_commit,original_reservation_id,current_reservation_id,original_fraction,"
                "accounted_remaining_fraction,accounted_realized_exit_net_wei,accounted_sold_cost_wei,"
                "settlement_sequence,updated_at,paper_only,live_money_authority"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,1,0) "
                "ON CONFLICT(trial_id) DO UPDATE SET current_reservation_id=excluded.current_reservation_id,"
                "updated_at=excluded.updated_at",
                (
                    trial_id,
                    release,
                    reservation_id,
                    reservation_id,
                    entry_fraction,
                    remaining_fraction,
                    str(realized_net),
                    str(sold_cost),
                    0,
                    now,
                ),
            )
            owner.store.db.commit()
        except Exception:
            if owner.store.db.in_transaction:
                owner.store.db.rollback()
            raise


def _recover_entry_link(
    owner: Any,
    *,
    payload: dict[str, Any],
    evidence: str,
    reservation_id: str,
) -> bool:
    lot = _matching_lot(owner, payload=payload, evidence=evidence)
    if lot is None:
        return False
    existing = str(lot.get("capital_reservation_id") or lot.get("trial_reservation_id") or "")
    if existing and existing != reservation_id:
        raise RuntimeError("v52_robinhood_shared_capital_conflicting_reservation")
    if _lineage(owner, int(lot["trial_id"])) is None or not existing:
        _link_reservation(owner, lot=lot, reservation_id=reservation_id)
    return True


def _cancel_unlinked_reservation(owner: Any, reservation_id: str) -> None:
    cancel_paper_capital(
        owner.store,
        release_commit=str(owner.release_commit),
        reservation_id=reservation_id,
        reason="v52_robinhood_entry_not_persisted",
    )


async def _validate_with_shared_capital(
    owner: Any,
    payload: dict[str, Any],
    *,
    venue_object: Any,
) -> bool:
    if _BASE_VALIDATE is None:
        raise RuntimeError("v52_robinhood_shared_capital_base_validator_missing")
    _ensure_schema(owner)
    token = _clean_address(payload.get("token"))
    pending = dict(lifecycle._pending_map(owner).get(token) or {})
    evidence = str(pending.get("evidence_fingerprint") or "")
    if not evidence:
        return await _BASE_VALIDATE(owner, payload, venue_object=venue_object)
    reservation_id = _entry_reservation_id(owner, payload, evidence)

    if _recover_entry_link(
        owner,
        payload=payload,
        evidence=evidence,
        reservation_id=reservation_id,
    ):
        return True

    fraction = max(0.0, float(payload.get("fraction") or 0.0))
    if fraction <= 0.0:
        return False
    reservation = reserve_paper_capital(
        owner.store,
        release_commit=str(owner.release_commit),
        reservation_id=reservation_id,
        lane="robinhood",
        candidate_id=evidence,
        requested_fraction=fraction,
        capacity_fraction=DEFAULT_CAPACITY_FRACTION,
        allow_downsize=False,
        minimum_fraction=fraction,
    )
    if str(reservation.get("status") or "") != "active":
        return False
    if float(reservation.get("reserved_fraction") or 0.0) + 1e-12 < fraction:
        return False

    try:
        committed = await _BASE_VALIDATE(owner, payload, venue_object=venue_object)
    except Exception:
        _cancel_unlinked_reservation(owner, reservation_id)
        raise
    if not committed:
        _cancel_unlinked_reservation(owner, reservation_id)
        return False

    if not _recover_entry_link(
        owner,
        payload=payload,
        evidence=evidence,
        reservation_id=reservation_id,
    ):
        # Once the durable entry may exist, retaining the reservation fails closed
        # against overspending until replay can complete the lineage link.
        raise RuntimeError("v52_robinhood_committed_lot_not_recoverable_for_capital_link")
    return True


def _settle_lot_delta(owner: Any, lot: dict[str, Any], line: dict[str, Any]) -> None:
    trial_id = int(lot["trial_id"])
    release = str(line["release_commit"])
    prior_remaining = max(0.0, float(line["accounted_remaining_fraction"] or 0.0))
    current_remaining = max(0.0, float(lot["remaining_fraction"] or 0.0))
    if current_remaining > prior_remaining + 1e-12:
        raise RuntimeError("v52_robinhood_capital_remaining_fraction_increased")
    delta_fraction = max(0.0, prior_remaining - current_remaining)

    total_cost = max(0, int(lot["entry_total_cost_wei"] or 0))
    remaining_cost = max(0, int(lot["remaining_entry_cost_wei"] or 0))
    cumulative_sold_cost = max(0, total_cost - remaining_cost)
    prior_sold_cost = max(0, int(line["accounted_sold_cost_wei"] or 0))
    delta_cost = max(0, cumulative_sold_cost - prior_sold_cost)
    cumulative_net = max(0, int(lot["realized_exit_net_wei"] or 0))
    prior_net = max(0, int(line["accounted_realized_exit_net_wei"] or 0))
    delta_net = max(0, cumulative_net - prior_net)

    if delta_fraction <= 1e-12:
        if delta_cost or delta_net:
            raise RuntimeError("v52_robinhood_capital_value_changed_without_fraction_release")
        return
    if delta_cost <= 0:
        raise RuntimeError("v52_robinhood_capital_release_missing_cost_basis")

    net_return = delta_net / max(1, delta_cost) - 1.0
    if not math.isfinite(net_return):
        raise RuntimeError("v52_robinhood_capital_release_nonfinite_return")
    contribution = delta_fraction * net_return
    sequence = int(line["settlement_sequence"] or 0) + 1
    old_reservation = str(line["current_reservation_id"] or "")
    if not old_reservation:
        raise RuntimeError("v52_robinhood_capital_current_reservation_missing")
    residual_id = f"robinhood-v52:trial:{trial_id}:residual:{sequence}"
    settlement_id = f"v52-robinhood-trial:{trial_id}:slice:{sequence}"
    now = _utcnow()

    with owner.store._lock:
        owner.store.db.execute("BEGIN IMMEDIATE")
        try:
            current = owner.store.db.execute(
                "SELECT * FROM v51_paper_capital_reservations "
                "WHERE release_commit=? AND reservation_id=? LIMIT 1",
                (release, old_reservation),
            ).fetchone()
            if current is None:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_capital_reservation_missing_during_release")
            if str(current["status"]) != "active":
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_capital_reservation_not_active_during_release")
            held = max(0.0, float(current["reserved_fraction"] or 0.0))
            if abs(held - prior_remaining) > 1e-9:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_capital_reservation_lineage_mismatch")

            owner.store.db.execute(
                "UPDATE v51_paper_capital_reservations SET status='settled',"
                "reason='v52_robinhood_realized_slice',net_return=?,realized_contribution=?,updated_at=? "
                "WHERE release_commit=? AND reservation_id=?",
                (net_return, contribution, now, release, old_reservation),
            )
            owner.store.db.execute(
                "INSERT INTO v51_paper_capital_settlements("
                "release_commit,settlement_id,reservation_id,net_return,realized_contribution,settled_at,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0)",
                (release, settlement_id, old_reservation, net_return, contribution, now),
            )

            next_reservation = old_reservation
            if current_remaining > 1e-12:
                owner.store.db.execute(
                    "INSERT INTO v51_paper_capital_reservations("
                    "release_commit,reservation_id,lane,candidate_id,requested_fraction,reserved_fraction,"
                    "capacity_fraction,status,reason,created_at,updated_at,paper_only,live_money_authority"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)",
                    (
                        release,
                        residual_id,
                        "robinhood",
                        f"trial:{trial_id}:residual:{sequence}",
                        current_remaining,
                        current_remaining,
                        DEFAULT_CAPACITY_FRACTION,
                        "active",
                        "v52_robinhood_residual_position",
                        now,
                        now,
                    ),
                )
                next_reservation = residual_id
                owner.store.db.execute(
                    "UPDATE v52_robinhood_position_lots SET capital_reservation_id=? WHERE trial_id=?",
                    (residual_id, trial_id),
                )
                owner.store.db.execute(
                    "UPDATE robinhood_paper_trials SET capital_reservation_id=? WHERE id=?",
                    (residual_id, trial_id),
                )

            owner.store.db.execute(
                "UPDATE v52_robinhood_capital_lineage SET current_reservation_id=?,"
                "accounted_remaining_fraction=?,accounted_realized_exit_net_wei=?,"
                "accounted_sold_cost_wei=?,settlement_sequence=?,updated_at=? WHERE trial_id=?",
                (
                    next_reservation,
                    current_remaining,
                    str(cumulative_net),
                    str(cumulative_sold_cost),
                    sequence,
                    now,
                    trial_id,
                ),
            )
            owner.store.db.commit()
        except Exception:
            if owner.store.db.in_transaction:
                owner.store.db.rollback()
            raise


def _sync_position_capital(owner: Any, position_id: int) -> None:
    _ensure_schema(owner)
    with owner.store._lock:
        rows = owner.store.db.execute(
            "SELECT l.*,t.release_commit,t.capital_reservation_id AS trial_reservation_id "
            "FROM v52_robinhood_position_lots l JOIN robinhood_paper_trials t ON t.id=l.trial_id "
            "WHERE l.position_id=? ORDER BY l.id",
            (int(position_id),),
        ).fetchall()
    for raw in rows:
        lot = dict(raw)
        line = _lineage(owner, int(lot["trial_id"]))
        if line is None:
            continue
        _settle_lot_delta(owner, lot, line)


def _apply_exit_with_shared_capital(owner: Any, **kwargs: Any) -> None:
    if _BASE_APPLY_EXIT is None:
        raise RuntimeError("v52_robinhood_shared_capital_base_exit_missing")
    position = dict(kwargs.get("position") or {})
    position_id = int(position.get("id") or 0)
    _BASE_APPLY_EXIT(owner, **kwargs)
    if position_id > 0:
        _sync_position_capital(owner, position_id)


def _paper_nav_with_shared_capital(owner: Any) -> float:
    if _BASE_NAV is None:
        raise RuntimeError("v52_robinhood_shared_capital_nav_base_missing")
    _ensure_schema(owner)
    with owner.store._lock:
        row = owner.store.db.execute(
            "SELECT COUNT(*) AS n FROM v52_robinhood_capital_lineage WHERE paper_only=1"
        ).fetchone()
    lineage_count = int(row["n"] or 0) if row is not None else 0

    # Release SHA is intentionally not a Robinhood portfolio-reset boundary. Until
    # a position is governed by the new shared-capital lineage, preserve the exact
    # durable cross-release NAV implementation that existed before this repair.
    if lineage_count == 0:
        return float(_BASE_NAV(owner))

    shared = capital_reconciliation(
        owner.store,
        release_commit=str(owner.release_commit),
        capacity_fraction=DEFAULT_CAPACITY_FRACTION,
    )
    shared_contribution = float(shared.get("realized_return_contribution") or 0.0)
    with owner.store._lock:
        # Closed legacy trials remain governed by their historical multiplier. A
        # lineaged trial is excluded because the shared capital ledger is now its
        # accounting authority.
        legacy_rows = owner.store.db.execute(
            "SELECT o.paper_nav_multiplier FROM robinhood_paper_outcomes o "
            "LEFT JOIN v52_robinhood_capital_lineage c ON c.trial_id=o.trial_id "
            "WHERE o.paper_only=1 AND c.trial_id IS NULL ORDER BY o.id"
        ).fetchall()
        # Preserve already-recorded pre-repair v5.2 lifecycle events for positions
        # that never entered shared-capital lineage. Managed positions are excluded
        # so staged exit slices cannot be compounded against one another twice.
        unmanaged_rows = owner.store.db.execute(
            "SELECT e.paper_nav_multiplier FROM v52_robinhood_position_events e "
            "WHERE e.paper_nav_multiplier IS NOT NULL AND NOT EXISTS ("
            "SELECT 1 FROM v52_robinhood_position_lots l "
            "JOIN v52_robinhood_capital_lineage c ON c.trial_id=l.trial_id "
            "WHERE l.position_id=e.position_id) ORDER BY e.id"
        ).fetchall()

    legacy_multiplier = 1.0
    for item in legacy_rows:
        legacy_multiplier *= max(0.0, float(item["paper_nav_multiplier"] or 1.0))
    unmanaged_multiplier = 1.0
    for item in unmanaged_rows:
        unmanaged_multiplier *= max(0.0, float(item["paper_nav_multiplier"] or 1.0))
    shared_multiplier = max(0.0, 1.0 + shared_contribution)
    return float(owner.starting_nav_usd) * legacy_multiplier * unmanaged_multiplier * shared_multiplier


def status(owner: Any | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "shared_capital_table": "v51_paper_capital_reservations",
        "shared_settlement_table": "v51_paper_capital_settlements",
        "entry_reservation_before_position_commit": True,
        "failed_entry_cancels_reservation": True,
        "partial_exit_residual_reservation": True,
        "partial_exit_exactly_once_lineage": True,
        "partial_exit_nav_is_linear_not_slice_compounded": True,
        "durable_pre_lineage_nav_preserved": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }
    if owner is not None:
        try:
            _ensure_schema(owner)
            shared = capital_reconciliation(owner.store, release_commit=str(owner.release_commit))
            with owner.store._lock:
                table = owner.store.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='v52_robinhood_position_lots' LIMIT 1"
                ).fetchone()
                if table is None:
                    unlinked_count = 0
                else:
                    unlinked = owner.store.db.execute(
                        "SELECT COUNT(*) AS n FROM v52_robinhood_position_lots l "
                        "JOIN robinhood_paper_trials t ON t.id=l.trial_id "
                        "WHERE t.release_commit=? AND l.remaining_fraction>0 "
                        "AND (l.capital_reservation_id IS NULL OR l.capital_reservation_id='')",
                        (str(owner.release_commit),),
                    ).fetchone()
                    unlinked_count = int(unlinked["n"] or 0) if unlinked is not None else 0
            payload["capital_reconciliation"] = shared
            payload["unlinked_open_lots"] = unlinked_count
            payload["failed_closed"] = bool(unlinked_count)
        except Exception as exc:
            payload["failed_closed"] = True
            payload["status_error"] = f"{type(exc).__name__}: shared capital unavailable"
    return payload


def install_v52_robinhood_shared_capital_repair() -> None:
    global _INSTALLED, _BASE_VALIDATE, _BASE_APPLY_EXIT, _BASE_NAV
    if _INSTALLED:
        return
    from .robinhood_chain_paper import RobinhoodChainPaperPlane

    _BASE_VALIDATE = lifecycle._validate_and_commit
    _BASE_APPLY_EXIT = lifecycle._apply_exit
    _BASE_NAV = RobinhoodChainPaperPlane._paper_nav_usd

    lifecycle._validate_and_commit = _validate_with_shared_capital
    lifecycle._apply_exit = _apply_exit_with_shared_capital
    lifecycle._paper_nav_with_lifecycle = _paper_nav_with_shared_capital
    RobinhoodChainPaperPlane._paper_nav_usd = _paper_nav_with_shared_capital  # type: ignore[method-assign]

    setattr(lifecycle._validate_and_commit, "_roi_v52_shared_paper_capital", True)
    setattr(lifecycle._apply_exit, "_roi_v52_shared_paper_capital", True)
    setattr(RobinhoodChainPaperPlane._paper_nav_usd, "_roi_v52_shared_paper_capital", True)
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "install_v52_robinhood_shared_capital_repair",
    "status",
]
