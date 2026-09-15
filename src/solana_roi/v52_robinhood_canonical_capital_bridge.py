from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from . import v52_robinhood_shared_capital_repair as shared
from .robinhood_chain_core import _clean_address
from .v51_atomic_paper_capital import CANONICAL_PORTFOLIO_ID, DEFAULT_CAPACITY_FRACTION


BRIDGE_VERSION = "v52-robinhood-canonical-capital-bridge-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _matching_lot_canonical(owner: Any, *, payload: dict[str, Any], evidence: str) -> dict[str, Any] | None:
    """Recover one durable Robinhood lot by economic identity, not deploy SHA."""
    token = _clean_address(payload.get("token"))
    market = _clean_address(payload.get("market"))
    if not token or not market or not evidence:
        return None
    with owner.store._lock:
        rows = owner.store.db.execute(
            "SELECT l.*,t.release_commit,t.token,t.market,t.capital_reservation_id AS trial_reservation_id "
            "FROM v52_robinhood_position_lots l "
            "JOIN robinhood_paper_trials t ON t.id=l.trial_id "
            "WHERE t.token=? AND t.market=? AND l.evidence_fingerprint=? "
            "ORDER BY CASE WHEN l.remaining_fraction>0 THEN 0 ELSE 1 END,l.id DESC LIMIT 2",
            (token, market, evidence),
        ).fetchall()
    if not rows:
        return None
    if len(rows) > 1 and float(rows[0]["remaining_fraction"] or 0.0) > 0 and float(rows[1]["remaining_fraction"] or 0.0) > 0:
        raise RuntimeError("v52_robinhood_duplicate_open_lot_identity")
    return dict(rows[0])


def _canonical_active_reservation(owner: Any, reservation_id: str) -> dict[str, Any] | None:
    with owner.store._lock:
        rows = owner.store.db.execute(
            "SELECT * FROM v51_paper_capital_reservations "
            "WHERE portfolio_id=? AND reservation_id=? AND status='active' ORDER BY id DESC LIMIT 2",
            (CANONICAL_PORTFOLIO_ID, reservation_id),
        ).fetchall()
    if len(rows) > 1:
        raise RuntimeError("v52_robinhood_duplicate_active_capital_identity")
    return dict(rows[0]) if rows else None


def _link_reservation_canonical(owner: Any, *, lot: dict[str, Any], reservation_id: str) -> None:
    trial_id = int(lot["trial_id"])
    lot_release = str(lot["release_commit"])
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
            rows = owner.store.db.execute(
                "SELECT * FROM v51_paper_capital_reservations "
                "WHERE portfolio_id=? AND reservation_id=? AND status='active' ORDER BY id DESC LIMIT 2",
                (CANONICAL_PORTFOLIO_ID, reservation_id),
            ).fetchall()
            if len(rows) != 1:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_shared_capital_reservation_not_unique_active")
            reservation = rows[0]
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
                    lot_release,
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


def _settle_lot_delta_canonical(owner: Any, lot: dict[str, Any], line: dict[str, Any]) -> None:
    """Atomically realize one staged slice while retaining its residual capital."""
    trial_id = int(lot["trial_id"])
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
            rows = owner.store.db.execute(
                "SELECT * FROM v51_paper_capital_reservations "
                "WHERE portfolio_id=? AND reservation_id=? AND status='active' ORDER BY id DESC LIMIT 2",
                (CANONICAL_PORTFOLIO_ID, old_reservation),
            ).fetchall()
            if len(rows) != 1:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_capital_reservation_not_unique_active_during_release")
            current = rows[0]
            origin_release = str(current["release_commit"])
            held = max(0.0, float(current["reserved_fraction"] or 0.0))
            if abs(held - prior_remaining) > 1e-9:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_capital_reservation_lineage_mismatch")

            existing = owner.store.db.execute(
                "SELECT 1 FROM v51_paper_capital_settlements "
                "WHERE portfolio_id=? AND reservation_id=? LIMIT 1",
                (CANONICAL_PORTFOLIO_ID, old_reservation),
            ).fetchone()
            if existing is not None:
                owner.store.db.rollback()
                raise RuntimeError("v52_robinhood_capital_duplicate_slice_settlement")

            owner.store.db.execute(
                "UPDATE v51_paper_capital_reservations SET status='settled',"
                "reason='v52_robinhood_realized_slice',net_return=?,realized_contribution=?,updated_at=? "
                "WHERE id=? AND status='active'",
                (net_return, contribution, now, int(current["id"])),
            )
            owner.store.db.execute(
                "INSERT INTO v51_paper_capital_settlements("
                "release_commit,settlement_id,reservation_id,net_return,realized_contribution,settled_at,"
                "paper_only,live_money_authority,portfolio_id) VALUES (?,?,?,?,?,?,1,0,?)",
                (
                    origin_release,
                    settlement_id,
                    old_reservation,
                    net_return,
                    contribution,
                    now,
                    CANONICAL_PORTFOLIO_ID,
                ),
            )

            next_reservation = old_reservation
            if current_remaining > 1e-12:
                owner.store.db.execute(
                    "INSERT INTO v51_paper_capital_reservations("
                    "release_commit,reservation_id,lane,candidate_id,requested_fraction,reserved_fraction,"
                    "capacity_fraction,status,reason,created_at,updated_at,paper_only,live_money_authority,portfolio_id"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0,?)",
                    (
                        origin_release,
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
                        CANONICAL_PORTFOLIO_ID,
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


def install_v52_robinhood_canonical_capital_bridge() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    shared._matching_lot = _matching_lot_canonical
    shared._link_reservation = _link_reservation_canonical
    shared._settle_lot_delta = _settle_lot_delta_canonical
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": BRIDGE_VERSION,
        "installed": _INSTALLED,
        "portfolio_id": CANONICAL_PORTFOLIO_ID,
        "release_sha_is_capital_reset_boundary": False,
        "cross_release_lot_recovery": True,
        "cross_release_reservation_recovery": True,
        "atomic_partial_exit_residual": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "BRIDGE_VERSION",
    "install_v52_robinhood_canonical_capital_bridge",
    "status",
]
