from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from .v51_measurement_integrity import MEASUREMENT_EPOCH


PROVENANCE_VERSION = "v51-synthetic-provenance-v1"
SYNTHETIC_SURFACE = "SEEDED_E2E"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_synthetic_provenance_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_synthetic_provenance ("
            "surface TEXT NOT NULL,candidate_id TEXT NOT NULL,synthetic INTEGER NOT NULL,"
            "origin TEXT NOT NULL,lane TEXT NOT NULL,economic_surface TEXT NOT NULL,venue TEXT NOT NULL,"
            "release_commit TEXT NOT NULL,authority_id TEXT NOT NULL,economic_freeze_epoch TEXT NOT NULL,"
            "measurement_epoch TEXT NOT NULL,created_at TEXT NOT NULL,"
            "PRIMARY KEY(surface,candidate_id))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_synthetic_provenance_lane "
            "ON v51_synthetic_provenance(lane,economic_surface,venue,created_at)"
        )


def _canonical_payload(
    *,
    candidate_id: str,
    origin: str,
    lane: str,
    economic_surface: str,
    venue: str,
    release_commit: str,
) -> dict[str, Any]:
    return {
        "provenance_version": PROVENANCE_VERSION,
        "surface": SYNTHETIC_SURFACE,
        "candidate_id": str(candidate_id),
        "synthetic": True,
        "origin": str(origin),
        "lane": str(lane),
        "economic_surface": str(economic_surface),
        "venue": str(venue),
        "release_commit": str(release_commit),
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "paper_only": True,
        "live_money_authority": False,
        "certification_eligible": False,
        "promotion_eligible": False,
    }


def register_synthetic_provenance(
    store: Any,
    *,
    candidate_id: str,
    origin: str,
    lane: str,
    economic_surface: str,
    venue: str,
    release_commit: str,
) -> dict[str, Any]:
    """Register immutable synthetic lineage before the first synthetic stage is written.

    Re-registering an identical record is idempotent. Any attempt to change the
    provenance of an existing synthetic candidate raises instead of silently
    rewriting history.
    """
    ensure_synthetic_provenance_schema(store)
    expected = _canonical_payload(
        candidate_id=candidate_id,
        origin=origin,
        lane=lane,
        economic_surface=economic_surface,
        venue=venue,
        release_commit=release_commit,
    )
    with store._lock, store.db:
        row = store.db.execute(
            "SELECT surface,candidate_id,synthetic,origin,lane,economic_surface,venue,release_commit,"
            "authority_id,economic_freeze_epoch,measurement_epoch,created_at "
            "FROM v51_synthetic_provenance WHERE surface=? AND candidate_id=?",
            (SYNTHETIC_SURFACE, str(candidate_id)),
        ).fetchone()
        if row is None:
            store.db.execute(
                "INSERT INTO v51_synthetic_provenance("
                "surface,candidate_id,synthetic,origin,lane,economic_surface,venue,release_commit,"
                "authority_id,economic_freeze_epoch,measurement_epoch,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    SYNTHETIC_SURFACE,
                    str(candidate_id),
                    1,
                    str(origin),
                    str(lane),
                    str(economic_surface),
                    str(venue),
                    str(release_commit),
                    AUTHORITY_ID,
                    ECONOMIC_FREEZE_EPOCH,
                    MEASUREMENT_EPOCH,
                    _utcnow(),
                ),
            )
            return expected

        actual = dict(row)
        comparable_actual = {
            "surface": str(actual.get("surface") or ""),
            "candidate_id": str(actual.get("candidate_id") or ""),
            "synthetic": bool(actual.get("synthetic")),
            "origin": str(actual.get("origin") or ""),
            "lane": str(actual.get("lane") or ""),
            "economic_surface": str(actual.get("economic_surface") or ""),
            "venue": str(actual.get("venue") or ""),
            "release_commit": str(actual.get("release_commit") or ""),
            "authority_id": str(actual.get("authority_id") or ""),
            "economic_freeze_epoch": str(actual.get("economic_freeze_epoch") or ""),
            "measurement_epoch": str(actual.get("measurement_epoch") or ""),
        }
        comparable_expected = {
            key: expected[key]
            for key in (
                "surface",
                "candidate_id",
                "synthetic",
                "origin",
                "lane",
                "economic_surface",
                "venue",
                "release_commit",
                "authority_id",
                "economic_freeze_epoch",
                "measurement_epoch",
            )
        }
        if comparable_actual != comparable_expected:
            raise RuntimeError("synthetic_provenance_is_immutable")
    return expected


def synthetic_provenance_for(store: Any, candidate_id: str) -> dict[str, Any] | None:
    ensure_synthetic_provenance_schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT surface,candidate_id,synthetic,origin,lane,economic_surface,venue,release_commit,"
            "authority_id,economic_freeze_epoch,measurement_epoch,created_at "
            "FROM v51_synthetic_provenance WHERE surface=? AND candidate_id=?",
            (SYNTHETIC_SURFACE, str(candidate_id)),
        ).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["synthetic"] = bool(payload.get("synthetic"))
    payload["provenance_version"] = PROVENANCE_VERSION
    payload["paper_only"] = True
    payload["live_money_authority"] = False
    payload["certification_eligible"] = False
    payload["promotion_eligible"] = False
    return payload


def attach_synthetic_provenance(
    payload: dict[str, Any] | None,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    result = dict(payload or {})
    result["synthetic"] = True
    result["certification_eligible"] = False
    result["promotion_eligible"] = False
    result["provenance"] = dict(provenance)
    return result


def synthetic_registry_snapshot(store: Any) -> dict[str, Any]:
    ensure_synthetic_provenance_schema(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT lane,economic_surface,venue,COUNT(*) AS count "
            "FROM v51_synthetic_provenance GROUP BY lane,economic_surface,venue "
            "ORDER BY lane,economic_surface,venue"
        ).fetchall()
        total = store.db.execute("SELECT COUNT(*) AS count FROM v51_synthetic_provenance").fetchone()
    by_lane = {
        str(row["lane"]): {
            "economic_surface": str(row["economic_surface"]),
            "venue": str(row["venue"]),
            "candidate_count": int(row["count"]),
        }
        for row in rows
    }
    return {
        "provenance_version": PROVENANCE_VERSION,
        "synthetic_candidate_count": int(total["count"] if total is not None else 0),
        "by_lane": by_lane,
        "synthetic_rows_are_certification_eligible": False,
        "synthetic_rows_are_promotion_eligible": False,
        "canonical_economic_tables_written_by_registry": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "PROVENANCE_VERSION",
    "SYNTHETIC_SURFACE",
    "attach_synthetic_provenance",
    "ensure_synthetic_provenance_schema",
    "register_synthetic_provenance",
    "synthetic_provenance_for",
    "synthetic_registry_snapshot",
]
