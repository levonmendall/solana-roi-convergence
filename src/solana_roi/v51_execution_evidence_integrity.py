from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


EXECUTION_EVIDENCE_INTEGRITY_VERSION = "v51-execution-evidence-integrity-batch5-v1"

REQUIRED_LINEAGE_KEYS = (
    "settlement_id",
    "exit_quote_or_reason",
    "position_id",
    "entry_id",
    "entry_quote_id",
    "authorization_id",
    "sizing_id",
    "strategy_evaluation_id",
    "candidate_id",
    "wallet_entity_source_signal_id",
    "normalized_event_id",
    "source_observation_signature",
    "release_commit",
    "strategy_authority_id",
    "economic_epoch",
    "measurement_epoch",
)


def _text(value: Any) -> str:
    return str(value or "").strip()


def economic_sample_id(row: dict[str, Any]) -> str:
    """Return a family-independent identity for certification sample counting.

    Explicit economic roots win when present. The conservative fallback clusters
    one token lifecycle as one economic sample across collectors/venue families so
    replay, recovery, FOMO context, or venue migration cannot manufacture multiple
    independent samples. Family-specific analytical cluster IDs remain separate.
    """

    explicit = _text(row.get("economic_event_root_id") or row.get("economic_event_id"))
    if explicit:
        basis = f"explicit|{explicit}"
    else:
        token = _text(row.get("token_mint") or row.get("token"))
        lifecycle = _text(row.get("lifecycle") or "unknown")
        if token:
            basis = f"token_lifecycle|{token}|{lifecycle}"
        else:
            signature = _text(row.get("source_signature") or row.get("trial_id") or row.get("id"))
            release = _text(row.get("release_commit"))
            basis = f"source|{release}|{signature}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def dedupe_economic_samples(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep one deterministic certification row per underlying economic sample."""

    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            _text(row.get("settled_at")),
            _text(row.get("release_commit")),
            _text(row.get("surface")),
            _text(row.get("family")),
            _text(row.get("source_signature") or row.get("trial_id") or row.get("id")),
        ),
    )
    kept: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for row in ordered:
        sample_id = economic_sample_id(row)
        row["economic_sample_id"] = sample_id
        if sample_id in seen:
            duplicate = dict(row)
            duplicate["duplicate_of_source_signature"] = _text(
                seen[sample_id].get("source_signature") or seen[sample_id].get("trial_id") or seen[sample_id].get("id")
            )
            duplicates.append(duplicate)
            continue
        seen[sample_id] = row
        kept.append(row)
    return kept, duplicates


def validate_settlement_lineage(lineage: dict[str, Any]) -> dict[str, Any]:
    """Fail closed unless an evidence settlement can be traced to every required node."""

    normalized = dict(lineage or {})
    missing = [key for key in REQUIRED_LINEAGE_KEYS if not _text(normalized.get(key))]
    return {
        "version": EXECUTION_EVIDENCE_INTEGRITY_VERSION,
        "complete": not missing,
        "evidence_eligible": not missing,
        "missing": missing,
        "economic_sample_id": economic_sample_id(normalized),
        "paper_only": True,
        "live_money_authority": False,
    }


def canonical_lineage_payload(**values: Any) -> dict[str, Any]:
    """Create the immutable minimal backward-trace payload used by Batch 5 proofs."""

    payload = {key: values.get(key) for key in REQUIRED_LINEAGE_KEYS}
    payload["economic_event_root_id"] = values.get("economic_event_root_id")
    payload["token_mint"] = values.get("token_mint")
    payload["lifecycle"] = values.get("lifecycle")
    status = validate_settlement_lineage(payload)
    if not status["complete"]:
        raise ValueError("incomplete_evidence_lineage:" + ",".join(status["missing"]))
    payload["economic_sample_id"] = status["economic_sample_id"]
    return payload


def ensure_lineage_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_evidence_settlement_lineage ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, settlement_id TEXT NOT NULL, "
            "economic_epoch TEXT NOT NULL, measurement_epoch TEXT NOT NULL, candidate_id TEXT NOT NULL, "
            "economic_sample_id TEXT NOT NULL, lineage_json TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,settlement_id), UNIQUE(economic_epoch,economic_sample_id))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_evidence_lineage_candidate "
            "ON v51_evidence_settlement_lineage(release_commit,candidate_id)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_evidence_lineage_measurement "
            "ON v51_evidence_settlement_lineage(economic_epoch,measurement_epoch)"
        )


def persist_complete_settlement_lineage(store: Any, lineage: dict[str, Any]) -> bool:
    """Persist only complete lineages; duplicate economic samples fail closed."""

    payload = canonical_lineage_payload(**dict(lineage or {}))
    ensure_lineage_schema(store)
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO v51_evidence_settlement_lineage("
            "release_commit,settlement_id,economic_epoch,measurement_epoch,candidate_id,economic_sample_id,"
            "lineage_json,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,1,0)",
            (
                _text(payload["release_commit"]),
                _text(payload["settlement_id"]),
                _text(payload["economic_epoch"]),
                _text(payload["measurement_epoch"]),
                _text(payload["candidate_id"]),
                _text(payload["economic_sample_id"]),
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
            ),
        )
    return int(cursor.rowcount or 0) == 1


def reconstruct_settlement_lineage(store: Any, *, release_commit: str, settlement_id: str) -> dict[str, Any] | None:
    ensure_lineage_schema(store)
    with store._lock:
        row = store.db.execute(
            "SELECT lineage_json FROM v51_evidence_settlement_lineage WHERE release_commit=? AND settlement_id=?",
            (release_commit, settlement_id),
        ).fetchone()
    if row is None:
        return None
    payload = json.loads(str(row["lineage_json"] or "{}"))
    return payload if isinstance(payload, dict) else None


__all__ = [
    "EXECUTION_EVIDENCE_INTEGRITY_VERSION",
    "REQUIRED_LINEAGE_KEYS",
    "canonical_lineage_payload",
    "dedupe_economic_samples",
    "economic_sample_id",
    "ensure_lineage_schema",
    "persist_complete_settlement_lineage",
    "reconstruct_settlement_lineage",
    "validate_settlement_lineage",
]
