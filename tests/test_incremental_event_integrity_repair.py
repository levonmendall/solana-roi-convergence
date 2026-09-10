from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import incremental_event_integrity_repair as repair
from solana_roi.storage import AppendOnlyEventStore


def _full_verifier(engine_like: object) -> tuple[bool, int, int | None]:
    store = getattr(engine_like, "store")
    previous: str | None = None
    verified_id = 0
    latest_engine: int | None = None
    with store._lock:
        rows = store.db.execute(
            "SELECT id,event_type,observed_at,payload_json,previous_hash,lineage_hash FROM events ORDER BY id"
        ).fetchall()
    for row in rows:
        if row["previous_hash"] != previous:
            return False, 0, None
        expected = hashlib.sha256(
            f"{previous or ''}|{row['event_type']}|{row['observed_at']}|{row['payload_json']}".encode()
        ).hexdigest()
        if expected != row["lineage_hash"]:
            return False, 0, None
        previous = str(row["lineage_hash"])
        verified_id = int(row["id"])
        if str(row["event_type"]) in repair.ENGINE_EVENT_TYPES:
            latest_engine = verified_id
    return True, verified_id, latest_engine


def test_first_full_verification_seeds_anchor_then_next_verification_hashes_tail_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = AppendOnlyEventStore(tmp_path / "ledger.sqlite3")
    store.append("first_touch", "2026-09-10T00:00:00+00:00", {"n": 1})
    store.append("research", "2026-09-10T00:00:01+00:00", {"n": 2})

    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_VERIFY", _full_verifier)
    verified, head, latest = repair._verify_with_checkpoint(store)
    assert verified is True
    assert head == 2
    assert latest == 1
    checkpoint_path = repair._checkpoint_path(store)
    assert checkpoint_path.is_file()

    store.append("price", "2026-09-10T00:00:02+00:00", {"n": 3})

    def forbidden_full_scan(_engine_like: object):
        raise AssertionError("valid anchor must use the append-only tail, not full history")

    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_VERIFY", forbidden_full_scan)
    verified, head, latest = repair._verify_with_checkpoint(store)
    assert verified is True
    assert head == 3
    assert latest == 3

    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert payload["verified_event_id"] == 3
    assert payload["latest_engine_event_id"] == 3
    store.close()


def test_tail_corruption_fails_closed_from_valid_anchor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = AppendOnlyEventStore(tmp_path / "ledger.sqlite3")
    store.append("first_touch", "2026-09-10T00:00:00+00:00", {"n": 1})
    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_VERIFY", _full_verifier)
    assert repair._verify_with_checkpoint(store)[0] is True

    store.append("price", "2026-09-10T00:00:01+00:00", {"n": 2})
    with store._lock, store.db:
        store.db.execute("UPDATE events SET payload_json='{}' WHERE id=2")

    monkeypatch.setattr(
        repair,
        "_ORIGINAL_BOUNDED_VERIFY",
        lambda _engine_like: (_ for _ in ()).throw(AssertionError("must not discard a valid anchor")),
    )
    verified, head, latest = repair._verify_with_checkpoint(store)
    assert verified is False
    assert head == 0
    assert latest is None
    store.close()


def test_corrupt_sidecar_falls_back_to_complete_fail_closed_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = AppendOnlyEventStore(tmp_path / "ledger.sqlite3")
    store.append("first_touch", "2026-09-10T00:00:00+00:00", {"n": 1})
    checkpoint = repair._checkpoint_path(store)
    checkpoint.write_text('{"schema":"bad"}', encoding="utf-8")
    calls: list[int] = []

    def counted_full(engine_like: object):
        calls.append(1)
        return _full_verifier(engine_like)

    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_VERIFY", counted_full)
    verified, head, latest = repair._verify_with_checkpoint(store)
    assert (verified, head, latest) == (True, 1, 1)
    assert calls == [1]
    assert repair._load_checkpoint(store) is not None
    store.close()


def test_checkpoint_anchor_row_is_validated_not_blindly_trusted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = AppendOnlyEventStore(tmp_path / "ledger.sqlite3")
    store.append("first_touch", "2026-09-10T00:00:00+00:00", {"n": 1})
    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_VERIFY", _full_verifier)
    assert repair._verify_with_checkpoint(store)[0] is True

    with store._lock, store.db:
        store.db.execute("UPDATE events SET lineage_hash=? WHERE id=1", ("f" * 64,))

    assert repair._load_checkpoint(store) is None
    calls: list[int] = []

    def failing_full(engine_like: object):
        calls.append(1)
        return _full_verifier(engine_like)

    monkeypatch.setattr(repair, "_ORIGINAL_BOUNDED_VERIFY", failing_full)
    assert repair._verify_with_checkpoint(store)[0] is False
    assert calls == [1]
    store.close()


def test_production_configures_incremental_integrity_before_composition() -> None:
    source = Path("src/solana_roi/production.py").read_text(encoding="utf-8")
    configure_at = source.index("configure_incremental_event_integrity_repair()")
    composition_at = source.index("from .production_system import")
    assert configure_at < composition_at
    assert "install_incremental_event_integrity_repair" not in source


def test_repair_status_preserves_paper_only_authority() -> None:
    status = repair.status()
    assert status["ordinary_start_verification"] == "validated_anchor_plus_append_only_tail"
    assert status["first_run_or_invalid_anchor"] == "complete_hash_chain_fail_closed"
    assert status["explicit_full_integrity_audit_retained"] is True
    assert status["canonical_evidence_reset"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["certification_thresholds_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
