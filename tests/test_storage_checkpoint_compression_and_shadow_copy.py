from __future__ import annotations

import hashlib
import sqlite3

import pytest

from solana_roi.active_storage import ActiveStorage, canonical_json
from solana_roi import storage_shadow_copy_bounded_repair as copy_repair
from solana_roi import storage_transition as transition


def _truth(repetitions: int = 250) -> dict[str, object]:
    repeated = [{"wallet": f"wallet-{index}", "signal": "x" * 96} for index in range(repetitions)]
    return {
        "strategy": {"state": "v5.2"},
        "wallet": {"profiles": repeated},
        "wallet_evidence_watermarks": {"max": repetitions},
        "provider_source": {"provider": "unchanged"},
        "freshness": {"status": "fresh"},
        "latest_event_ids": {"events": repetitions},
        "active_candidates": {"rows": []},
        "active_lifecycles": {"rows": []},
        "portfolio": {"cash": 500.0, "positions": {}},
        "replication_watermarks": {"change_id": repetitions},
        "certification": {"status": "pending"},
        "continuity": {"release": "test"},
    }


def _checkpoint(tmp_path, repetitions: int = 250):
    storage = ActiveStorage(tmp_path / "active.sqlite3")
    storage.initialize(epoch_id="test-epoch")
    truth = _truth(repetitions)
    payload = transition.build_checkpoint_payload(
        release_sha="release-test",
        current_truth=truth,
        provenance={"source": "test"},
    )
    return storage, truth, payload


def test_compressed_checkpoint_round_trip_preserves_exact_payload_and_semantics(tmp_path):
    storage, truth, payload = _checkpoint(tmp_path)
    verification = transition.persist_verified_checkpoint(
        storage,
        checkpoint_payload=payload,
        source_truth=truth,
    )
    assert verification.equivalent is True

    with storage.connect() as conn:
        stored_body, stored_hash = conn.execute(
            "SELECT payload_json,payload_hash FROM checkpoint_current WHERE verified=1"
        ).fetchone()

    plain = canonical_json(payload)
    assert str(stored_body).startswith(transition.CHECKPOINT_ENCODING_PREFIX)
    assert len(str(stored_body)) < len(plain)
    assert str(stored_hash) == hashlib.sha256(plain.encode("utf-8")).hexdigest()
    assert transition.load_verified_checkpoint(storage.path) == payload


def test_legacy_plaintext_checkpoint_remains_readable(tmp_path):
    storage, truth, payload = _checkpoint(tmp_path)
    transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    plain = canonical_json(payload)
    with storage.connect() as conn:
        conn.execute(
            "UPDATE checkpoint_current SET payload_json=? WHERE checkpoint_id=?",
            (plain, payload["checkpoint_id"]),
        )
        conn.commit()

    assert transition.load_verified_checkpoint(storage.path) == payload


def test_corrupt_compressed_checkpoint_fails_closed(tmp_path):
    storage, truth, payload = _checkpoint(tmp_path)
    transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    with storage.connect() as conn:
        conn.execute(
            "UPDATE checkpoint_current SET payload_json=? WHERE checkpoint_id=?",
            (transition.CHECKPOINT_ENCODING_PREFIX + "not-valid-base64!!", payload["checkpoint_id"]),
        )
        conn.commit()

    with pytest.raises(RuntimeError, match="compressed payload unreadable"):
        transition.load_verified_checkpoint(storage.path)


def test_compressed_checkpoint_still_detects_uncompressed_payload_hash_mismatch(tmp_path):
    storage, truth, payload = _checkpoint(tmp_path)
    transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    with storage.connect() as conn:
        conn.execute(
            "UPDATE checkpoint_current SET payload_hash='tampered' WHERE checkpoint_id=?",
            (payload["checkpoint_id"],),
        )
        conn.commit()

    with pytest.raises(RuntimeError, match="payload hash mismatch"):
        transition.load_verified_checkpoint(storage.path)


def test_shadow_query_copy_is_bounded_and_exact(monkeypatch):
    source = sqlite3.connect(":memory:")
    source.row_factory = sqlite3.Row
    destination = sqlite3.connect(":memory:")
    source.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,payload TEXT NOT NULL)")
    source.executemany(
        "INSERT INTO sample(id,payload) VALUES(?,?)",
        [(index, f"payload-{index}") for index in range(1, 18)],
    )
    source.commit()

    monkeypatch.setattr(copy_repair.storage_manifest, "contract_for", lambda table: None)
    monkeypatch.setattr(copy_repair, "COPY_BATCH_ROWS", 3)

    copied = copy_repair._bounded_copy_query(
        source,
        destination,
        "sample",
        "SELECT id,payload FROM sample ORDER BY id",
    )

    assert copied == 17
    assert destination.execute("SELECT id,payload FROM sample ORDER BY id").fetchall() == [
        (index, f"payload-{index}") for index in range(1, 18)
    ]
    source.close()
    destination.close()


def test_shadow_dict_copy_batches_without_changing_hex_materialization(monkeypatch):
    source = sqlite3.connect(":memory:")
    source.row_factory = sqlite3.Row
    destination = sqlite3.connect(":memory:")
    source.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,payload BLOB NOT NULL)")
    source.commit()
    rows = [
        {"id": index, "payload": {"hex": bytes([index]).hex()}}
        for index in range(1, 8)
    ]
    monkeypatch.setattr(copy_repair.storage_manifest, "contract_for", lambda table: None)
    monkeypatch.setattr(copy_repair, "COPY_BATCH_ROWS", 2)

    copied = copy_repair._bounded_copy_dict_rows(source, destination, "sample", rows)

    assert copied == 7
    assert destination.execute("SELECT id,payload FROM sample ORDER BY id").fetchall() == [
        (index, bytes([index])) for index in range(1, 8)
    ]
    source.close()
    destination.close()
