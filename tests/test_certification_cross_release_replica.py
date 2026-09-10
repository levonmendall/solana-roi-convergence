from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_replica_client as client


OLD_RELEASE = "a" * 40
NEW_RELEASE = "b" * 40
EPOCH = "stable-replication-epoch-1234"
FINGERPRINT = "c" * 64


def _create_valid_replica(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("INSERT INTO sample(id,value) VALUES (1,'base')")
    connection.commit()
    connection.close()


def _state(*, release: str = OLD_RELEASE, watermark: int = 17) -> dict[str, object]:
    return {
        "client_version": client.CLIENT_VERSION,
        "release_commit": release,
        "replication_version": replication.REPLICATION_VERSION,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "watermark": watermark,
        "bootstrap_complete": True,
        "catchup_complete": True,
        "last_transport": "incremental_delta",
    }


def test_release_change_reuses_valid_replica_and_attempts_exact_release_delta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"
    _create_valid_replica(replica)
    client._atomic_state(client._state_path(replica), _state())
    monkeypatch.setattr(client, "_replica_path", lambda: replica)

    def forbidden_bootstrap(*args, **kwargs):
        raise AssertionError("release change alone must not force a history bootstrap")

    def caught_up(
        path: Path,
        state: dict[str, object],
        *,
        base: str,
        token: str,
        expected_release: str,
    ) -> dict[str, object]:
        assert path == replica
        assert state["release_commit"] == OLD_RELEASE
        assert state["epoch"] == EPOCH
        assert state["schema_fingerprint"] == FINGERPRINT
        assert int(state["watermark"]) == 17
        assert expected_release == NEW_RELEASE
        return {
            **state,
            "watermark": 23,
            "caught_up": True,
            "delta_applied": True,
            "delta_batches": 1,
        }

    monkeypatch.setattr(client, "_bootstrap", forbidden_bootstrap)
    monkeypatch.setattr(client, "_catch_up_deltas", caught_up)

    path, result = client.synchronize_replica(
        base="https://runtime.invalid",
        token="redacted",
        expected_release=NEW_RELEASE,
    )

    assert path == replica
    assert result["release_commit"] == NEW_RELEASE
    assert result["watermark"] == 23
    persisted = json.loads(client._state_path(replica).read_text(encoding="utf-8"))
    assert persisted["release_commit"] == NEW_RELEASE
    assert persisted["watermark"] == 23


def test_current_release_is_persisted_after_each_successful_delta_batch(tmp_path: Path) -> None:
    replica = tmp_path / "replica.sqlite3"
    _create_valid_replica(replica)
    state = _state(watermark=17)
    client._atomic_state(client._state_path(replica), state)

    result = client._apply_delta(
        replica,
        state,
        {
            "release_commit": NEW_RELEASE,
            "from_watermark": 17,
            "to_watermark": 18,
            "changes": [],
            "source_change_count": 0,
            "payload_bytes": 0,
            "caught_up": False,
        },
    )

    assert result["release_commit"] == NEW_RELEASE
    assert result["watermark"] == 18
    persisted = json.loads(client._state_path(replica).read_text(encoding="utf-8"))
    assert persisted["release_commit"] == NEW_RELEASE
    assert persisted["watermark"] == 18


def test_incompatible_authoritative_identity_still_forces_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"
    _create_valid_replica(replica)
    client._atomic_state(client._state_path(replica), _state())
    monkeypatch.setattr(client, "_replica_path", lambda: replica)

    def incompatible(*args, **kwargs):
        raise client.ReplicaBootstrapRequired("authoritative certification schema fingerprint changed")

    seen: list[str] = []

    def bootstrap(path: Path, *, base: str, token: str, expected_release: str):
        seen.append(expected_release)
        return {
            **_state(release=expected_release),
            "bootstrapped": True,
            "bootstrap_complete": True,
        }

    monkeypatch.setattr(client, "_catch_up_deltas", incompatible)
    monkeypatch.setattr(client, "_bootstrap", bootstrap)

    path, result = client.synchronize_replica(
        base="https://runtime.invalid",
        token="redacted",
        expected_release=NEW_RELEASE,
    )

    assert path == replica
    assert seen == [NEW_RELEASE]
    assert result["bootstrapped"] is True


def test_cross_release_compatibility_does_not_change_authority_contract() -> None:
    status = client.status()
    assert status["release_sha_is_artifact_truth_not_replica_identity"] is True
    assert status["cross_release_incremental_catchup"] is True
    assert status["replica_compatibility_identity"] == "replication_version+epoch+schema_fingerprint+watermark"
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
