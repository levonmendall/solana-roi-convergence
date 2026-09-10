from __future__ import annotations

import json
import sqlite3
import urllib.error
from pathlib import Path

import pytest

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_replica_client as client


RELEASE = "a" * 40
EPOCH = "epoch-resume-12345678"
FINGERPRINT = "b" * 64


def _create_valid_replica(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT)")
    connection.execute("INSERT INTO sample(id,value) VALUES (1,'base')")
    connection.commit()
    connection.close()


def _state(watermark: int) -> dict[str, object]:
    return {
        "client_version": client.CLIENT_VERSION,
        "release_commit": RELEASE,
        "replication_version": replication.REPLICATION_VERSION,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "watermark": watermark,
        "bootstrap_complete": True,
        "catchup_complete": False,
        "last_transport": "bounded_logical_bootstrap",
    }


def test_completed_logical_bootstrap_survives_transient_first_delta_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"

    def fake_logical_bootstrap(
        destination: Path,
        *,
        base: str,
        token: str,
        expected_release: str,
    ) -> dict[str, object]:
        assert base == "https://runtime.invalid"
        assert token == "redacted"
        assert expected_release == RELEASE
        _create_valid_replica(destination)
        return {
            "replication_version": replication.REPLICATION_VERSION,
            "epoch": EPOCH,
            "schema_fingerprint": FINGERPRINT,
            "watermark": 17,
            "bootstrap_rows": 1,
            "bootstrap_payload_bytes": 4,
            "bootstrap_tables": 1,
        }

    def transient_failure(*args, **kwargs):
        raise client.ReplicaDeltaTransportError("synthetic 502")

    monkeypatch.setattr(client, "logical_bootstrap", fake_logical_bootstrap)
    monkeypatch.setattr(client, "_catch_up_deltas", transient_failure)

    with pytest.raises(client.ReplicaDeltaTransportError):
        client._bootstrap(
            replica,
            base="https://runtime.invalid",
            token="redacted",
            expected_release=RELEASE,
        )

    assert replica.is_file()
    persisted = json.loads(client._state_path(replica).read_text(encoding="utf-8"))
    assert persisted["watermark"] == 17
    assert persisted["bootstrap_complete"] is True
    assert persisted["catchup_complete"] is False
    assert persisted["release_commit"] == RELEASE
    client._validate_sqlite(replica)


def test_next_cycle_resumes_preserved_replica_without_rebootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"
    _create_valid_replica(replica)
    client._atomic_state(client._state_path(replica), _state(17))

    monkeypatch.setattr(client, "_replica_path", lambda: replica)

    def forbidden_bootstrap(*args, **kwargs):
        raise AssertionError("a valid preserved replica must not be bootstrapped again")

    def caught_up(
        path: Path,
        state: dict[str, object],
        *,
        base: str,
        token: str,
        expected_release: str,
    ) -> dict[str, object]:
        assert path == replica
        assert int(state["watermark"]) == 17
        return {
            **state,
            "watermark": 23,
            "delta_applied": True,
            "delta_batches": 1,
            "caught_up": True,
        }

    monkeypatch.setattr(client, "_bootstrap", forbidden_bootstrap)
    monkeypatch.setattr(client, "_catch_up_deltas", caught_up)

    path, result = client.synchronize_replica(
        base="https://runtime.invalid",
        token="redacted",
        expected_release=RELEASE,
    )
    assert path == replica
    assert result["watermark"] == 23
    assert result["catchup_complete"] is True


def test_bounded_catchup_exhaustion_preserves_advanced_watermark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"
    _create_valid_replica(replica)
    initial = _state(0)
    client._atomic_state(client._state_path(replica), initial)

    monkeypatch.setattr(client, "_max_delta_batches", lambda: 2)
    monkeypatch.setattr(client, "_delta_batch_pause", lambda: 0.0)

    def next_page(*, base: str, token: str, expected_release: str, state: dict[str, object]):
        start = int(state["watermark"])
        return {
            "replication_version": replication.REPLICATION_VERSION,
            "release_commit": RELEASE,
            "epoch": EPOCH,
            "schema_fingerprint": FINGERPRINT,
            "from_watermark": start,
            "to_watermark": start + 1,
            "changes": [],
            "source_change_count": 1,
            "payload_bytes": 0,
            "caught_up": False,
        }

    monkeypatch.setattr(client, "_fetch_delta", next_page)

    with pytest.raises(client.ReplicaCatchupPending):
        client._catch_up_deltas(
            replica,
            initial,
            base="https://runtime.invalid",
            token="redacted",
            expected_release=RELEASE,
        )

    persisted = json.loads(client._state_path(replica).read_text(encoding="utf-8"))
    assert persisted["watermark"] == 2
    assert replica.is_file()


def test_http_502_is_transient_delta_error_not_bootstrap_invalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args, **kwargs):
        raise urllib.error.HTTPError(
            "https://runtime.invalid/v1/operations/certification-db-delta",
            502,
            "bad gateway",
            None,
            None,
        )

    monkeypatch.setattr(client.urllib.request, "urlopen", fail)
    with pytest.raises(client.ReplicaDeltaTransportError):
        client._fetch_delta(
            base="https://runtime.invalid",
            token="redacted",
            expected_release=RELEASE,
            state=_state(1),
        )


def test_default_authoritative_delta_page_is_small_and_safety_contract_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS", raising=False)
    monkeypatch.delenv("SOLANA_ROI_CERTIFIER_DELTA_BATCH_PAUSE_SECONDS", raising=False)
    assert replication.DEFAULT_MAX_DELTA_ROWS == 2_000
    assert replication._max_delta_rows() == 2_000
    assert 0.0 < client._delta_batch_pause() <= 2.0
    status = client.status()
    assert status["resumable_delta_catchup"] is True
    assert status["transient_delta_failure_preserves_replica"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
