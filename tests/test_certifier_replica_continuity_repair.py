from __future__ import annotations

from pathlib import Path

import pytest

from solana_roi import certification_logical_bootstrap_client as logical
from solana_roi import certification_replica_client as client
from solana_roi import certifier_replica_continuity_repair as repair
from solana_roi.certification_incremental_replication import REPLICATION_VERSION


OLD_RELEASE = "a" * 40
NEW_RELEASE = "b" * 40
EPOCH = "stable-replication-epoch"
FINGERPRINT = "c" * 64


def _state() -> dict[str, object]:
    return {
        "client_version": client.CLIENT_VERSION,
        "release_commit": OLD_RELEASE,
        "replication_version": REPLICATION_VERSION,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "watermark": 42,
        "bootstrap_complete": True,
        "catchup_complete": True,
        "last_transport": "incremental_delta",
    }


def _partial_state(start_watermark: int = 42) -> dict[str, object]:
    return {
        "client_version": logical.CLIENT_VERSION,
        "bootstrap_version": logical.BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": OLD_RELEASE,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "start_watermark": start_watermark,
        "next_table_index": 1,
        "current_table": "events",
        "cursor": "123",
        "bootstrap_rows": 123,
        "bootstrap_payload_bytes": 4096,
        "bootstrap_tables": 2,
    }


def test_valid_replica_is_reused_across_release_and_rebound_after_current_delta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"
    replica.write_bytes(b"placeholder")
    state = _state()
    persisted: list[dict[str, object]] = []

    monkeypatch.setattr(client, "_replica_path", lambda: replica)
    monkeypatch.setattr(client, "_read_state", lambda path: dict(state))
    monkeypatch.setattr(client, "_validate_sqlite", lambda path: 1)

    def forbidden_bootstrap(*args, **kwargs):
        raise AssertionError("release change alone must not force bootstrap")

    def caught_up(path, saved, *, base, token, expected_release):
        assert path == replica
        assert saved["release_commit"] == OLD_RELEASE
        assert saved["epoch"] == EPOCH
        assert saved["schema_fingerprint"] == FINGERPRINT
        assert saved["watermark"] == 42
        assert expected_release == NEW_RELEASE
        return {
            **saved,
            "watermark": 47,
            "delta_applied": True,
            "caught_up": True,
            "last_transport": "incremental_delta",
        }

    monkeypatch.setattr(client, "_bootstrap", forbidden_bootstrap)
    monkeypatch.setattr(client, "_catch_up_deltas", caught_up)
    monkeypatch.setattr(client, "_atomic_state", lambda path, payload: persisted.append(dict(payload)))

    path, result = repair._synchronize_replica(
        base="https://runtime.invalid",
        token="redacted",
        expected_release=NEW_RELEASE,
    )

    assert path == replica
    assert result["release_commit"] == NEW_RELEASE
    assert result["replica_reused_across_release"] is True
    assert result["artifact_release_binding_preserved"] is True
    assert result["replica_compatibility_identity"] == "replication_version+epoch+schema_fingerprint+watermark"
    assert persisted[-1]["release_commit"] == NEW_RELEASE


def test_authoritative_identity_discontinuity_still_forces_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "replica.sqlite3"
    replica.write_bytes(b"placeholder")
    monkeypatch.setattr(client, "_replica_path", lambda: replica)
    monkeypatch.setattr(client, "_read_state", lambda path: _state())
    monkeypatch.setattr(client, "_validate_sqlite", lambda path: 1)

    def incompatible(*args, **kwargs):
        raise client.ReplicaBootstrapRequired("schema changed")

    bootstraps: list[str] = []

    def bootstrap(path, *, base, token, expected_release):
        bootstraps.append(expected_release)
        return {"release_commit": expected_release, "bootstrapped": True}

    monkeypatch.setattr(client, "_catch_up_deltas", incompatible)
    monkeypatch.setattr(client, "_bootstrap", bootstrap)

    _path, result = repair._synchronize_replica(
        base="https://runtime.invalid",
        token="redacted",
        expected_release=NEW_RELEASE,
    )

    assert bootstraps == [NEW_RELEASE]
    assert result["bootstrapped"] is True


def test_default_replica_uses_real_durable_mount_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOLANA_ROI_CERTIFIER_REPLICA_PATH", raising=False)
    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_DURABLE_ROOT", str(tmp_path))
    monkeypatch.setattr(repair, "_durable_mount_available", lambda root: True)
    assert repair._replica_path() == tmp_path / repair.DEFAULT_REPLICA_NAME

    monkeypatch.setattr(repair, "_durable_mount_available", lambda root: False)
    path = repair._replica_path()
    assert path.name == repair.DEFAULT_REPLICA_NAME
    assert path.parent != tmp_path


def test_required_durable_storage_blocks_history_bootstrap_before_authoritative_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "ephemeral-replica.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_REQUIRE_DURABLE_REPLICA", "true")
    monkeypatch.setattr(client, "_replica_path", lambda: replica)
    monkeypatch.setattr(client, "_read_state", lambda path: None)
    monkeypatch.setattr(repair, "_replica_storage_is_durable", lambda path: False)

    calls: list[int] = []

    def forbidden_bootstrap(*args, **kwargs):
        calls.append(1)
        raise AssertionError("authoritative history bootstrap must not begin")

    monkeypatch.setattr(client, "_bootstrap", forbidden_bootstrap)

    with pytest.raises(RuntimeError, match="durable replica storage required"):
        repair._synchronize_replica(
            base="https://runtime.invalid",
            token="redacted",
            expected_release=NEW_RELEASE,
        )
    assert calls == []


def test_required_durable_storage_allows_one_time_bootstrap_on_real_mount(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replica = tmp_path / "durable-replica.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_REQUIRE_DURABLE_REPLICA", "true")
    monkeypatch.setattr(client, "_replica_path", lambda: replica)
    monkeypatch.setattr(client, "_read_state", lambda path: None)
    monkeypatch.setattr(repair, "_replica_storage_is_durable", lambda path: True)

    calls: list[str] = []

    def bootstrap(path, *, base, token, expected_release):
        calls.append(expected_release)
        return {"release_commit": expected_release, "bootstrapped": True}

    monkeypatch.setattr(client, "_bootstrap", bootstrap)
    _path, result = repair._synchronize_replica(
        base="https://runtime.invalid",
        token="redacted",
        expected_release=NEW_RELEASE,
    )
    assert calls == [NEW_RELEASE]
    assert result["bootstrapped"] is True


def test_partial_resume_filename_is_release_independent(tmp_path: Path) -> None:
    destination = tmp_path / "replica.sqlite3"
    old_paths = repair._stable_resume_paths(
        destination,
        base="https://runtime.invalid",
        expected_release=OLD_RELEASE,
    )
    new_paths = repair._stable_resume_paths(
        destination,
        base="https://runtime.invalid",
        expected_release=NEW_RELEASE,
    )
    assert old_paths == new_paths


def test_compatible_partial_survives_release_change_and_keeps_older_watermark() -> None:
    state = _partial_state(start_watermark=42)
    assert repair._compatible_checkpoint_matches(
        state,
        expected_release=NEW_RELEASE,
        epoch=EPOCH,
        fingerprint=FINGERPRINT,
        start_watermark=57,
    ) is True
    assert state["release_commit"] == NEW_RELEASE
    assert repair._RESUME_CONTEXT.saved_watermark == 42
    assert repair._RESUME_CONTEXT.manifest_watermark == 57
    assert repair._RESUME_CONTEXT.release_changed is True


def test_partial_resume_rejects_watermark_regression_or_identity_change() -> None:
    assert repair._compatible_checkpoint_matches(
        _partial_state(start_watermark=60),
        expected_release=NEW_RELEASE,
        epoch=EPOCH,
        fingerprint=FINGERPRINT,
        start_watermark=57,
    ) is False
    assert repair._compatible_checkpoint_matches(
        _partial_state(start_watermark=42),
        expected_release=NEW_RELEASE,
        epoch="different-epoch",
        fingerprint=FINGERPRINT,
        start_watermark=57,
    ) is False
    assert repair._compatible_checkpoint_matches(
        _partial_state(start_watermark=42),
        expected_release=NEW_RELEASE,
        epoch=EPOCH,
        fingerprint="d" * 64,
        start_watermark=57,
    ) is False


def test_cross_release_partial_completion_returns_original_catchup_watermark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _partial_state(start_watermark=42)

    def fake_original(destination, *, base, token, expected_release):
        assert repair._compatible_checkpoint_matches(
            state,
            expected_release=expected_release,
            epoch=EPOCH,
            fingerprint=FINGERPRINT,
            start_watermark=57,
        ) is True
        return {
            "release_commit": expected_release,
            "replication_version": REPLICATION_VERSION,
            "epoch": EPOCH,
            "schema_fingerprint": FINGERPRINT,
            "watermark": 57,
        }

    monkeypatch.setattr(repair, "_ORIGINAL_LOGICAL_BOOTSTRAP", fake_original)
    result = repair._cross_release_logical_bootstrap(
        tmp_path / "replica.sqlite3",
        base="https://runtime.invalid",
        token="redacted",
        expected_release=NEW_RELEASE,
    )
    assert result["watermark"] == 42
    assert result["partial_reused"] is True
    assert result["partial_reused_across_release"] is True
    assert result["partial_resume_manifest_watermark"] == 57
    assert result["page_release_binding_preserved"] is True


def test_current_manifest_pages_and_deltas_remain_exact_release_bound() -> None:
    logical_source = Path("src/solana_roi/certification_logical_bootstrap_client.py").read_text(encoding="utf-8")
    replica_source = Path("src/solana_roi/certification_replica_client.py").read_text(encoding="utf-8")
    assert 'str(payload.get("release_commit") or "") != expected_release' in logical_source
    assert "certification logical bootstrap release changed" in logical_source
    assert 'str(payload.get("release_commit") or "") != expected_release' in replica_source
    assert "authoritative certification delta release mismatch" in replica_source


def test_repair_preserves_authority_and_certification_thresholds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_REQUIRE_DURABLE_REPLICA", "true")
    monkeypatch.setattr(repair, "_replica_storage_is_durable", lambda path: False)
    status = repair.status()
    assert status["release_change_requires_bootstrap"] is False
    assert status["partial_bootstrap_release_change_requires_restart"] is False
    assert status["partial_bootstrap_preserves_original_watermark"] is True
    assert status["artifact_release_binding_preserved"] is True
    assert status["durable_replica_required"] is True
    assert status["history_bootstrap_permitted"] is False
    assert status["ephemeral_history_bootstrap_blocked"] is True
    assert status["strategy_thresholds_changed"] is False
    assert status["certification_thresholds_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False


def test_package_bootstrap_is_explicitly_certifier_scoped() -> None:
    source = Path("src/solana_roi/__init__.py").read_text(encoding="utf-8")
    assert "SOLANA_ROI_CERTIFIER_REPLICA_CONTINUITY" in source
    assert "configure_certifier_replica_continuity_repair()" in source
