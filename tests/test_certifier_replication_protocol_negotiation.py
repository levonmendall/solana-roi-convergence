from __future__ import annotations

import json

import pytest

from solana_roi.certification_logical_bootstrap import BOOTSTRAP_VERSION
from solana_roi.certification_logical_bootstrap_client import (
    ACTIVE_MANIFEST_REPLICATION_VERSION,
    BASE_REPLICATION_VERSION,
    _base_checkpoint,
    _checkpoint_matches,
    _validate_manifest,
)
from solana_roi.certification_replica_client import ReplicaBootstrapRequired, _fetch_delta


RELEASE = "a" * 40
EPOCH = "epoch-12345678"
FINGERPRINT = "f" * 64


def _manifest(version: str) -> dict[str, object]:
    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": version,
        "release_commit": RELEASE,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "start_watermark": 17,
    }


@pytest.mark.parametrize(
    "version",
    [BASE_REPLICATION_VERSION, ACTIVE_MANIFEST_REPLICATION_VERSION],
)
def test_logical_manifest_accepts_only_known_protocols_and_returns_exact_version(version: str) -> None:
    selected, epoch, fingerprint, watermark = _validate_manifest(_manifest(version), RELEASE)
    assert selected == version
    assert epoch == EPOCH
    assert fingerprint == FINGERPRINT
    assert watermark == 17


def test_logical_manifest_rejects_unknown_protocol_suffix() -> None:
    with pytest.raises(RuntimeError, match="replication version mismatch"):
        _validate_manifest(_manifest(BASE_REPLICATION_VERSION + "+unknown-v2"), RELEASE)


def test_resume_checkpoint_is_pinned_to_exact_protocol() -> None:
    checkpoint = _base_checkpoint(
        expected_release=RELEASE,
        replication_version=ACTIVE_MANIFEST_REPLICATION_VERSION,
        epoch=EPOCH,
        fingerprint=FINGERPRINT,
        start_watermark=17,
    )
    assert checkpoint["replication_version"] == ACTIVE_MANIFEST_REPLICATION_VERSION
    assert _checkpoint_matches(
        checkpoint,
        expected_release=RELEASE,
        replication_version=ACTIVE_MANIFEST_REPLICATION_VERSION,
        epoch=EPOCH,
        fingerprint=FINGERPRINT,
        start_watermark=17,
    )
    assert not _checkpoint_matches(
        checkpoint,
        expected_release=RELEASE,
        replication_version=BASE_REPLICATION_VERSION,
        epoch=EPOCH,
        fingerprint=FINGERPRINT,
        start_watermark=17,
    )


class _Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _state(version: str) -> dict[str, object]:
    return {
        "replication_version": version,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "watermark": 17,
    }


def _delta(version: str) -> dict[str, object]:
    return {
        "release_commit": RELEASE,
        "replication_version": version,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "from_watermark": 17,
        "to_watermark": 17,
        "changes": [],
        "caught_up": True,
    }


@pytest.mark.parametrize(
    ("selected", "drifted"),
    [
        (BASE_REPLICATION_VERSION, ACTIVE_MANIFEST_REPLICATION_VERSION),
        (ACTIVE_MANIFEST_REPLICATION_VERSION, BASE_REPLICATION_VERSION),
    ],
)
def test_delta_protocol_drift_requires_rebootstrap(monkeypatch: pytest.MonkeyPatch, selected: str, drifted: str) -> None:
    monkeypatch.setattr(
        "solana_roi.certification_replica_client.urllib.request.urlopen",
        lambda *_args, **_kwargs: _Response(_delta(drifted)),
    )
    with pytest.raises(ReplicaBootstrapRequired, match="replication version changed"):
        _fetch_delta(
            base="https://authoritative.invalid",
            token="token",
            expected_release=RELEASE,
            state=_state(selected),
        )


def test_delta_accepts_exact_pinned_active_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "solana_roi.certification_replica_client.urllib.request.urlopen",
        lambda *_args, **_kwargs: _Response(_delta(ACTIVE_MANIFEST_REPLICATION_VERSION)),
    )
    payload = _fetch_delta(
        base="https://authoritative.invalid",
        token="token",
        expected_release=RELEASE,
        state=_state(ACTIVE_MANIFEST_REPLICATION_VERSION),
    )
    assert payload["replication_version"] == ACTIVE_MANIFEST_REPLICATION_VERSION


def test_delta_rejects_unknown_local_protocol_before_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def _unexpected(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("transport must not be called")

    monkeypatch.setattr("solana_roi.certification_replica_client.urllib.request.urlopen", _unexpected)
    with pytest.raises(ReplicaBootstrapRequired, match="unsupported"):
        _fetch_delta(
            base="https://authoritative.invalid",
            token="token",
            expected_release=RELEASE,
            state=_state(BASE_REPLICATION_VERSION + "+unknown-v2"),
        )
    assert called is False
