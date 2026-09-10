from __future__ import annotations

"""Keep certifier replica state durable and release-compatible without weakening truth.

Certification artifacts remain bound to the exact running release. The SQLite replica
itself is reusable across a code release when the authoritative replication protocol,
epoch, schema fingerprint, and watermark continuity still prove compatibility. A
schema/epoch/protocol discontinuity or invalid local SQLite state still forces a full
logical bootstrap, but production can require that such a bootstrap only run on a real
certifier-owned persistent disk so ephemeral replacements cannot repeatedly reread the
authoritative history-scale database.
"""

import os
import tempfile
from pathlib import Path
from typing import Any

from . import certification_replica_client as client
from .certification_incremental_replication import REPLICATION_VERSION

REPAIR_VERSION = "certifier-replica-continuity-v2-durable-bootstrap-guard"
DEFAULT_DURABLE_ROOT = Path("/var/data")
DEFAULT_REPLICA_NAME = "solana-roi-certifier-replica.sqlite3"

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False

_INSTALLED = False
_ORIGINAL_REPLICA_PATH: Any = None
_ORIGINAL_APPLY_DELTA: Any = None
_ORIGINAL_SYNCHRONIZE: Any = None
_ORIGINAL_STATUS: Any = None


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _durable_root() -> Path:
    configured = os.getenv("SOLANA_ROI_CERTIFIER_DURABLE_ROOT", "").strip()
    return Path(configured) if configured else DEFAULT_DURABLE_ROOT


def _durable_mount_available(root: Path) -> bool:
    try:
        return root.is_dir() and root.is_mount() and os.access(root, os.W_OK)
    except OSError:
        return False


def _replica_path() -> Path:
    configured = os.getenv("SOLANA_ROI_CERTIFIER_REPLICA_PATH", "").strip()
    if configured:
        path = Path(configured)
    else:
        root = _durable_root()
        path = root / DEFAULT_REPLICA_NAME if _durable_mount_available(root) else Path(tempfile.gettempdir()) / DEFAULT_REPLICA_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _replica_storage_is_durable(replica: Path) -> bool:
    root = _durable_root()
    if not _durable_mount_available(root):
        return False
    try:
        resolved = replica.resolve(strict=False)
        durable = root.resolve(strict=False)
    except OSError:
        return False
    return resolved == durable or durable in resolved.parents


def _durable_replica_required() -> bool:
    return _truthy_env("SOLANA_ROI_CERTIFIER_REQUIRE_DURABLE_REPLICA", default=False)


def _require_durable_before_history_bootstrap(replica: Path) -> None:
    if _durable_replica_required() and not _replica_storage_is_durable(replica):
        raise RuntimeError(
            "certifier durable replica storage required before logical bootstrap; "
            "refusing history-scale authoritative reread on ephemeral filesystem"
        )


def _bootstrap(replica: Path, *, base: str, token: str, expected_release: str) -> dict[str, Any]:
    _require_durable_before_history_bootstrap(replica)
    return client._bootstrap(replica, base=base, token=token, expected_release=expected_release)


def _apply_delta(replica: Path, state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    assert _ORIGINAL_APPLY_DELTA is not None
    result = _ORIGINAL_APPLY_DELTA(replica, state, payload)
    release = str(payload.get("release_commit") or "")
    if not release:
        raise client.ReplicaDeltaTransportError("authoritative certification delta release missing")
    result["release_commit"] = release
    result["replica_reused_across_release"] = str(state.get("release_commit") or "") not in {"", release}
    result["replica_storage_durable"] = _replica_storage_is_durable(replica)
    client._atomic_state(client._state_path(replica), result)
    return result


def _synchronize_replica(*, base: str, token: str, expected_release: str) -> tuple[Path, dict[str, Any]]:
    """Reuse a valid replica across releases; authoritative identity decides compatibility."""

    if not base or not token:
        raise RuntimeError("incremental certification replica source is not configured")
    replica = client._replica_path()
    state = client._read_state(replica)
    if (
        state is None
        or not replica.is_file()
        or str(state.get("replication_version") or "") != REPLICATION_VERSION
    ):
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)

    try:
        watermark = int(state.get("watermark") if state.get("watermark") is not None else -1)
    except (TypeError, ValueError):
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)
    if watermark < 0 or not str(state.get("epoch") or "") or not str(state.get("schema_fingerprint") or ""):
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)

    try:
        client._validate_sqlite(replica)
    except BaseException:
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)

    prior_release = str(state.get("release_commit") or "")
    try:
        result = client._catch_up_deltas(
            replica,
            state,
            base=base,
            token=token,
            expected_release=expected_release,
        )
    except client.ReplicaBootstrapRequired:
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)

    result["release_commit"] = expected_release
    result["catchup_complete"] = True
    result["replica_reused_across_release"] = bool(prior_release and prior_release != expected_release)
    result["replica_compatibility_identity"] = "replication_version+epoch+schema_fingerprint+watermark"
    result["artifact_release_binding_preserved"] = True
    result["replica_storage_durable"] = _replica_storage_is_durable(replica)
    client._atomic_state(client._state_path(replica), result)
    return replica, result


def _status() -> dict[str, Any]:
    assert _ORIGINAL_STATUS is not None
    payload = dict(_ORIGINAL_STATUS())
    replica = client._replica_path()
    durable = _replica_storage_is_durable(replica)
    required = _durable_replica_required()
    payload.update(
        {
            "replica_storage_path": str(replica),
            "replica_storage_durable": durable,
            "replica_storage_kind": "render_persistent_disk" if durable else "ephemeral_filesystem",
            "durable_replica_required": required,
            "history_bootstrap_permitted": (not required) or durable,
            "ephemeral_history_bootstrap_blocked": required and not durable,
            "replica_compatibility_identity": "replication_version+epoch+schema_fingerprint+watermark",
            "release_change_requires_bootstrap": False,
            "artifact_release_binding_preserved": True,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }
    )
    return payload


def configure_certifier_replica_continuity_repair() -> None:
    global _INSTALLED, _ORIGINAL_REPLICA_PATH, _ORIGINAL_APPLY_DELTA, _ORIGINAL_SYNCHRONIZE, _ORIGINAL_STATUS
    if _INSTALLED:
        return
    _ORIGINAL_REPLICA_PATH = client._replica_path
    _ORIGINAL_APPLY_DELTA = client._apply_delta
    _ORIGINAL_SYNCHRONIZE = client.synchronize_replica
    _ORIGINAL_STATUS = client.status

    client._replica_path = _replica_path  # type: ignore[assignment]
    client._apply_delta = _apply_delta  # type: ignore[assignment]
    client.synchronize_replica = _synchronize_replica  # type: ignore[assignment]
    client.status = _status  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    replica = _replica_path()
    durable = _replica_storage_is_durable(replica)
    required = _durable_replica_required()
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "replica_path": str(replica),
        "replica_storage_durable": durable,
        "durable_replica_required": required,
        "history_bootstrap_permitted": (not required) or durable,
        "ephemeral_history_bootstrap_blocked": required and not durable,
        "release_change_requires_bootstrap": False,
        "replica_compatibility_identity": "replication_version+epoch+schema_fingerprint+watermark",
        "artifact_release_binding_preserved": True,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "configure_certifier_replica_continuity_repair",
    "status",
]
