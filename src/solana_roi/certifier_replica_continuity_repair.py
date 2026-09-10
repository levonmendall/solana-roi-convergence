from __future__ import annotations

"""Keep certifier bootstrap and replica state durable across compatible releases.

Certification artifacts, manifests, pages, and deltas remain bound to the exact running
release. Durable SQLite state is reusable across a code release only when the
replication/bootstrap protocol, epoch, schema fingerprint, and watermark continuity
prove compatibility. A schema/epoch/protocol discontinuity or invalid local SQLite
state still forces a fresh logical bootstrap. Production can additionally require that
history-scale bootstrap work run only on a real certifier-owned persistent disk.
"""

import hashlib
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

from . import certification_logical_bootstrap_client as logical
from . import certification_replica_client as client
from .certification_incremental_replication import REPLICATION_VERSION

REPAIR_VERSION = "certifier-replica-continuity-v3-partial-cross-release-resume"
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
_ORIGINAL_LOGICAL_RESUME_PATHS: Any = None
_ORIGINAL_LOGICAL_CHECKPOINT_MATCHES: Any = None
_ORIGINAL_LOGICAL_BOOTSTRAP: Any = None
_RESUME_CONTEXT = threading.local()


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


def _stable_resume_paths(destination: Path, *, base: str, expected_release: str) -> tuple[Path, Path]:
    """Name durable partial state by source/protocol, never by deployment SHA."""

    del expected_release
    identity = f"{base}|{destination.name}|{logical.BOOTSTRAP_VERSION}|{REPLICATION_VERSION}"
    key = hashlib.sha256(identity.encode()).hexdigest()[:20]
    partial = destination.parent / f".certifier-logical-bootstrap-{key}.sqlite3"
    return partial, partial.with_suffix(partial.suffix + ".state.json")


def _compatible_checkpoint_matches(
    state: dict[str, Any] | None,
    *,
    expected_release: str,
    epoch: str,
    fingerprint: str,
    start_watermark: int,
) -> bool:
    """Accept an older-release partial only when delta catch-up can preserve truth.

    The saved watermark is deliberately allowed to be older than the current manifest
    watermark. It is retained as the completed replica's catch-up frontier so every
    source mutation since the original partial began must be replayed through the
    authoritative delta journal. A saved watermark ahead of the current manifest is a
    continuity regression and cannot be reused.
    """

    setattr(_RESUME_CONTEXT, "reused", False)
    setattr(_RESUME_CONTEXT, "saved_watermark", None)
    setattr(_RESUME_CONTEXT, "manifest_watermark", int(start_watermark))
    setattr(_RESUME_CONTEXT, "release_changed", False)
    if not isinstance(state, dict):
        return False
    try:
        saved_watermark = int(state.get("start_watermark") if state.get("start_watermark") is not None else -1)
    except (TypeError, ValueError):
        return False
    compatible = (
        str(state.get("client_version") or "") == logical.CLIENT_VERSION
        and str(state.get("bootstrap_version") or "") == logical.BOOTSTRAP_VERSION
        and str(state.get("replication_version") or "") == REPLICATION_VERSION
        and str(state.get("epoch") or "") == epoch
        and str(state.get("schema_fingerprint") or "") == fingerprint
        and saved_watermark >= 0
        and saved_watermark <= int(start_watermark)
    )
    if not compatible:
        return False

    prior_release = str(state.get("release_commit") or "")
    release_changed = bool(prior_release and prior_release != expected_release)
    # The current manifest has already passed exact-release validation before this
    # matcher is called. Rebinding this metadata is observability only; compatibility
    # continues to come from protocol/epoch/schema/watermark continuity.
    state["release_commit"] = expected_release
    setattr(_RESUME_CONTEXT, "reused", True)
    setattr(_RESUME_CONTEXT, "saved_watermark", saved_watermark)
    setattr(_RESUME_CONTEXT, "manifest_watermark", int(start_watermark))
    setattr(_RESUME_CONTEXT, "release_changed", release_changed)
    return True


def _cross_release_logical_bootstrap(
    destination: Path,
    *,
    base: str,
    token: str,
    expected_release: str,
) -> dict[str, Any]:
    """Run canonical bootstrap while retaining a compatible partial's old watermark."""

    assert _ORIGINAL_LOGICAL_BOOTSTRAP is not None
    setattr(_RESUME_CONTEXT, "reused", False)
    setattr(_RESUME_CONTEXT, "saved_watermark", None)
    setattr(_RESUME_CONTEXT, "manifest_watermark", None)
    setattr(_RESUME_CONTEXT, "release_changed", False)
    result = dict(
        _ORIGINAL_LOGICAL_BOOTSTRAP(
            destination,
            base=base,
            token=token,
            expected_release=expected_release,
        )
    )
    if bool(getattr(_RESUME_CONTEXT, "reused", False)):
        saved_watermark = getattr(_RESUME_CONTEXT, "saved_watermark", None)
        if not isinstance(saved_watermark, int) or saved_watermark < 0:
            raise RuntimeError("certification logical bootstrap resume watermark invalid")
        # This is the critical correctness property: completed tables may predate the
        # new release, so catch-up must begin at the original bootstrap frontier.
        result["watermark"] = saved_watermark
        result["partial_reused"] = True
        result["partial_reused_across_release"] = bool(getattr(_RESUME_CONTEXT, "release_changed", False))
        result["partial_resume_manifest_watermark"] = getattr(_RESUME_CONTEXT, "manifest_watermark", None)
        result["partial_resume_compatibility"] = "bootstrap_version+replication_version+epoch+schema_fingerprint+watermark_nonregression"
        result["page_release_binding_preserved"] = True
    else:
        result["partial_reused"] = False
        result["partial_reused_across_release"] = False
    return result


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
            "partial_bootstrap_release_change_requires_restart": False,
            "partial_bootstrap_preserves_original_watermark": True,
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
    global _ORIGINAL_LOGICAL_RESUME_PATHS, _ORIGINAL_LOGICAL_CHECKPOINT_MATCHES, _ORIGINAL_LOGICAL_BOOTSTRAP
    if _INSTALLED:
        return
    _ORIGINAL_REPLICA_PATH = client._replica_path
    _ORIGINAL_APPLY_DELTA = client._apply_delta
    _ORIGINAL_SYNCHRONIZE = client.synchronize_replica
    _ORIGINAL_STATUS = client.status
    _ORIGINAL_LOGICAL_RESUME_PATHS = logical._resume_paths
    _ORIGINAL_LOGICAL_CHECKPOINT_MATCHES = logical._checkpoint_matches
    _ORIGINAL_LOGICAL_BOOTSTRAP = logical.logical_bootstrap

    logical._resume_paths = _stable_resume_paths  # type: ignore[assignment]
    logical._checkpoint_matches = _compatible_checkpoint_matches  # type: ignore[assignment]
    logical.logical_bootstrap = _cross_release_logical_bootstrap  # type: ignore[assignment]
    # certification_replica_client imported the function by name, so update that
    # reference explicitly as well.
    client.logical_bootstrap = _cross_release_logical_bootstrap  # type: ignore[assignment]
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
        "partial_bootstrap_release_change_requires_restart": False,
        "partial_bootstrap_preserves_original_watermark": True,
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
