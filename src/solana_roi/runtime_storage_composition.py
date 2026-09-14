from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .active_runtime import ActiveDurablePaperTradingEngine, ActiveObservationEventStore
from .certification_active_manifest import install_active_certification_manifest
from .durable_engine import DurablePaperTradingEngine
from .observation_store import ObservationEventStore
from .storage_shadow_migration import build_shadow_database
from .storage_transition import (
    ACTIVE_PATH_ENV,
    ACTIVATE_ENV,
    LEGACY_PATH_ENV,
    LEGACY_RECORD_ENV,
    SHADOW_ENV,
    activate_runtime_database_environment,
)

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
FINALIZE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY"
HIDE_LEGACY_ENV = "SOLANA_ROI_ACTIVE_STORAGE_HIDE_LEGACY"
LEGACY_QUARANTINE_ENV = "SOLANA_ROI_LEGACY_QUARANTINE_DIR"


def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _release_sha() -> str | None:
    return (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip() or None


def _legacy_path() -> Path:
    return Path(os.getenv(LEGACY_PATH_ENV, "data/solana-roi.sqlite3"))


def _active_path() -> Path:
    raw = (os.getenv(ACTIVE_PATH_ENV) or "").strip()
    if not raw:
        raise RuntimeError(f"{ACTIVE_PATH_ENV} must be configured for shadow/active storage")
    return Path(raw)


def _report(prefix: str, payload: dict[str, Any]) -> None:
    print(prefix + " " + json.dumps(payload, sort_keys=True, default=str), flush=True)


def _build_exact_snapshot(*, release: str, mode: str) -> dict[str, Any]:
    legacy = _legacy_path()
    active = _active_path()
    report = build_shadow_database(
        legacy_path=legacy,
        active_path=active,
        release_sha=release,
        replace_existing=True,
    )
    payload = {
        **report.__dict__,
        "mode": mode,
        "authoritative_runtime_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    _report("ROI_ACTIVE_STORAGE_SNAPSHOT", payload)
    return payload


def _shadow_once() -> dict[str, Any] | None:
    if not _truthy(SHADOW_ENV) or _truthy(ACTIVATE_ENV):
        return None
    release = _release_sha()
    if not release:
        raise RuntimeError("shadow storage requires exact release SHA")
    return _build_exact_snapshot(release=release, mode="shadow")


def _quarantine_legacy_for_independence_proof(legacy: Path) -> dict[str, Any]:
    """Make the legacy SQLite family physically unavailable without deleting it.

    Sidecars are moved first and the main DB last, all on the same persistent
    disk.  The operation is reversible; no historical bytes are deleted.
    """
    root_raw = (os.getenv(LEGACY_QUARANTINE_ENV) or "").strip()
    root = Path(root_raw) if root_raw else legacy.parent / "legacy-quarantine"
    root.mkdir(parents=True, exist_ok=True)
    moved: list[dict[str, str]] = []
    for suffix in ("-wal", "-shm", ""):
        source = Path(str(legacy) + suffix)
        target = root / (legacy.name + suffix)
        if source.exists() and target.exists():
            raise RuntimeError(f"legacy independence proof blocked: both source and quarantine exist: {source}")
        if source.exists():
            os.replace(source, target)
            moved.append({"source": str(source), "target": str(target)})
    if legacy.exists():
        raise RuntimeError("legacy independence proof blocked: legacy main database remains available")
    if not (root / legacy.name).exists():
        raise RuntimeError("legacy independence proof blocked: quarantined legacy main database missing")
    try:
        fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
    os.environ[LEGACY_RECORD_ENV] = str(root / legacy.name)
    payload = {
        "status": "legacy_physically_unavailable_at_original_path",
        "original_path": str(legacy),
        "quarantine_path": str(root / legacy.name),
        "moved": moved,
        "deleted": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    _report("ROI_ACTIVE_STORAGE_LEGACY_QUARANTINE", payload)
    return payload


def compose_runtime_storage() -> tuple[ObservationEventStore, DurablePaperTradingEngine, dict[str, Any]]:
    """Compose exactly one runtime store/engine without hidden package installers."""
    shadow = _shadow_once()
    if _truthy(ACTIVATE_ENV):
        release = _release_sha()
        if not release:
            raise RuntimeError("active storage requires exact release SHA")
        legacy_before = _legacy_path()
        finalization: dict[str, Any] | None = None
        if _truthy(FINALIZE_ENV):
            # Service startup is quiescent here: no production runtime store has
            # opened yet. Build one final exact snapshot from that boundary so
            # activation never relies on an older shadow copy.
            finalization = _build_exact_snapshot(release=release, mode="finalize")
        active_path = activate_runtime_database_environment(expected_release_sha=release)
        if active_path is None:
            raise RuntimeError("active storage enabled but no verified active database selected")
        quarantine: dict[str, Any] | None = None
        if _truthy(HIDE_LEGACY_ENV):
            quarantine = _quarantine_legacy_for_independence_proof(legacy_before)
        install_active_certification_manifest()
        store: ObservationEventStore = ActiveObservationEventStore(active_path, expected_release_sha=release)
        engine: DurablePaperTradingEngine = ActiveDurablePaperTradingEngine(store=store)
        return store, engine, {
            "mode": "active",
            "path": str(active_path),
            "release_sha": release,
            "legacy_fallback": False,
            "finalization": finalization,
            "legacy_quarantine": quarantine,
            "paper_only": True,
            "live_money_authority": False,
        }

    store = ObservationEventStore(_legacy_path())
    engine = DurablePaperTradingEngine(store=store)
    return store, engine, {
        "mode": "legacy_authoritative",
        "path": str(store.path),
        "shadow": shadow,
        "paper_only": True,
        "live_money_authority": False,
    }
