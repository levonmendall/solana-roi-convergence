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
    SHADOW_ENV,
    activate_runtime_database_environment,
)

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


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


def _shadow_once() -> dict[str, Any] | None:
    if not _truthy(SHADOW_ENV) or _truthy(ACTIVATE_ENV):
        return None
    release = _release_sha()
    if not release:
        raise RuntimeError("shadow storage requires exact release SHA")
    report = build_shadow_database(
        legacy_path=_legacy_path(),
        active_path=_active_path(),
        release_sha=release,
        replace_existing=True,
    )
    payload = {
        **report.__dict__,
        "mode": "shadow",
        "authoritative_runtime_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    print("ROI_ACTIVE_STORAGE_SHADOW " + json.dumps(payload, sort_keys=True, default=str), flush=True)
    return payload


def compose_runtime_storage() -> tuple[ObservationEventStore, DurablePaperTradingEngine, dict[str, Any]]:
    """Compose exactly one runtime store/engine without hidden package installers."""
    shadow = _shadow_once()
    if _truthy(ACTIVATE_ENV):
        release = _release_sha()
        if not release:
            raise RuntimeError("active storage requires exact release SHA")
        active_path = activate_runtime_database_environment(expected_release_sha=release)
        if active_path is None:
            raise RuntimeError("active storage enabled but no verified active database selected")
        install_active_certification_manifest()
        store: ObservationEventStore = ActiveObservationEventStore(active_path, expected_release_sha=release)
        engine: DurablePaperTradingEngine = ActiveDurablePaperTradingEngine(store=store)
        return store, engine, {
            "mode": "active",
            "path": str(active_path),
            "release_sha": release,
            "legacy_fallback": False,
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
