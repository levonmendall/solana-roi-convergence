from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .active_runtime import ActiveDurablePaperTradingEngine, ActiveObservationEventStore
from .certification_active_manifest import install_active_certification_manifest
from .durable_engine import DurablePaperTradingEngine
from .legacy_storage_containment import LegacyContainedObservationEventStore
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
RESTORE_LEGACY_ENV = "SOLANA_ROI_ACTIVE_STORAGE_RESTORE_LEGACY"
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


def _quarantine_root(legacy: Path) -> Path:
    root_raw = (os.getenv(LEGACY_QUARANTINE_ENV) or "").strip()
    return Path(root_raw) if root_raw else legacy.parent / "legacy-quarantine"


def _report(prefix: str, payload: dict[str, Any]) -> None:
    print(prefix + " " + json.dumps(payload, sort_keys=True, default=str), flush=True)


def _legacy_checkpoint_row(path: Path) -> tuple[int, str] | None:
    """Read the singleton paper checkpoint with a bounded primary-key lookup."""
    if not path.is_file():
        raise RuntimeError(f"legacy paper checkpoint unavailable: database missing:{path}")
    uri = f"file:{path.resolve()}?mode=ro&cache=private"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        try:
            row = connection.execute(
                "SELECT last_engine_event_id,state_sha256 FROM paper_engine_checkpoint WHERE id=1"
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise RuntimeError("legacy paper checkpoint unavailable: checkpoint table missing or unreadable") from exc
        if row is None:
            return None
        return int(row[0]), str(row[1])
    finally:
        connection.close()


def _require_materialized_legacy_checkpoint_for_snapshot(path: Path) -> tuple[int, str]:
    """Block migration before any history-scaled paper-state proof can run.

    A normal legacy runtime startup owns the expensive event-ledger verification.
    When that verifier proves the no-engine-event genesis case, composition
    materializes the exact durable checkpoint once. Shadow/finalize migration is
    intentionally allowed to consume only that bounded singleton state.
    """
    row = _legacy_checkpoint_row(path)
    if row is None:
        raise RuntimeError(
            "storage snapshot blocked: legacy paper checkpoint is not materialized; "
            "run one normal legacy-authoritative startup on this exact release before shadow/finalize"
        )
    return row


def _materialize_verified_genesis_checkpoint_if_needed(
    store: ObservationEventStore,
    engine: DurablePaperTradingEngine,
) -> dict[str, Any] | None:
    """Persist exact genesis only after DurablePaperTradingEngine verified history.

    If engine history exists without a checkpoint, the durable engine constructor
    fails closed before this helper can run. Therefore a missing row here means the
    already-completed canonical restore proved that no paper-engine event exists and
    the in-memory engine remains at exact genesis. Persisting its own `_state_dict`
    removes the need for migration to rescan the historical events table.
    """
    with store._lock:
        row = store.db.execute(
            "SELECT last_engine_event_id,state_sha256 FROM paper_engine_checkpoint WHERE id=1"
        ).fetchone()
    if row is not None:
        return None
    if int(getattr(engine, "_last_engine_event_id", -1)) != 0:
        raise RuntimeError("legacy genesis checkpoint materialization blocked: nonzero engine event id")
    state = engine._state_dict()
    expected = {
        "schema": "roi-convergence-paper-engine-checkpoint.v1",
        "strategy_version": engine.config.version,
        "initial_capital_usd": engine.config.initial_capital_usd,
        "cash_usd": engine.config.initial_capital_usd,
        "marks": {},
        "trade_start_nav": {},
        "candidates": {},
        "positions": {},
        "closed": [],
    }
    if state != expected:
        raise RuntimeError("legacy genesis checkpoint materialization blocked: engine state is not exact genesis")
    engine._save_checkpoint()
    with store._lock:
        persisted = store.db.execute(
            "SELECT last_engine_event_id,state_sha256 FROM paper_engine_checkpoint WHERE id=1"
        ).fetchone()
    if persisted is None or int(persisted[0]) != 0 or not str(persisted[1]):
        raise RuntimeError("legacy genesis checkpoint materialization blocked: durable write not verified")
    payload = {
        "status": "verified_genesis_checkpoint_materialized",
        "path": str(store.path),
        "last_engine_event_id": 0,
        "state_sha256": str(persisted[1]),
        "source": "durable_engine_verified_restore",
        "history_rescanned_by_migration": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    _report("ROI_LEGACY_GENESIS_CHECKPOINT_MATERIALIZED", payload)
    return payload


def _build_exact_snapshot(*, release: str, mode: str) -> dict[str, Any]:
    legacy = _legacy_path()
    active = _active_path()
    checkpoint_event_id, checkpoint_sha256 = _require_materialized_legacy_checkpoint_for_snapshot(legacy)
    report = build_shadow_database(
        legacy_path=legacy,
        active_path=active,
        release_sha=release,
        replace_existing=True,
    )
    payload = {
        **report.__dict__,
        "mode": mode,
        "source_checkpoint_event_id": checkpoint_event_id,
        "source_checkpoint_sha256": checkpoint_sha256,
        "source_checkpoint_bounded_lookup": True,
        "single_pinned_source_transaction": True,
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
    disk. The original main path remains absent unless the whole family has been
    moved. No historical bytes are deleted.
    """
    root = _quarantine_root(legacy)
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


def _restore_legacy_from_quarantine(legacy: Path) -> dict[str, Any]:
    """Restore a quarantined legacy SQLite family before any store is opened.

    Sidecars are restored before the main file, mirroring the quarantine safety
    boundary: the original main path does not reappear until all available
    matching sidecars are in place. This is an explicit rollback mechanism, not
    a hidden fallback from active mode.
    """
    root = _quarantine_root(legacy)
    quarantined_main = root / legacy.name
    if legacy.exists() and quarantined_main.exists():
        raise RuntimeError("legacy restore blocked: main database exists at both original and quarantine paths")
    if legacy.exists() and not quarantined_main.exists():
        return {
            "status": "legacy_already_available",
            "original_path": str(legacy),
            "restored": [],
            "deleted": False,
        }
    if not quarantined_main.exists():
        raise RuntimeError("legacy restore blocked: quarantined main database missing")
    legacy.parent.mkdir(parents=True, exist_ok=True)
    restored: list[dict[str, str]] = []
    for suffix in ("-wal", "-shm", ""):
        source = root / (legacy.name + suffix)
        target = Path(str(legacy) + suffix)
        if source.exists() and target.exists():
            raise RuntimeError(f"legacy restore blocked: both source and destination exist: {target}")
        if source.exists():
            os.replace(source, target)
            restored.append({"source": str(source), "target": str(target)})
    if not legacy.exists():
        raise RuntimeError("legacy restore blocked: restored main database missing")
    try:
        fd = os.open(legacy.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
    payload = {
        "status": "legacy_restored_to_original_path",
        "original_path": str(legacy),
        "quarantine_path": str(quarantined_main),
        "restored": restored,
        "deleted": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    _report("ROI_ACTIVE_STORAGE_LEGACY_RESTORE", payload)
    return payload


def compose_runtime_storage() -> tuple[ObservationEventStore, DurablePaperTradingEngine, dict[str, Any]]:
    """Compose exactly one runtime store/engine without hidden package installers."""
    if _truthy(HIDE_LEGACY_ENV) and _truthy(RESTORE_LEGACY_ENV):
        raise RuntimeError("storage composition blocked: hide-legacy and restore-legacy cannot both be enabled")

    restored: dict[str, Any] | None = None
    if _truthy(RESTORE_LEGACY_ENV):
        restored = _restore_legacy_from_quarantine(_legacy_path())

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
            "legacy_restore": restored,
            "paper_only": True,
            "live_money_authority": False,
        }

    # Until the active-store cutover is proven, legacy remains authoritative but
    # no longer accumulates every positively classified low-value row forever.
    # Containment is bounded, best-effort maintenance only and cannot authorize
    # or block strategy/portfolio writes.
    contained_store = LegacyContainedObservationEventStore(_legacy_path())
    engine = DurablePaperTradingEngine(store=contained_store)
    genesis_checkpoint = _materialize_verified_genesis_checkpoint_if_needed(contained_store, engine)
    return contained_store, engine, {
        "mode": "legacy_authoritative",
        "path": str(contained_store.path),
        "shadow": shadow,
        "legacy_restore": restored,
        "legacy_containment": contained_store.containment_status(),
        "legacy_genesis_checkpoint": genesis_checkpoint,
        "paper_only": True,
        "live_money_authority": False,
    }
