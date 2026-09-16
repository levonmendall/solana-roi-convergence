from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections import namedtuple
from pathlib import Path

from fastapi import FastAPI

from solana_roi import active_storage_epoch_rollover as rollover
from solana_roi import production_cleanup_runtime_install as runtime_install
from solana_roi import production_disk_ownership as disk_ownership
from solana_roi import sealed_epoch_reclamation as reclamation
from solana_roi.active_runtime import ActiveObservationEventStore
from solana_roi.storage_shadow_migration import build_shadow_database
from solana_roi.storage_transition import load_verified_checkpoint
from test_runtime_evidence_retention import _seed


PREVIOUS_RELEASE = "a" * 40
CURRENT_RELEASE = "b" * 40
DiskUsage = namedtuple("DiskUsage", "total used free")


def _seed_active(path: Path) -> None:
    legacy = path.with_name("initial-legacy.sqlite3")
    _seed(legacy)
    with sqlite3.connect(legacy) as connection:
        connection.execute(
            "CREATE TABLE certification_release_epochs("
            "release_commit TEXT PRIMARY KEY,started_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO certification_release_epochs VALUES(?,?)",
            (PREVIOUS_RELEASE, "2026-09-16T00:00:00+00:00"),
        )
    report = build_shadow_database(
        legacy_path=legacy,
        active_path=path,
        release_sha=PREVIOUS_RELEASE,
    )
    assert report.equivalent


def _append_generation(
    active: Path,
    generation: int,
    *,
    release_sha: str = PREVIOUS_RELEASE,
) -> None:
    store = ActiveObservationEventStore(active, expected_release_sha=release_sha)
    try:
        store.append(
            "storage_recovery_generation",
            f"2026-09-16T00:00:{generation:02d}+00:00",
            {"generation": generation},
        )
    finally:
        store.close()


def _advance_without_lifecycle_guard(
    active: Path,
    generation: int,
    *,
    release_sha: str = PREVIOUS_RELEASE,
) -> Path:
    """Create representative pre-repair retained generations on disposable data."""

    successor = active.with_name(f".{active.name}.fixture-next-{generation}")
    report = rollover._build_shadow_database_for_rollover(
        source=active,
        successor=successor,
        target_release_sha=release_sha,
    )
    assert report.equivalent
    sealed_dir = active.parent / rollover.SEALED_EPOCH_DIR / f"sealed-fixture-{generation}"
    sealed_dir.mkdir(parents=True)
    sealed = sealed_dir / active.name
    os.link(active, sealed)
    os.replace(successor, active)
    return sealed


def _over_ceiling_chain(active: Path) -> list[Path]:
    _seed_active(active)
    sealed: list[Path] = []
    for generation in range(1, 5):
        _append_generation(active, generation)
        sealed.append(_advance_without_lifecycle_guard(active, generation))
    uncertain_dir = active.parent / rollover.SEALED_EPOCH_DIR / "sealed-uncertain"
    uncertain_dir.mkdir(parents=True)
    uncertain = uncertain_dir / active.name
    with sqlite3.connect(uncertain) as connection:
        connection.execute("CREATE TABLE unrelated(value TEXT NOT NULL)")
    sealed.append(uncertain)
    return sealed


def _establish_prior_release(active: Path, monkeypatch) -> None:
    monkeypatch.setenv("RENDER_GIT_COMMIT", PREVIOUS_RELEASE)
    lease = asyncio.run(
        disk_ownership.acquire_runtime_disk_lease(active, timeout_seconds=1.0)
    )
    try:
        marker = disk_ownership.mark_same_release_established(active, lease)
    finally:
        lease.release()
    assert marker["release_commit"] == PREVIOUS_RELEASE


def test_supported_startup_recovery_cleans_proven_prefix_then_starts_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", PREVIOUS_RELEASE)
    monkeypatch.setenv("RENDER_GIT_COMMIT", PREVIOUS_RELEASE)
    active = tmp_path / "active.sqlite3"
    sealed = _over_ceiling_chain(active)
    assert len(sealed) == 5
    checkpoint = load_verified_checkpoint(active)
    checkpoint_id = str(checkpoint["checkpoint_id"])
    _establish_prior_release(active, monkeypatch)

    monkeypatch.setenv("RENDER_GIT_COMMIT", CURRENT_RELEASE)
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "true")
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(active))
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_MAINTENANCE_SECONDS", "0")
    monkeypatch.setenv("SOLANA_ROI_PRODUCTION_CLEANUP_ROLE", "authoritative")
    monkeypatch.setenv(
        runtime_install.sealed_reclamation.ENABLED_ENV,
        "true",
    )
    monkeypatch.setenv(
        runtime_install.sealed_reclamation.CHECKPOINT_APPROVAL_ENV,
        checkpoint_id,
    )
    monkeypatch.delenv(runtime_install.cleanup.ENABLED_ENV, raising=False)

    dry_run = reclamation.preflight_sealed_epoch_reclamation(
        active,
        expected_release_sha=CURRENT_RELEASE,
        approved_checkpoint_id=checkpoint_id,
    )
    assert dry_run["read_only"] is True
    assert dry_run["operator_approval_bound"] is True
    assert dry_run["candidate_count"] == 5
    assert len(dry_run["eligible_candidates"]) >= 3
    assert dry_run["protected_candidate_count"] >= 1
    assert all(path.exists() for path in sealed)

    # Recovery needs no successor-sized temporary allocation. Model headroom
    # far below the 2 GiB rollover reserve while still proving physical reclaim.
    usages = iter(
        [
            DiskUsage(total=20 * 1024**3, used=20 * 1024**3 - 64 * 1024**2, free=64 * 1024**2),
            DiskUsage(total=20 * 1024**3, used=20 * 1024**3 - 128 * 1024**2, free=128 * 1024**2),
        ]
    )
    monkeypatch.setattr(reclamation.shutil, "disk_usage", lambda _path: next(usages))

    calls: list[str] = []

    async def normal_startup(_stop: asyncio.Event) -> None:
        store = ActiveObservationEventStore(active, expected_release_sha=CURRENT_RELEASE)
        store.close()
        calls.append("normal_startup")
        runtime_install.bootstrap._BOOTSTRAP_STATE["state"] = "full_runtime"

    monkeypatch.setattr(runtime_install.bootstrap, "_bootstrap_and_run", normal_startup)
    app = FastAPI()
    runtime_install.install_production_cleanup_runtime(app)
    asyncio.run(runtime_install.bootstrap._bootstrap_and_run(asyncio.Event()))

    result = app.state.roi_sealed_epoch_reclamation
    assert result["status"] == "complete"
    assert result["startup_recovery"] is True
    assert result["same_release_established"] is False
    assert result["startup_recovery_binding"]["prior_established_release_sha"] == PREVIOUS_RELEASE
    assert result["checkpoint_id"] == checkpoint_id
    assert result["deleted_candidate_count"] >= 3
    assert result["filesystem_free_bytes_delta"] == 64 * 1024**2
    assert calls == ["normal_startup"]
    assert runtime_install.bootstrap._BOOTSTRAP_STATE["state"] == "full_runtime"
    assert disk_ownership.same_release_established(active) is True
    assert sum(path.exists() for path in sealed) == dry_run["protected_candidate_count"]

    # One later rollover reaches, but does not exceed, the two-generation
    # lifecycle boundary. Same-release maintenance then reclaims its newly
    # proven predecessor before another rollover can leave the runtime stuck.
    _append_generation(active, 5, release_sha=CURRENT_RELEASE)
    new_sealed = _advance_without_lifecycle_guard(
        active,
        5,
        release_sha=CURRENT_RELEASE,
    )
    current_checkpoint_id = str(load_verified_checkpoint(active)["checkpoint_id"])
    monkeypatch.setenv(
        runtime_install.sealed_reclamation.CHECKPOINT_APPROVAL_ENV,
        current_checkpoint_id,
    )
    later_usages = iter(
        [
            DiskUsage(total=20 * 1024**3, used=10 * 1024**3, free=10 * 1024**3),
            DiskUsage(total=20 * 1024**3, used=9 * 1024**3, free=11 * 1024**3),
        ]
    )
    monkeypatch.setattr(
        reclamation.shutil,
        "disk_usage",
        lambda _path: next(later_usages),
    )
    lease = asyncio.run(
        disk_ownership.acquire_runtime_disk_lease(active, timeout_seconds=1.0)
    )
    try:
        later = runtime_install.sealed_reclamation.execute_enabled(active, lease)
    finally:
        lease.release()
    assert later["status"] == "complete"
    assert later["same_release_established"] is True
    assert later["startup_recovery"] is False
    assert later["checkpoint_id"] == current_checkpoint_id
    assert not new_sealed.exists()
    inventory = rollover._enforce_sealed_epoch_bound(active)
    assert (
        inventory["physical_inode_count"]
        < rollover.MAX_UNRECLAIMED_SEALED_EPOCHS
    )


def test_startup_recovery_rejects_stale_checkpoint_approval(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", PREVIOUS_RELEASE)
    monkeypatch.setenv("RENDER_GIT_COMMIT", PREVIOUS_RELEASE)
    active = tmp_path / "active.sqlite3"
    sealed = _over_ceiling_chain(active)
    _establish_prior_release(active, monkeypatch)
    monkeypatch.setenv("RENDER_GIT_COMMIT", CURRENT_RELEASE)
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "true")
    monkeypatch.setenv("SOLANA_ROI_PRODUCTION_CLEANUP_ROLE", "authoritative")
    monkeypatch.setenv(runtime_install.sealed_reclamation.ENABLED_ENV, "true")
    monkeypatch.setenv(
        runtime_install.sealed_reclamation.CHECKPOINT_APPROVAL_ENV,
        "stale-checkpoint-id",
    )
    lease = asyncio.run(
        disk_ownership.acquire_runtime_disk_lease(active, timeout_seconds=1.0)
    )
    try:
        try:
            runtime_install.sealed_reclamation.execute_enabled(active, lease)
        except runtime_install.cleanup.CleanupBlocked as exc:
            assert "approval is not bound" in str(exc)
        else:
            raise AssertionError("stale recovery approval unexpectedly executed")
    finally:
        lease.release()
    assert all(path.exists() for path in sealed)


def test_interrupted_startup_recovery_resumes_before_normal_startup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", PREVIOUS_RELEASE)
    monkeypatch.setenv("RENDER_GIT_COMMIT", PREVIOUS_RELEASE)
    active = tmp_path / "active.sqlite3"
    sealed = _over_ceiling_chain(active)
    checkpoint_id = str(load_verified_checkpoint(active)["checkpoint_id"])
    _establish_prior_release(active, monkeypatch)

    monkeypatch.setenv("RENDER_GIT_COMMIT", CURRENT_RELEASE)
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "true")
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(active))
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_MAINTENANCE_SECONDS", "0")
    monkeypatch.setenv("SOLANA_ROI_PRODUCTION_CLEANUP_ROLE", "authoritative")
    monkeypatch.setenv(runtime_install.sealed_reclamation.ENABLED_ENV, "true")
    monkeypatch.setenv(
        runtime_install.sealed_reclamation.CHECKPOINT_APPROVAL_ENV,
        checkpoint_id,
    )
    monkeypatch.delenv(runtime_install.cleanup.ENABLED_ENV, raising=False)

    calls: list[str] = []

    async def normal_startup(_stop: asyncio.Event) -> None:
        calls.append("normal_startup")
        runtime_install.bootstrap._BOOTSTRAP_STATE["state"] = "full_runtime"

    monkeypatch.setattr(runtime_install.bootstrap, "_bootstrap_and_run", normal_startup)
    monkeypatch.setattr(
        reclamation.shutil,
        "disk_usage",
        lambda _path: DiskUsage(
            total=20 * 1024**3,
            used=19 * 1024**3,
            free=1024**3,
        ),
    )
    original_write = reclamation._write_receipt
    interrupted = False

    def interrupt_after_first_unlink(path: Path, payload: dict) -> Path:
        nonlocal interrupted
        result = original_write(path, payload)
        if len(payload.get("deleted_candidate_paths") or ()) == 1 and not interrupted:
            interrupted = True
            raise RuntimeError("synthetic startup recovery interruption")
        return result

    monkeypatch.setattr(reclamation, "_write_receipt", interrupt_after_first_unlink)
    app = FastAPI()
    runtime_install.install_production_cleanup_runtime(app)
    asyncio.run(runtime_install.bootstrap._bootstrap_and_run(asyncio.Event()))

    assert app.state.roi_production_data_cleanup["status"] == "blocked"
    assert app.state.roi_production_data_cleanup["phase"] == "sealed_epoch_reclamation"
    assert calls == []
    assert sum(path.exists() for path in sealed) == len(sealed) - 1
    receipt_path = Path(str(active) + reclamation.RECLAMATION_RECEIPT_SUFFIX)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "intent"
    assert len(receipt["deleted_candidate_paths"]) == 1

    monkeypatch.setattr(reclamation, "_write_receipt", original_write)
    asyncio.run(runtime_install.bootstrap._bootstrap_and_run(asyncio.Event()))

    result = app.state.roi_sealed_epoch_reclamation
    assert result["status"] == "complete"
    assert result["startup_recovery"] is True
    assert result["startup_recovery_binding"]["candidate_count_before"] > 2
    assert result["recovered_missing_candidate_paths"] == receipt["deleted_candidate_paths"]
    assert calls == ["normal_startup"]
    assert runtime_install.bootstrap._BOOTSTRAP_STATE["state"] == "full_runtime"
    assert disk_ownership.same_release_established(active) is True
