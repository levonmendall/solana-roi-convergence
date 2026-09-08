from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any, Callable


DIAGNOSTIC_VERSION = "certification-status-private-log-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
DEEP_BUILDERS_INVOKED = False
SQLITE_OPENED = False
FILE_CONTENTS_READ = False
STATE_MUTATED = False
POLL_SECONDS = 2.0
MAX_WAIT_SECONDS = 150.0
MINIMUM_PUBLISHER_SUCCESSES = 2

_LOGGER = logging.getLogger(__name__)
_LOG_EMITTED = False
_INSTALLED = False


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _release_commit() -> str:
    for key in ("RENDER_GIT_COMMIT", "GITHUB_SHA", "SOLANA_ROI_RELEASE_COMMIT"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return "unknown"


def _bounded_blockers(value: Any, *, limit: int = 20) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item)[:240] for item in value[:limit]]


def _cache_summary(cache: Any) -> dict[str, Any]:
    cache = cache if isinstance(cache, dict) else {}
    return {
        "snapshot_available": bool(cache.get("snapshot_available")),
        "snapshot_fresh": bool(cache.get("snapshot_fresh")),
        "snapshot_age_seconds": cache.get("snapshot_age_seconds"),
        "attempts": int(cache.get("attempts", 0) or 0),
        "successes": int(cache.get("successes", 0) or 0),
        "failures": int(cache.get("failures", 0) or 0),
        "consecutive_failures": int(cache.get("consecutive_failures", 0) or 0),
        "guard_rejections": int(cache.get("guard_rejections", 0) or 0),
        "last_error_type": cache.get("last_error_type"),
        "last_duration_seconds": cache.get("last_duration_seconds"),
        "last_completed_at": cache.get("last_completed_at"),
        "last_generation_id": cache.get("last_generation_id"),
    }


def _disk_metadata() -> dict[str, Any]:
    root = Path("/var/data")
    result: dict[str, Any] = {"root": str(root), "disk_free_bytes": None, "canonical_wal_bytes": None}
    try:
        result["disk_free_bytes"] = int(shutil.disk_usage(root).free)
    except OSError:
        pass
    db_raw = os.getenv("SOLANA_ROI_DB_PATH", "").strip()
    if db_raw:
        try:
            result["canonical_wal_bytes"] = int(Path(db_raw + "-wal").stat().st_size)
        except OSError:
            result["canonical_wal_bytes"] = 0
    return result


def _collect_status() -> dict[str, Any]:
    """Collect only already-published in-process status and filesystem metadata.

    No deep certification builder is called here. Cached accessors return immutable
    snapshots or their existing fail-closed payloads; coordinator status is in-memory
    state only. This diagnostic never opens SQLite or reads file contents.
    """
    from . import certification_generation_coordinator as coordinator
    from . import certification_generation_runtime_repair as generation
    from . import e2e_status_read_boundary_repair as e2e
    from . import production_proof_read_boundary_repair as proof
    from . import render_runtime_bootstrap_repair as bootstrap

    e2e_cache = _cache_summary(e2e._cache_state())
    proof_cache = _cache_summary(proof._cache_state())
    forward_cache = _cache_summary(generation._forward_cache_state())

    e2e_payload = e2e._cached_e2e_status()
    proof_payload = proof._cached_production_proof()
    forward_payload = generation._cached_forward_endpoint()

    overall = e2e_payload.get("overall") if isinstance(e2e_payload, dict) else {}
    overall = overall if isinstance(overall, dict) else {}
    final_certification = proof_payload.get("final_certification") if isinstance(proof_payload, dict) else {}
    final_certification = final_certification if isinstance(final_certification, dict) else {}
    release_gate = proof_payload.get("batch6_release_gate") if isinstance(proof_payload, dict) else {}
    release_gate = release_gate if isinstance(release_gate, dict) else {}
    proof_release = proof_payload.get("release") if isinstance(proof_payload, dict) else {}
    proof_release = proof_release if isinstance(proof_release, dict) else {}

    coordinator_state = coordinator.status()
    publisher_caches = (e2e_cache, proof_cache, forward_cache)
    all_minimum_successes = all(
        int(cache.get("successes", 0) or 0) >= MINIMUM_PUBLISHER_SUCCESSES for cache in publisher_caches
    )
    all_fresh = all(bool(cache.get("snapshot_fresh")) for cache in publisher_caches)

    return {
        "diagnostic": "certification_status_private_log",
        "diagnostic_version": DIAGNOSTIC_VERSION,
        "release_commit": _release_commit(),
        "all_publishers_have_minimum_successes": all_minimum_successes,
        "minimum_publisher_successes": MINIMUM_PUBLISHER_SUCCESSES,
        "all_snapshots_fresh": all_fresh,
        "runtime_bootstrap": bootstrap._public_status(),
        "coordinator": {
            "active": bool(coordinator_state.get("active")),
            "active_surface": coordinator_state.get("active_surface"),
            "acquisitions": int(coordinator_state.get("acquisitions", 0) or 0),
            "contentions": int(coordinator_state.get("contentions", 0) or 0),
            "nested_acquisitions": int(coordinator_state.get("nested_acquisitions", 0) or 0),
            "guard_rejections": int(coordinator_state.get("guard_rejections", 0) or 0),
            "last_guard_reason": coordinator_state.get("last_guard_reason"),
            "last_surface": coordinator_state.get("last_surface"),
            "last_duration_seconds": coordinator_state.get("last_duration_seconds"),
            "disk_reserve_bytes": coordinator_state.get("disk_reserve_bytes"),
            "wal_start_max_bytes": coordinator_state.get("wal_start_max_bytes"),
            "memory_start_limit_fraction": coordinator_state.get("memory_start_limit_fraction"),
        },
        "resource_metadata": _disk_metadata(),
        "e2e": {
            "cache": e2e_cache,
            "release_commit": e2e_payload.get("release_commit") if isinstance(e2e_payload, dict) else None,
            "all_strategy_transports_ready": bool(overall.get("all_strategy_transports_ready")),
            "all_regimes_paper_capable": bool(overall.get("all_regimes_paper_capable")),
            "all_regimes_e2e_achievable": bool(overall.get("all_regimes_e2e_achievable")),
            "all_regimes_e2e_proven": bool(overall.get("all_regimes_e2e_proven")),
            "blockers": _bounded_blockers(overall.get("blockers")),
        },
        "production_proof": {
            "cache": proof_cache,
            "release_commit": proof_release.get("release_commit"),
            "state": proof_payload.get("state") if isinstance(proof_payload, dict) else None,
            "ready_for_forward_proof": bool(proof_payload.get("ready_for_forward_proof")) if isinstance(proof_payload, dict) else False,
            "production_proof_pass": bool(proof_payload.get("production_proof_pass")) if isinstance(proof_payload, dict) else False,
            "classification": final_certification.get("classification"),
            "final_blockers": _bounded_blockers(final_certification.get("blockers")),
            "release_gate_pass": bool(release_gate.get("pass")),
            "release_gate_verdict": release_gate.get("verdict"),
            "release_gate_blockers": _bounded_blockers(release_gate.get("blockers")),
            "blockers": _bounded_blockers(proof_payload.get("blockers")) if isinstance(proof_payload, dict) else [],
        },
        "forward_certification": {
            "cache": forward_cache,
            "state": forward_payload.get("state") if isinstance(forward_payload, dict) else None,
            "system_forward_certified": bool(forward_payload.get("system_forward_certified")) if isinstance(forward_payload, dict) else False,
            "hard_operational_gates_ok": bool(forward_payload.get("hard_operational_gates_ok")) if isinstance(forward_payload, dict) else False,
            "evidence_maturity_ok": bool(forward_payload.get("evidence_maturity_ok")) if isinstance(forward_payload, dict) else False,
            "blockers": _bounded_blockers(forward_payload.get("blockers")) if isinstance(forward_payload, dict) else [],
        },
        "read_only": True,
        "deep_builders_invoked": DEEP_BUILDERS_INVOKED,
        "sqlite_opened": SQLITE_OPENED,
        "file_contents_read": FILE_CONTENTS_READ,
        "state_mutated": STATE_MUTATED,
        "strategy_thresholds_changed": False,
        "economic_thresholds_changed": False,
        "canonical_evidence_reset": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


async def _emit_certification_status_once_if_enabled() -> None:
    global _LOG_EMITTED
    if _LOG_EMITTED or not _env_true("SOLANA_ROI_CERTIFICATION_STATUS_LOG_ONCE"):
        return
    _LOG_EMITTED = True

    actual = _release_commit()
    expected = os.getenv("SOLANA_ROI_CERTIFICATION_STATUS_EXPECTED_RELEASE", "").strip()
    if not expected or expected != actual:
        _LOGGER.warning(
            "SOLANA_ROI_CERTIFICATION_STATUS %s",
            json.dumps(
                {
                    "diagnostic": "certification_status_private_log",
                    "diagnostic_version": DIAGNOSTIC_VERSION,
                    "status": "refused_release_mismatch",
                    "expected_release": expected or None,
                    "actual_release": actual,
                    "read_only": True,
                    "deep_builders_invoked": False,
                    "sqlite_opened": False,
                    "state_mutated": False,
                    "paper_only": True,
                    "live_money_authority": False,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        return

    deadline = time.monotonic() + max(0.0, float(MAX_WAIT_SECONDS))
    payload: dict[str, Any] | None = None
    while True:
        try:
            payload = await asyncio.to_thread(_collect_status)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            payload = {
                "diagnostic": "certification_status_private_log",
                "diagnostic_version": DIAGNOSTIC_VERSION,
                "release_commit": actual,
                "status": "collection_error",
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:300],
                "read_only": True,
                "deep_builders_invoked": False,
                "sqlite_opened": False,
                "state_mutated": False,
                "paper_only": True,
                "live_money_authority": False,
            }
        if bool(payload.get("all_publishers_have_minimum_successes")):
            payload["status"] = "observed_multiple_generations"
            break
        if time.monotonic() >= deadline:
            payload["status"] = "observation_timeout_fail_closed"
            break
        await asyncio.sleep(max(0.01, float(POLL_SECONDS)))

    payload["expected_release"] = expected
    payload["expected_release_matched"] = True
    _LOGGER.warning(
        "SOLANA_ROI_CERTIFICATION_STATUS %s",
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
    )


def install_certification_status_log_diagnostic(app: Any) -> None:
    """Wrap the existing lifespan with a disabled-by-default private log observer."""
    global _INSTALLED
    if _INSTALLED or bool(getattr(app.state, "roi_certification_status_log_diagnostic", False)):
        return
    original_lifespan: Callable[..., Any] = app.router.lifespan_context

    @asynccontextmanager
    async def _diagnostic_lifespan(inner_app: Any):
        async with original_lifespan(inner_app):
            task = asyncio.create_task(
                _emit_certification_status_once_if_enabled(),
                name="private-certification-status-diagnostic",
            )
            try:
                yield
            finally:
                if not task.done():
                    task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    app.router.lifespan_context = _diagnostic_lifespan
    app.state.roi_certification_status_log_diagnostic = True
    app.state.roi_certification_status_log_diagnostic_version = DIAGNOSTIC_VERSION
    app.state.roi_certification_status_log_diagnostic_public_route = False
    app.state.roi_certification_status_log_diagnostic_deep_builders = False
    app.state.roi_certification_status_log_diagnostic_paper_only = True
    app.state.roi_certification_status_log_diagnostic_live_money_authority = False
    _INSTALLED = True


__all__ = [
    "DIAGNOSTIC_VERSION",
    "_collect_status",
    "_emit_certification_status_once_if_enabled",
    "install_certification_status_log_diagnostic",
]
