from __future__ import annotations

import json
import os
import time
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
EXPECTED_SHA = os.getenv("EXPECTED_RELEASE_COMMIT", "").strip()
SAMPLES = int(os.getenv("LIVE_MEMORY_SAMPLES", "16"))
SLEEP_SECONDS = float(os.getenv("LIVE_MEMORY_SAMPLE_SECONDS", "5"))
TIMEOUT_SECONDS = float(os.getenv("LIVE_MEMORY_HTTP_TIMEOUT_SECONDS", "20"))


def get(path: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-30df-memory-forensics-readonly/2"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {"_error": "non_object_json"}


def safe(path: str) -> dict:
    try:
        return get(path)
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}: {exc}"}


def snapshot_summary(snapshot: dict | None) -> dict:
    snap = snapshot or {}
    stat = snap.get("memory_stat") or {}
    return {
        "captured_at": snap.get("captured_at"),
        "reason": snap.get("reason"),
        "process_epoch": snap.get("process_epoch"),
        "pid": snap.get("pid"),
        "active_phases": snap.get("active_phases"),
        "memory_current_bytes": snap.get("memory_current_bytes"),
        "memory_max_bytes": snap.get("memory_max_bytes"),
        "memory_fraction": snap.get("memory_fraction"),
        "memory_headroom_bytes": snap.get("memory_headroom_bytes"),
        "process_rss_bytes": snap.get("process_rss_bytes"),
        "memory_events": snap.get("memory_events"),
        "memory_pressure": snap.get("memory_pressure"),
        "database_bytes": snap.get("database_bytes"),
        "wal_bytes": snap.get("wal_bytes"),
        "memory_stat": {key: stat.get(key) for key in (
            "anon", "file", "file_mapped", "file_dirty", "file_writeback",
            "active_anon", "inactive_anon", "active_file", "inactive_file",
            "shmem", "slab", "slab_reclaimable", "slab_unreclaimable",
            "kernel", "kernel_stack", "pagetables", "sock",
        )},
    }


def cache_summary(payload: dict) -> dict:
    return {key: payload.get(key) for key in (
        "started_at", "updated_at", "last_success_at", "attempted_at", "error",
        "consecutive_errors", "build_attempts", "build_successes",
        "snapshot_age_seconds", "snapshot_max_age_seconds", "last_build_duration_seconds",
        "refresh_interval_seconds",
    )}


def main() -> None:
    last_epoch = None
    for number in range(1, SAMPLES + 1):
        health = safe("/health")
        composition = safe("/v1/operations/production-composition")
        coordinator = safe("/v1/operations/certification-generation-coordinator")
        repair = safe("/v1/operations/certification-proof-memory-repair")
        e2e_cache = safe("/v1/strategy/e2e-status/cache")
        forward_cache = safe("/v1/strategy/forward-certification/cache")
        proof_cache = safe("/v1/strategy/production-proof/cache")

        runtime_memory = composition.get("runtime_memory_capacity") or {}
        forensics = runtime_memory.get("cgroup_oom_forensics") or {}
        current = forensics.get("current_snapshot") or {}
        previous = forensics.get("previous_epoch_snapshot") or {}
        epoch = current.get("process_epoch")
        restarted = bool(last_epoch is not None and epoch and epoch != last_epoch)
        if epoch:
            last_epoch = epoch

        output = {
            "sample": number,
            "expected_sha": EXPECTED_SHA,
            "health": health,
            "composition_error": composition.get("_error"),
            "restarted_since_previous_sample": restarted,
            "current": snapshot_summary(current),
            "previous": snapshot_summary(previous),
            "coordinator": {key: coordinator.get(key) for key in (
                "acquisitions", "active", "guard_rejections", "last_guard_reason", "last_surface"
            )},
            "repair": {key: repair.get(key) for key in (
                "repair_version", "bootstrap_memory_repair_version", "proof_generations",
                "bounded_record_builds", "fomo_outcome_rows_read", "promotion_cache_hits",
                "promotion_cache_misses", "bounded_bootstrap_calls", "bounded_bootstrap_samples",
                "bounded_bootstrap_max_source_values", "bootstrap_materialized_resample_draw_lists",
                "process_thread_count", "python_thread_count", "resource_guard_relaxed",
                "stale_gate_relaxed", "continuity_gate_relaxed", "economic_thresholds_changed",
                "canonical_evidence_reset", "paper_only", "live_money_authority"
            )},
            "runtime_context": {key: runtime_memory.get(key) for key in (
                "active_worker_tasks", "peak_worker_tasks", "active_fetches", "peak_fetches",
                "active_candidate_prefills", "peak_candidate_prefills",
                "active_background_prefills", "peak_background_prefills",
                "worker_tasks_created", "memory_pressure_deferrals"
            )},
            "e2e_cache": cache_summary(e2e_cache),
            "forward_cache": cache_summary(forward_cache),
            "proof_cache": cache_summary(proof_cache),
        }
        print("LIVE_MEMORY_SAMPLE=" + json.dumps(output, sort_keys=True, default=str), flush=True)
        if health.get("_error"):
            raise AssertionError(f"health endpoint failed: {health['_error']}")
        if number < SAMPLES:
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
