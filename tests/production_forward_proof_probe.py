from __future__ import annotations

import json
import os
import time
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
EXPECTED_SHA = os.getenv("EXPECTED_RELEASE_COMMIT", "").strip()
TIMEOUT_SECONDS = float(os.getenv("FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "60"))


def get(path: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-live-certification-sampler/3"},
    )
    started = time.monotonic()
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    print(f"GET {path} elapsed={time.monotonic()-started:.2f}s", flush=True)
    return payload


def safe(path: str) -> dict:
    try:
        return get(path)
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}: {exc}"}


def compact_cache(payload: dict) -> dict:
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    return {
        "error": payload.get("_error"),
        "successes": payload.get("successes"),
        "failures": payload.get("failures"),
        "guard_rejections": payload.get("guard_rejections"),
        "last_error": payload.get("last_error"),
        "snapshot_age_seconds": payload.get("snapshot_age_seconds"),
        "stale_after_seconds": payload.get("stale_after_seconds"),
        "fresh": payload.get("fresh"),
        "snapshot_release_commit": snapshot.get("release_commit") or payload.get("release_commit"),
    }


def sample(number: int) -> None:
    health = safe("/health")
    repair = safe("/v1/operations/certification-proof-memory-repair")
    coordinator = safe("/v1/operations/certification-generation-coordinator")
    composition = safe("/v1/operations/production-composition")
    direct = safe("/v1/direct-solana/status")
    e2e_cache = safe("/v1/strategy/e2e-status/cache")
    forward_cache = safe("/v1/strategy/forward-certification/cache")
    proof_cache = safe("/v1/strategy/production-proof/cache")
    proof = safe("/v1/strategy/production-proof")
    robinhood = safe("/v1/robinhood-chain/status")

    memory = composition.get("runtime_memory_capacity") or {}
    forensic = memory.get("cgroup_oom_forensics") or {}
    current = forensic.get("current_snapshot") or {}
    memory_stat = current.get("memory_stat") or {}
    output = {
        "sample": number,
        "expected_sha": EXPECTED_SHA,
        "health": {
            "error": health.get("_error"),
            "release_commit": health.get("release_commit"),
            "paper_only": health.get("paper_only"),
            "live_money_authority": health.get("live_money_authority"),
        },
        "repair": {
            "error": repair.get("_error"),
            "installed": repair.get("installed"),
            "repair_version": repair.get("repair_version"),
            "bootstrap_memory_repair_version": repair.get("bootstrap_memory_repair_version"),
            "proof_generations": repair.get("proof_generations"),
            "bounded_record_builds": repair.get("bounded_record_builds"),
            "bounded_bootstrap_calls": repair.get("bounded_bootstrap_calls"),
            "bounded_bootstrap_samples": repair.get("bounded_bootstrap_samples"),
            "bounded_bootstrap_max_source_values": repair.get("bounded_bootstrap_max_source_values"),
            "bootstrap_materialized_resample_draw_lists": repair.get("bootstrap_materialized_resample_draw_lists"),
            "process_thread_count": repair.get("process_thread_count"),
            "python_thread_count": repair.get("python_thread_count"),
            "promotion_cache_hits": repair.get("promotion_cache_hits"),
            "promotion_cache_misses": repair.get("promotion_cache_misses"),
            "resource_guard_relaxed": repair.get("resource_guard_relaxed"),
            "stale_gate_relaxed": repair.get("stale_gate_relaxed"),
            "continuity_gate_relaxed": repair.get("continuity_gate_relaxed"),
            "economic_thresholds_changed": repair.get("economic_thresholds_changed"),
            "canonical_evidence_reset": repair.get("canonical_evidence_reset"),
            "paper_only": repair.get("paper_only"),
            "live_money_authority": repair.get("live_money_authority"),
            "signing_available": repair.get("signing_available"),
            "transaction_submission_available": repair.get("transaction_submission_available"),
        },
        "coordinator": {
            "error": coordinator.get("_error"),
            "acquisitions": coordinator.get("acquisitions"),
            "guard_rejections": coordinator.get("guard_rejections"),
            "last_guard_reason": coordinator.get("last_guard_reason"),
            "last_surface": coordinator.get("last_surface"),
            "active": coordinator.get("active"),
        },
        "memory": {
            "error": composition.get("_error"),
            "memory_current_bytes": current.get("memory_current_bytes"),
            "memory_max_bytes": current.get("memory_max_bytes"),
            "memory_fraction": current.get("memory_fraction"),
            "memory_headroom_bytes": current.get("memory_headroom_bytes"),
            "process_rss_bytes": current.get("process_rss_bytes"),
            "file_bytes": memory_stat.get("file"),
            "file_dirty_bytes": memory_stat.get("file_dirty"),
            "file_writeback_bytes": memory_stat.get("file_writeback"),
            "inactive_file_bytes": memory_stat.get("inactive_file"),
            "active_file_bytes": memory_stat.get("active_file"),
            "anon_bytes": memory_stat.get("anon"),
            "kernel_bytes": memory_stat.get("kernel"),
            "slab_bytes": memory_stat.get("slab"),
            "pagetables_bytes": memory_stat.get("pagetables"),
            "memory_events": current.get("memory_events"),
            "active_phases": current.get("active_phases"),
            "database_bytes": current.get("database_bytes"),
            "wal_bytes": current.get("wal_bytes"),
            "process_epoch": current.get("process_epoch"),
        },
        "direct_solana": {
            "error": direct.get("_error"),
            "enabled": direct.get("enabled"),
            "continuity_ok": direct.get("continuity_ok"),
            "unresolved_gap": direct.get("unresolved_gap"),
            "strategy_relevant_continuity": direct.get("strategy_relevant_continuity"),
            "full_scope_target_quorum": direct.get("full_scope_target_quorum"),
            "continuity_epoch": direct.get("continuity_epoch"),
            "status": direct.get("status"),
            "blockers": direct.get("blockers"),
        },
        "e2e_cache": compact_cache(e2e_cache),
        "forward_cache": compact_cache(forward_cache),
        "proof_cache": compact_cache(proof_cache),
        "proof": {
            "error": proof.get("_error"),
            "release_commit": (proof.get("release") or {}).get("release_commit"),
            "state": proof.get("state"),
            "production_proof_pass": proof.get("production_proof_pass"),
            "blockers": proof.get("blockers"),
            "paper_only": proof.get("paper_only"),
            "live_money_authority": proof.get("live_money_authority"),
            "signing_available": proof.get("signing_available"),
            "transaction_submission_available": proof.get("transaction_submission_available"),
        },
        "robinhood": {
            "error": robinhood.get("_error"),
            "status": robinhood.get("status"),
            "blockers": robinhood.get("blockers"),
            "all_regimes_e2e_achievable": robinhood.get("all_regimes_e2e_achievable"),
            "paper_only": robinhood.get("paper_only"),
            "live_money_authority": robinhood.get("live_money_authority"),
            "worker": robinhood.get("worker"),
            "transport": robinhood.get("transport"),
            "catchup": robinhood.get("catchup"),
        },
    }
    print("LIVE_CERT_SAMPLE=" + json.dumps(output, sort_keys=True, default=str), flush=True)


def main() -> None:
    for number in range(1, 4):
        sample(number)
        if number < 3:
            time.sleep(20)


if __name__ == "__main__":
    main()
