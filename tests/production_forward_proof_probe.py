from __future__ import annotations

import json
import os
import time
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
EXPECTED_SHA = os.getenv("EXPECTED_RELEASE_COMMIT", "").strip()
TIMEOUT_SECONDS = float(os.getenv("FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "45"))


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


def lane_summary(proof: dict) -> dict:
    accounting = proof.get("candidate_accounting") or {}
    lanes = accounting.get("lanes") or accounting.get("lane_accounting") or {}
    result = {}
    if isinstance(lanes, dict):
        for lane, value in lanes.items():
            if isinstance(value, dict):
                result[lane] = {
                    "verified": value.get("verified"),
                    "status": value.get("status"),
                    "observed": value.get("observed_candidate_count"),
                    "terminal": value.get("terminal_candidate_count"),
                    "pending": value.get("valid_pending_candidate_count"),
                    "coverage_debt": value.get("coverage_debt_candidate_count"),
                    "unexplained": value.get("unexplained_candidate_count"),
                    "reconciled": value.get("reconciled"),
                    "proof_state": value.get("proof_state"),
                }
    return result


def sample(number: int) -> None:
    health = safe("/health")
    repair = safe("/v1/operations/certification-proof-memory-repair")
    coordinator = safe("/v1/operations/certification-generation-coordinator")
    composition = safe("/v1/operations/production-composition")
    direct = safe("/v1/direct-solana/status")
    e2e_cache = safe("/v1/strategy/e2e-status/cache")
    e2e = safe("/v1/strategy/e2e-status")
    forward_cache = safe("/v1/strategy/forward-certification/cache")
    forward = safe("/v1/strategy/forward-certification")
    proof_cache = safe("/v1/strategy/production-proof/cache")
    proof = safe("/v1/strategy/production-proof")
    robinhood = safe("/v1/robinhood-chain/status")

    memory = composition.get("runtime_memory_capacity") or {}
    forensic = memory.get("cgroup_oom_forensics") or {}
    current = forensic.get("current_snapshot") or {}
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
            "proof_generations": repair.get("proof_generations"),
            "bounded_record_builds": repair.get("bounded_record_builds"),
            "fomo_outcome_rows_read": repair.get("fomo_outcome_rows_read"),
            "promotion_cache_hits": repair.get("promotion_cache_hits"),
            "promotion_cache_misses": repair.get("promotion_cache_misses"),
            "unbounded_fomo_shadow_scan": repair.get("unbounded_fomo_shadow_scan"),
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
            "file_bytes": (current.get("memory_stat") or {}).get("file"),
            "anon_bytes": (current.get("memory_stat") or {}).get("anon"),
            "kernel_bytes": (current.get("memory_stat") or {}).get("kernel"),
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
            "target_stream_fanout": direct.get("target_stream_fanout"),
            "live_poll_redundancy": direct.get("live_poll_redundancy"),
            "continuity_epoch": direct.get("continuity_epoch"),
        },
        "e2e_cache": e2e_cache,
        "e2e": {
            "error": e2e.get("_error"),
            "release_commit": e2e.get("release_commit"),
            "overall": e2e.get("overall"),
            "solana": e2e.get("solana"),
            "fomo": e2e.get("fomo"),
            "robinhood": e2e.get("robinhood"),
        },
        "forward_cache": forward_cache,
        "forward": {
            "error": forward.get("_error"),
            "release_commit": forward.get("release_commit"),
            "state": forward.get("state"),
            "system_forward_certified": forward.get("system_forward_certified"),
            "blockers": forward.get("blockers"),
            "paper_only": forward.get("paper_only"),
            "live_money_authority": forward.get("live_money_authority"),
        },
        "proof_cache": proof_cache,
        "proof": {
            "error": proof.get("_error"),
            "release": proof.get("release"),
            "state": proof.get("state"),
            "production_proof_pass": proof.get("production_proof_pass"),
            "blockers": proof.get("blockers"),
            "lanes": lane_summary(proof),
            "final_certification": proof.get("final_certification"),
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
            time.sleep(16)


if __name__ == "__main__":
    main()
