from __future__ import annotations

import json
import os
import time
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "60"))


def get(path: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-live-resource-forensics/1"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def main() -> None:
    coordinator = get("/v1/operations/certification-generation-coordinator")
    composition = get("/v1/operations/production-composition")
    direct = get("/v1/direct-solana/status")
    e2e_cache = get("/v1/strategy/e2e-status/cache")
    forward_cache = get("/v1/strategy/forward-certification/cache")
    proof_cache = get("/v1/strategy/production-proof/cache")

    memory = composition.get("runtime_memory_capacity") or {}
    forensics = memory.get("cgroup_oom_forensics") or {}
    output = {
        "coordinator": coordinator,
        "memory": {
            "cgroup_memory": memory.get("cgroup_memory"),
            "memory_pressure_deferrals": memory.get("memory_pressure_deferrals"),
            "forensics": {
                "process_epoch": forensics.get("process_epoch"),
                "current_snapshot": forensics.get("current_snapshot"),
                "previous_epoch_snapshot": forensics.get("previous_epoch_snapshot"),
                "persist_writes": forensics.get("persist_writes"),
                "last_persist_error": forensics.get("last_persist_error"),
            },
        },
        "direct_solana": {
            "continuity_ok": direct.get("continuity_ok"),
            "unresolved_gap": direct.get("unresolved_gap"),
            "strategy_relevant_continuity": direct.get("strategy_relevant_continuity"),
            "full_scope_target_quorum": direct.get("full_scope_target_quorum"),
            "target_stream_fanout": direct.get("target_stream_fanout"),
        },
        "e2e_cache": e2e_cache,
        "forward_cache": forward_cache,
        "proof_cache": proof_cache,
    }
    print("LIVE_RESOURCE_FORENSICS=" + json.dumps(output, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()
