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
        headers={"Accept": "application/json", "User-Agent": "solana-roi-memory-forensics/1"},
    )
    started = time.monotonic()
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    print(f"GET {path} elapsed={time.monotonic()-started:.2f}s", flush=True)
    return payload


def main() -> None:
    health = get("/health")
    composition = get("/v1/operations/production-composition")
    memory = composition.get("runtime_memory_capacity") or {}
    forensic = memory.get("cgroup_oom_forensics") or {}
    output = {
        "expected_sha": EXPECTED_SHA,
        "health_release_commit": health.get("release_commit"),
        "health_paper_only": health.get("paper_only"),
        "health_live_money_authority": health.get("live_money_authority"),
        "forensics": forensic,
    }
    print("LIVE_CGROUP_FORENSICS=" + json.dumps(output, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()
