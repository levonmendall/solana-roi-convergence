from __future__ import annotations

import json
import os
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")
TIMEOUT_SECONDS = 20.0


def get(path: str) -> dict:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-live-coordinator-probe/1"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def main() -> None:
    output = {
        "coordinator": get("/v1/operations/certification-generation-coordinator"),
        "e2e_cache": get("/v1/strategy/e2e-status/cache"),
        "forward_cache": get("/v1/strategy/forward-certification/cache"),
        "proof_cache": get("/v1/strategy/production-proof/cache"),
    }
    print("LIVE_COORDINATOR=" + json.dumps(output, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()
