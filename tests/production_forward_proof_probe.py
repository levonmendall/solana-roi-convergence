from __future__ import annotations

import json
import os
import urllib.request

BASE_URL = os.getenv("SOLANA_ROI_PRODUCTION_URL", "https://solana-roi-convergence.onrender.com").rstrip("/")


def main() -> None:
    request = urllib.request.Request(
        f"{BASE_URL}/v1/operations/production-composition",
        headers={"Accept": "application/json", "User-Agent": "solana-roi-cgroup-forensics-probe/1"},
    )
    with urllib.request.urlopen(request, timeout=15.0) as response:
        payload = json.loads(response.read().decode("utf-8"))
    memory = payload.get("runtime_memory_capacity") or {}
    forensics = memory.get("cgroup_oom_forensics") or {}
    output = {
        "cgroup_memory": memory.get("cgroup_memory"),
        "memory_pressure_deferrals": memory.get("memory_pressure_deferrals"),
        "current_snapshot": forensics.get("current_snapshot"),
        "previous_epoch_snapshot": forensics.get("previous_epoch_snapshot"),
        "process_epoch": forensics.get("process_epoch"),
        "last_persist_error": forensics.get("last_persist_error"),
    }
    print("CGROUP_FORENSICS=" + json.dumps(output, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()
