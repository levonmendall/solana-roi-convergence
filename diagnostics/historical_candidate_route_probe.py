from __future__ import annotations

import json
import os
import pathlib
import re
import urllib.request

BASE = os.environ.get("ROI_BASE", "https://solana-roi-convergence.onrender.com").rstrip("/")
ROOT = pathlib.Path("src")
NEEDLES = (
    "certification-db-logical-bootstrap-page",
    "semantic-candidates",
    "market-candidates",
    "robinhood/current-candidates",
    "candidate-coverage",
)


def get_json(url: str, timeout: float = 8.0):
    req = urllib.request.Request(url, headers={"User-Agent": "historical-candidate-route-probe/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}:{exc}", "_url": url}


def main() -> int:
    print("SOURCE_MATCHES_BEGIN")
    for path in sorted(ROOT.rglob("*.py")):
        try:
            text = path.read_text(errors="replace")
        except Exception:
            continue
        if not any(n in text for n in NEEDLES):
            continue
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if any(n in line for n in NEEDLES):
                lo, hi = max(0, i - 12), min(len(lines), i + 30)
                print(f"--- {path}:{i+1} ---")
                for j in range(lo, hi):
                    print(f"{j+1:05d}: {lines[j]}")
    print("SOURCE_MATCHES_END")

    spec = get_json(BASE + "/openapi.json")
    paths = (spec.get("paths") or {}) if isinstance(spec, dict) else {}
    wanted = [p for p in sorted(paths) if any(n in p for n in NEEDLES)]
    print("OPENAPI_DEFS_BEGIN")
    for p in wanted:
        print("PATH", p)
        print(json.dumps(paths[p], indent=2, sort_keys=True)[:12000])
    print("OPENAPI_DEFS_END")

    print("DIRECT_PROBES_BEGIN")
    for p in [
        "/v1/candidate-coverage",
        "/v1/market-candidates",
        "/v1/ops/semantic-candidates",
        "/v1/robinhood/current-candidate-count",
        "/v1/robinhood/current-candidates",
    ]:
        result = get_json(BASE + p)
        print("DIRECT", p, json.dumps(result, sort_keys=True, default=str)[:20000])
    print("DIRECT_PROBES_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
