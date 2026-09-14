from __future__ import annotations

import json
import urllib.request

BASE = "https://solana-roi-convergence.onrender.com"


def get_json(url: str, timeout: float = 10.0):
    req = urllib.request.Request(url, headers={"User-Agent": "historical-candidate-route-probe/2"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}:{exc}", "_url": url}


def main() -> int:
    spec = get_json(BASE + "/openapi.json")
    paths = (spec.get("paths") or {}) if isinstance(spec, dict) else {}
    wanted = []
    for path, definition in sorted(paths.items()):
        low = path.lower()
        if any(k in low for k in ("candidate", "opportun", "semantic", "robinhood", "performance", "wallet-forward")):
            wanted.append((path, definition))
    print("CANDIDATE_OPENAPI_BEGIN")
    for path, definition in wanted:
        print("PATH", path)
        print(json.dumps(definition, sort_keys=True, separators=(",", ":")))
    print("CANDIDATE_OPENAPI_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
