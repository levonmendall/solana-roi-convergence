from __future__ import annotations

import json
import urllib.request

BASE = "https://solana-roi-convergence.onrender.com"


def get_json(path: str, timeout: float = 15.0):
    url = BASE + path
    req = urllib.request.Request(url, headers={"User-Agent": "historical-candidate-route-probe/3"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}:{exc}", "_url": url}


def main() -> int:
    for path in (
        "/v1/strategy/candidate-coverage",
        "/v1/robinhood-chain/status",
    ):
        result = get_json(path)
        print("RESULT_BEGIN", path)
        print(json.dumps(result, sort_keys=True, indent=2, default=str))
        print("RESULT_END", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
