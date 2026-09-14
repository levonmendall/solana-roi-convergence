from __future__ import annotations

import json
import urllib.request
from collections import Counter
from typing import Any

BASE = "https://solana-roi-convergence.onrender.com"
INTEREST = (
    "candidate", "opportun", "mint", "token", "signature", "observed", "received",
    "created", "updated", "venue", "lane", "wallet", "pool", "chain", "lifecycle",
    "program", "market", "address", "id", "status", "risk", "eligible", "decision",
)


def get_json(path: str, timeout: float = 45.0):
    url = BASE + path
    req = urllib.request.Request(url, headers={"User-Agent": "historical-candidate-route-probe/4"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}:{exc}", "_url": url}


def scalar(v: Any) -> Any:
    if isinstance(v, str):
        return v if len(v) <= 180 else v[:177] + "..."
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    return None


def compact_record(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        lk = str(k).lower()
        if any(term in lk for term in INTEREST):
            sv = scalar(v)
            if sv is not None:
                out[str(k)] = sv
    return out


def walk(obj: Any, records: list[dict[str, Any]], shapes: Counter[str], path: str = "$", depth: int = 0) -> None:
    if depth > 10:
        return
    if isinstance(obj, dict):
        shapes["dict"] += 1
        c = compact_record(obj)
        strong = sum(any(t in k.lower() for t in ("candidate", "opportun", "mint", "token", "pool", "signature", "wallet")) for k in c)
        timed = any(any(t in k.lower() for t in ("observed", "received", "created", "updated")) for k in c)
        if c and (strong >= 2 or (strong >= 1 and timed)) and len(records) < 80:
            records.append({"_path": path, **c})
        for k, v in obj.items():
            walk(v, records, shapes, f"{path}.{k}", depth + 1)
    elif isinstance(obj, list):
        shapes["list"] += 1
        shapes[f"list_len:{min(len(obj), 1000)}"] += 1
        for i, v in enumerate(obj[:2000]):
            walk(v, records, shapes, f"{path}[{i}]", depth + 1)
    else:
        shapes[type(obj).__name__] += 1


def summarize(path: str, obj: Any) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    shapes: Counter[str] = Counter()
    walk(obj, records, shapes)
    top = list(obj.keys()) if isinstance(obj, dict) else None
    return {
        "path": path,
        "error": obj.get("_error") if isinstance(obj, dict) else None,
        "top_level_keys": top,
        "json_bytes": len(json.dumps(obj, default=str, separators=(",", ":")).encode()),
        "shapes": dict(shapes.most_common(20)),
        "candidate_like_count_captured": len(records),
        "candidate_like_records": records,
    }


def main() -> int:
    output = []
    for path in ("/v1/strategy/candidate-coverage", "/v1/robinhood-chain/status"):
        output.append(summarize(path, get_json(path)))
    print("COMPACT_CANDIDATE_COVERAGE_BEGIN")
    print(json.dumps(output, sort_keys=True, indent=2, default=str))
    print("COMPACT_CANDIDATE_COVERAGE_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
