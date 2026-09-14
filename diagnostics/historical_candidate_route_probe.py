from __future__ import annotations

import pathlib
import re

ROOTS = [pathlib.Path("src"), pathlib.Path("tests")]
NEEDLES = (
    "CERTIFICATION_LOGICAL_BOOTSTRAP_ALLOWLIST",
    "semantic_candidate_opportunities",
    "semantic_candidate_events",
    "normalized_swaps",
    "paper_trial",
    "paper_outcome",
    "replica.sqlite3",
    "certification-db-logical-bootstrap-page",
)
ROUTE_MARKERS = ("@router.get", "@app.get", "add_api_route")


def blocks(path: pathlib.Path, lines: list[str]) -> list[tuple[int, int]]:
    hits = [i for i, line in enumerate(lines) if any(n.lower() in line.lower() for n in NEEDLES)]
    spans: list[tuple[int, int]] = []
    for i in hits:
        lo, hi = max(0, i - 8), min(len(lines), i + 20)
        # If this looks like a constant/container declaration, continue until closing delimiter.
        if "ALLOWLIST" in lines[i] or re.search(r"\b(semantic_candidate|normalized_swaps|paper_(trial|outcome))", lines[i], re.I):
            hi = min(len(lines), i + 60)
        if spans and lo <= spans[-1][1]:
            spans[-1] = (spans[-1][0], max(spans[-1][1], hi))
        else:
            spans.append((lo, hi))
    return spans


def main() -> int:
    print("TARGETED_REPLAY_TRACE_BEGIN")
    for root in ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                lines = path.read_text(errors="replace").splitlines()
            except Exception:
                continue
            spans = blocks(path, lines)
            if not spans:
                continue
            for lo, hi in spans:
                print(f"--- {path}:{lo+1}-{hi} ---")
                for j in range(lo, hi):
                    print(f"{j+1:05d}: {lines[j]}")
    print("TARGETED_REPLAY_TRACE_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
