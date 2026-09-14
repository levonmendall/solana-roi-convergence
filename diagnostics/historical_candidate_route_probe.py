from __future__ import annotations

import pathlib

ROOTS = [pathlib.Path("src"), pathlib.Path("tests")]
NEEDLES = (
    "semantic_candidate_opportunities",
    "semantic_candidate_events",
    "normalized_swaps",
    "paper_trials",
    "paper_outcomes",
    "logical_bootstrap",
    "certification-db-logical-bootstrap",
    "replica.sqlite3",
    "adaptive_wallet_cohorts",
    "anonymous_candidate_latency_failures",
    "REPLICATED",
    "replication_manifest",
    "bootstrap_tables",
    "table_manifest",
)


def emit(path: pathlib.Path) -> None:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except Exception:
        return
    hits = [i for i, line in enumerate(lines) if any(n.lower() in line.lower() for n in NEEDLES)]
    if not hits:
        return
    merged: list[tuple[int, int]] = []
    for i in hits:
        lo, hi = max(0, i - 14), min(len(lines), i + 32)
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    for lo, hi in merged:
        print(f"--- {path}:{lo+1}-{hi} ---")
        for j in range(lo, hi):
            print(f"{j+1:05d}: {lines[j]}")


def main() -> int:
    print("MANIFEST_TRACE_BEGIN")
    for root in ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            emit(path)
    print("MANIFEST_TRACE_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
