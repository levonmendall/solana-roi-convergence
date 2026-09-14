from __future__ import annotations

import pathlib

ROOT = pathlib.Path("src")
NEEDLES = (
    "CERTIFICATION_TOKEN",
    "certification_token",
    "_require_shared_token",
    "semantic_candidate_opportunities",
    "semantic_candidate_events",
    "normalized_swaps",
    "LOGICAL_BOOTSTRAP",
)


def main() -> int:
    for path in sorted(ROOT.rglob("*.py")):
        try:
            lines = path.read_text(errors="replace").splitlines()
        except Exception:
            continue
        hit_indexes = [i for i, line in enumerate(lines) if any(n in line for n in NEEDLES)]
        if not hit_indexes:
            continue
        emitted = []
        for i in hit_indexes:
            lo, hi = max(0, i - 8), min(len(lines), i + 18)
            if any(a <= lo and hi <= b for a, b in emitted):
                continue
            emitted.append((lo, hi))
            print(f"--- {path}:{i+1} ---")
            for j in range(lo, hi):
                print(f"{j+1:05d}: {lines[j]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
