from __future__ import annotations

import argparse
import json

from solana_roi.storage_inventory_diagnostic import (
    DEFAULT_ROOT,
    DEFAULT_TOP_N,
    inventory_storage,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only metadata inventory for the persistent data mount."
    )
    parser.add_argument("root", nargs="?", default=str(DEFAULT_ROOT))
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, dest="top_n")
    args = parser.parse_args()
    print(json.dumps(inventory_storage(args.root, top_n=args.top_n), sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
