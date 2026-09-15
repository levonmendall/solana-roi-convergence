from __future__ import annotations

import argparse
import json

from solana_roi.sqlite_page_inventory import (
    DEFAULT_CACHE_KIB,
    DEFAULT_MAX_OBJECTS,
    inventory_sqlite_pages,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only per-table/index SQLite page inventory using dbstat aggregate mode."
    )
    parser.add_argument("database")
    parser.add_argument("--max-objects", type=int, default=DEFAULT_MAX_OBJECTS)
    parser.add_argument("--cache-kib", type=int, default=DEFAULT_CACHE_KIB)
    args = parser.parse_args()
    payload = inventory_sqlite_pages(
        args.database,
        max_objects=args.max_objects,
        cache_kib=args.cache_kib,
    )
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0 if payload.get("status") == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
