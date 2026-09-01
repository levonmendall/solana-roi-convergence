from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from .config import BASELINE


def main() -> int:
    parser = argparse.ArgumentParser(prog="solana-roi")
    parser.add_argument("command", choices=["baseline"])
    args = parser.parse_args()
    if args.command == "baseline":
        print(json.dumps(asdict(BASELINE), indent=2, sort_keys=True))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
