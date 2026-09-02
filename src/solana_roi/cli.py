from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict

from .config import BASELINE
from .deployment import deployment_preflight
from .split_webhooks import SplitHeliusWebhookManager


def main() -> int:
    parser = argparse.ArgumentParser(prog="solana-roi")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("baseline")
    subparsers.add_parser("preflight")
    webhook = subparsers.add_parser("sync-helius-webhook")
    webhook.add_argument("--service-url", default=os.getenv("RENDER_EXTERNAL_URL", ""))
    args = parser.parse_args()

    if args.command == "baseline":
        print(json.dumps(asdict(BASELINE), indent=2, sort_keys=True))
        return 0

    if args.command == "preflight":
        status = deployment_preflight()
        print(json.dumps(status, indent=2, sort_keys=True))
        return 0 if status["ready_for_live_shadow_collection"] else 1

    if args.command == "sync-helius-webhook":
        service_url = str(args.service_url or "").strip()
        if not service_url:
            parser.error("--service-url or RENDER_EXTERNAL_URL is required")
        manager = SplitHeliusWebhookManager(
            api_key=os.getenv("HELIUS_API_KEY", "").strip(),
            auth_header=os.getenv("HELIUS_WEBHOOK_AUTH", "").strip(),
        )
        result = asyncio.run(manager.sync(service_url))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
