#!/usr/bin/env python3
"""Bounded local HTTP observer for reproduction evidence.

Only loopback URLs are accepted. The observer records raw bounded response bodies so
publication/readiness fields can be interpreted later without coupling the harness
to one response schema.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_PATHS = (
    "/health",
    "/readiness",
    "/v1/system-proof",
    "/v1/operations/production-composition",
)


def _validate_base_url(base_url: str) -> None:
    parsed = urllib.parse.urlparse(base_url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("observer base URL must be loopback HTTP")


def fetch_one(base_url: str, path: str, *, timeout: float, max_body_bytes: int) -> dict[str, Any]:
    _validate_base_url(base_url)
    if not path.startswith("/"):
        raise ValueError("observer path must start with /")
    url = base_url.rstrip("/") + path
    started = time.time()
    record: dict[str, Any] = {"ts_unix": started, "path": path, "url": url}
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - loopback validated above
            body = response.read(max_body_bytes + 1)
            record["status"] = int(response.status)
            record["content_type"] = response.headers.get("content-type")
            record["body_truncated"] = len(body) > max_body_bytes
            body = body[:max_body_bytes]
            record["body"] = body.decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        body = exc.read(max_body_bytes + 1)
        record["status"] = int(exc.code)
        record["body_truncated"] = len(body) > max_body_bytes
        record["body"] = body[:max_body_bytes].decode("utf-8", "replace")
        record["error"] = f"HTTPError:{exc.code}"
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}:{exc}"
    record["duration_ms"] = round((time.time() - started) * 1000.0, 3)
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:10000")
    parser.add_argument("--path", action="append", dest="paths")
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--samples", type=int, default=90)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--max-body-bytes", type=int, default=262144)
    args = parser.parse_args()
    _validate_base_url(args.base_url)
    if args.interval <= 0 or args.samples <= 0 or args.timeout <= 0 or args.max_body_bytes <= 0:
        raise SystemExit("interval, samples, timeout and max-body-bytes must be positive")
    paths = tuple(args.paths or DEFAULT_PATHS)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a", encoding="utf-8") as handle:
        for idx in range(args.samples):
            for path in paths:
                handle.write(json.dumps(fetch_one(
                    args.base_url,
                    path,
                    timeout=args.timeout,
                    max_body_bytes=args.max_body_bytes,
                ), sort_keys=True) + "\n")
                handle.flush()
            if idx + 1 < args.samples:
                time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
