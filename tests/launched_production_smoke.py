from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


AUTHORITY_ID = "roi-convergence-v5.1-consolidated-proof-1"
STRATEGY_VERSION = "roi-convergence-v5.1-context-exactness-1"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _get_json(url: str, *, deadline: float) -> dict:
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status != 200:
                    raise RuntimeError(f"unexpected HTTP status {response.status} from {url}")
                payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, dict):
                    raise RuntimeError(f"non-object JSON from {url}")
                return payload
        except (urllib.error.URLError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {url}: {last_error}")


def main() -> int:
    port = _free_port()
    with tempfile.TemporaryDirectory(prefix="roi-launched-production-") as tmp:
        env = os.environ.copy()
        env.update(
            {
                "PAPER_ONLY": "true",
                "SOLANA_NETWORK": "mainnet-beta",
                "SOLANA_ROI_DB_PATH": str(Path(tmp) / "smoke.sqlite3"),
                "SOLANA_ROI_DIRECT_SOLANA_ENABLED": "false",
                "SOLANA_ROI_SHADOW_CLOCK_ENABLED": "false",
                "SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE": "false",
                "ROBINHOOD_CHAIN_ENABLED": "false",
                "SOLANA_ROI_RELEASE_COMMIT": env.get("GITHUB_SHA", "f" * 40),
            }
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "solana_roi.production:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            deadline = time.monotonic() + 20.0
            health = _get_json(f"http://127.0.0.1:{port}/health", deadline=deadline)
            authority = _get_json(
                f"http://127.0.0.1:{port}/v1/strategy/authority", deadline=deadline
            )

            assert health["status"] == "ok"
            assert health["paper_only"] is True
            assert health["live_money_authority"] is False

            assert authority["authority_id"] == AUTHORITY_ID
            assert authority["strategy_version"] == STRATEGY_VERSION
            assert authority["paper_only"] is True
            assert authority["live_money_authority"] is False
            assert authority["signing_available"] is False
            assert authority["transaction_submission_available"] is False
            assert authority["canonical"] is True

            print(
                json.dumps(
                    {
                        "launched_production": True,
                        "authority_id": authority["authority_id"],
                        "strategy_version": authority["strategy_version"],
                        "paper_only": authority["paper_only"],
                        "live_money_authority": authority["live_money_authority"],
                    },
                    sort_keys=True,
                )
            )
            return 0
        except BaseException:
            if process.poll() is not None and process.stdout is not None:
                output = process.stdout.read()
                if output:
                    print(output, file=sys.stderr)
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5.0)


if __name__ == "__main__":
    raise SystemExit(main())
