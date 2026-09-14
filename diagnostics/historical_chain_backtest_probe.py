from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("ROI_BASE", "https://solana-roi-convergence.onrender.com").rstrip("/")


def get_json(url: str, *, timeout: float = 30.0):
    req = urllib.request.Request(url, headers={"User-Agent": "solana-roi-historical-backtest/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}:{exc}", "_url": url}


def rpc(url: str, method: str, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "User-Agent": "solana-roi-historical-backtest/1"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        return {"_error": f"{type(exc).__name__}:{exc}", "method": method}


def main() -> int:
    out = {}
    openapi = get_json(f"{BASE}/openapi.json")
    out["openapi_error"] = openapi.get("_error") if isinstance(openapi, dict) else None
    paths = sorted((openapi.get("paths") or {}).keys()) if isinstance(openapi, dict) else []
    interesting = [p for p in paths if any(k in p.lower() for k in ("candidate", "opportun", "trial", "outcome", "wallet", "performance", "robinhood", "strategy/v52", "semantic"))]
    out["interesting_paths"] = interesting
    out["path_methods"] = {p: sorted((openapi.get("paths", {}).get(p) or {}).keys()) for p in interesting}

    probes = [
        "/health",
        "/v1/ingestion/status",
        "/v1/strategy/v52",
        "/v1/strategy/v52/performance/24h",
        "/v1/strategy/v52/performance/7d",
        "/v1/strategy/v52/wallet-forward-alpha",
        "/v1/wallet-discovery/status",
        "/v1/wallet-intelligence/status",
    ]
    out["probes"] = {p: get_json(BASE + p) for p in probes}

    out["solana_rpc"] = {
        "slot": rpc("https://api.mainnet-beta.solana.com", "getSlot", [{"commitment": "finalized"}]),
        "pump_recent": rpc("https://api.mainnet-beta.solana.com", "getSignaturesForAddress", ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", {"limit": 10, "commitment": "finalized"}]),
        "pumpswap_recent": rpc("https://api.mainnet-beta.solana.com", "getSignaturesForAddress", ["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", {"limit": 10, "commitment": "finalized"}]),
        "raydium_cpmm_recent": rpc("https://api.mainnet-beta.solana.com", "getSignaturesForAddress", ["CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", {"limit": 10, "commitment": "finalized"}]),
    }
    out["robinhood_rpc"] = {
        "chain_id": rpc("https://rpc.mainnet.chain.robinhood.com", "eth_chainId", []),
        "latest_block": rpc("https://rpc.mainnet.chain.robinhood.com", "eth_blockNumber", []),
    }

    print("HISTORICAL_CHAIN_BACKTEST_PROBE_BEGIN")
    print(json.dumps(out, sort_keys=True, indent=2, default=str))
    print("HISTORICAL_CHAIN_BACKTEST_PROBE_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
