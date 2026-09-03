from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

from .quote import JupiterQuoteOnlyClient
from .solana_rpc import SolanaRpcPool


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DirectRpcJupiterQuoteClient(JupiterQuoteOnlyClient):
    """Jupiter quote client backed by the redundant standard Solana RPC pool."""

    def __init__(
        self,
        *,
        jupiter_api_key: str,
        rpc: SolanaRpcPool,
        client: Any | None = None,
        now_fn: Callable[[], datetime] = utcnow,
        perf_fn: Callable[[], float] = time.perf_counter,
    ):
        if not jupiter_api_key:
            raise ValueError("JUPITER_API_KEY is required for quote-only execution observations")
        self.jupiter_api_key = jupiter_api_key
        self.client = client or httpx.AsyncClient(timeout=2.0)
        self.rpc = rpc
        self.now_fn = now_fn
        self.perf_fn = perf_fn
        self._sol_usd_cache: tuple[datetime, float] | None = None


# Package initialization installs exact-fill accounting first. The production
# runtime imports this direct quote adapter before constructing its handoff and
# activation gate, so install the all-in admission layer here without freezing or
# arming any cohort state. This only changes candidate economics: the existing 15%
# chase ceiling remains the authority, now applied to route price plus observed
# signature/priority/rent cash costs.
from .all_in_execution_cost_gate import install_all_in_execution_cost_gate

install_all_in_execution_cost_gate()
