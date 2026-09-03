from __future__ import annotations

import asyncio
from typing import Any

from . import funding_provenance_repair as funding
from .direct_funding import SolanaRpcFundingCollector


FUNDING_TRANSACTION_RPC_ROUNDS = 3
FUNDING_TRANSACTION_RETRY_DELAY_SECONDS = 0.05


def _transaction_params(signature: str) -> list[Any]:
    return [
        signature,
        {
            "encoding": "jsonParsed",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        },
    ]


async def _secondary_non_null_read(
    self: SolanaRpcFundingCollector,
    signature: str,
    first_provider: str | None,
) -> dict[str, Any] | None:
    rpc = self.rpc
    ordered = list(rpc._ordered("getTransaction"))  # type: ignore[attr-defined]
    params = _transaction_params(signature)
    for endpoint in ordered:
        if first_provider and endpoint.name == first_provider:
            continue
        try:
            result, _provider, _latency = await rpc._call_endpoint(  # type: ignore[attr-defined]
                endpoint,
                "getTransaction",
                params,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            funding._increment(self, "transaction_rpc_errors")
            continue
        if isinstance(result, dict):
            funding._increment(self, "transaction_secondary_non_null_recoveries")
            return result
    return None


async def _transaction_with_freshness_retry(
    self: SolanaRpcFundingCollector,
    signature: str,
) -> dict[str, Any] | None:
    """Require a real confirmed transaction before declaring provenance unavailable.

    Public load-balanced Solana RPC can return a successful JSON-RPC response whose
    `result` is temporarily null on one backend even when another backend already
    has the confirmed transaction. The previous funding reader treated two such
    rounds as a hard provenance failure. Keep the read-only bounded behavior, but
    when the first successful endpoint returns null, explicitly consult the other
    configured endpoint before consuming another retry round.
    """

    last_error: Exception | None = None
    for attempt in range(FUNDING_TRANSACTION_RPC_ROUNDS):
        provider: str | None = None
        try:
            result, provider, _latency = await self.rpc.get_transaction(signature, hedge=True)
            if isinstance(result, dict):
                if attempt:
                    funding._increment(self, "transaction_rpc_recovered_after_retry")
                return result
            funding._increment(self, "transaction_null_results")
            secondary = await _secondary_non_null_read(self, signature, provider)
            if isinstance(secondary, dict):
                if attempt:
                    funding._increment(self, "transaction_rpc_recovered_after_retry")
                return secondary
            last_error = RuntimeError("confirmed funding transaction unavailable across configured RPC endpoints")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            funding._increment(self, "transaction_rpc_errors")

        if attempt + 1 < FUNDING_TRANSACTION_RPC_ROUNDS:
            await asyncio.sleep(FUNDING_TRANSACTION_RETRY_DELAY_SECONDS)

    if last_error is not None:
        raise last_error
    return None


def _status_with_funding_rpc(original: Any) -> Any:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        bridge = payload.get("launch_coverage_bridge")
        if isinstance(bridge, dict):
            raw = funding.bridge._raw_collectors(self)
            collector = getattr(raw, "funding", None)
            bridge.update(
                {
                    "funding_transaction_rpc_rounds": FUNDING_TRANSACTION_RPC_ROUNDS,
                    "funding_transaction_null_result_secondary_fallback": True,
                    "funding_transaction_retry_delay_seconds": FUNDING_TRANSACTION_RETRY_DELAY_SECONDS,
                    "funding_history_page_cap_unchanged": True,
                }
            )
            if collector is not None:
                bridge.update(
                    {
                        "funding_transaction_null_results": int(
                            getattr(collector, "_roi_funding_provenance_transaction_null_results", 0) or 0
                        ),
                        "funding_transaction_secondary_non_null_recoveries": int(
                            getattr(
                                collector,
                                "_roi_funding_provenance_transaction_secondary_non_null_recoveries",
                                0,
                            )
                            or 0
                        ),
                    }
                )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_funding_rpc_freshness_repair", True)
    return status


def install_funding_rpc_freshness_repair() -> None:
    funding._transaction_with_retry = _transaction_with_freshness_retry  # type: ignore[assignment]

    from .direct_solana import DirectSolanaIngestionPlane

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_funding_rpc_freshness_repair", False)):
        DirectSolanaIngestionPlane.status = _status_with_funding_rpc(current_status)  # type: ignore[method-assign]


__all__ = [
    "FUNDING_TRANSACTION_RPC_ROUNDS",
    "FUNDING_TRANSACTION_RETRY_DELAY_SECONDS",
    "install_funding_rpc_freshness_repair",
    "_transaction_with_freshness_retry",
]
