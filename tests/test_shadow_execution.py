from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.quote import ExecutableQuote
from solana_roi.shadow_execution import (
    JupiterShadowTransactionSimulator,
    ShadowExecutionCertificationGate,
    ShadowExecutionLedger,
    validate_solana_public_key,
)


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "transaction": base64.b64encode(b"unsigned-versioned-transaction").decode(),
            "outAmount": "20000000",
            "router": "iris",
            "signatureFeeLamports": 5000,
            "prioritizationFeeLamports": 250000,
            "rentFeeLamports": 2039280,
            "lastValidBlockHeight": 123,
        }


class FakeHttp:
    async def get(self, *args, **kwargs):
        assert kwargs["params"]["taker"] == "11111111111111111111111111111111"
        return FakeResponse()


class FakeRpc:
    async def call(self, method, params):
        assert method == "simulateTransaction"
        assert params[1]["sigVerify"] is False
        assert params[1]["replaceRecentBlockhash"] is True
        return {
            "context": {"slot": 999},
            "value": {"err": None, "logs": ["ok"], "unitsConsumed": 123456},
        }


def _quote(now: datetime) -> ExecutableQuote:
    return ExecutableQuote(
        token_mint="mint",
        stage="starter",
        requested_notional_usd=3.75,
        input_sol=.02,
        sol_usd=187.5,
        output_token_units=20.0,
        effective_price_sol=.001,
        scout_reference_price_sol=.001,
        drift_fraction=0.0,
        router="iris",
        fee_bps=0,
        token_decimals=6,
        quoted_at=now,
        received_at=now,
        quote_latency_ms=100,
        chain_to_quote_ms=500,
        usable=True,
        reason="within chase ceiling",
    )


def test_shadow_wallet_public_key_is_identity_only_and_unsigned_simulation_succeeds(tmp_path):
    wallet = validate_solana_public_key("11111111111111111111111111111111")
    assert wallet == "11111111111111111111111111111111"
    simulator = JupiterShadowTransactionSimulator(
        jupiter_api_key="test",
        shadow_wallet_public_key=wallet,
        http_client=FakeHttp(),
        rpc=FakeRpc(),
    )

    result = asyncio.run(simulator.observe(_quote(datetime(2026, 9, 1, tzinfo=timezone.utc))))
    assert result.transaction_built is True
    assert result.simulation_ok is True
    assert result.units_consumed == 123456
    assert result.signature_fee_lamports == 5000
    assert result.rent_fee_lamports == 2039280
    assert result.transaction_sha256

    store = ObservationEventStore(tmp_path / "shadow.sqlite3")
    ledger = ShadowExecutionLedger(store)
    ledger.record(result)
    gate = ShadowExecutionCertificationGate(
        ledger,
        shadow_wallet_public_key=wallet,
    )
    status = gate.status()
    assert status["configured"] is True
    assert status["private_key_access"] is False
    assert status["signing_available"] is False
    assert status["submission_available"] is False
    assert status["certified"] is False  # one sample cannot satisfy the prospective 100-sample gate


def test_invalid_shadow_wallet_fails_validation():
    with pytest.raises(ValueError):
        validate_solana_public_key("not-a-solana-key")
