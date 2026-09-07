from __future__ import annotations

import asyncio

from solana_roi import v51_paper_balance_artifact_hardening as hardening


class DummyRpc:
    def __init__(self, amount: int) -> None:
        self.amount = amount
        self.calls: list[tuple[str, object]] = []

    async def call(self, method: str, params: object) -> dict:
        self.calls.append((method, params))
        return {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "tokenAmount": {"amount": str(self.amount)}
                                }
                            }
                        }
                    }
                }
            ]
        }


class DummyDiscovery:
    def __init__(self, amount: int) -> None:
        self.rpc = DummyRpc(amount)


class DummyAdapter:
    def __init__(self, amount: int) -> None:
        self.discovery = DummyDiscovery(amount)


def _base_evidence() -> dict:
    return {
        "error": "InstructionError: InsufficientFunds",
        "simulation_error_class": "account_failure",
        "amount_match": True,
        "transaction_built": True,
        "route_valid": True,
        "expected_output_lamports": 2_000_000,
        "total_fee_lamports": 5_000,
        "token_restriction": False,
        "transfer_failure": False,
    }


def test_balance_reader_sums_exact_mint_accounts() -> None:
    adapter = DummyAdapter(7)
    observed, raw, count, error = asyncio.run(
        hardening._read_shadow_token_balance(
            adapter,
            owner="shadow-owner",
            token_mint="TOKEN",
        )
    )
    assert observed is True
    assert raw == 7
    assert count == 1
    assert error is None
    method, params = adapter.discovery.rpc.calls[0]
    assert method == "getTokenAccountsByOwner"
    assert params[0] == "shadow-owner"
    assert params[1] == {"mint": "TOKEN"}


def test_hardened_classifier_rejects_broad_string_without_mechanical_proof(monkeypatch) -> None:
    monkeypatch.setattr(hardening, "_ORIGINAL_CLASSIFIER", lambda evidence: True)
    assert hardening._mechanically_proven_paper_balance_artifact(_base_evidence()) is False


def test_hardened_classifier_requires_real_balance_shortfall(monkeypatch) -> None:
    monkeypatch.setattr(hardening, "_ORIGINAL_CLASSIFIER", lambda evidence: True)
    evidence = {
        **_base_evidence(),
        "shadow_wallet_balance_observed": True,
        "shadow_wallet_input_balance_raw": 0,
        "paper_position_exceeds_shadow_balance": True,
    }
    assert hardening._mechanically_proven_paper_balance_artifact(evidence) is True
    assert hardening._mechanically_proven_paper_balance_artifact(
        {**evidence, "paper_position_exceeds_shadow_balance": False}
    ) is False
    assert hardening._mechanically_proven_paper_balance_artifact(
        {**evidence, "shadow_wallet_balance_observed": False}
    ) is False


def test_observer_adds_independent_balance_evidence(monkeypatch) -> None:
    adapter = DummyAdapter(25)

    async def original(received, *, token_mint: str, actual_position_raw: int):
        assert received is adapter
        return {**_base_evidence(), "token_mint": token_mint, "actual_position_raw": actual_position_raw}

    monkeypatch.setattr(hardening, "_ORIGINAL_OBSERVE", original)
    monkeypatch.setenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "shadow-owner")
    result = asyncio.run(
        hardening._observe_exact_exit_order_with_balance_proof(
            adapter,
            token_mint="TOKEN",
            actual_position_raw=100,
        )
    )
    assert result["shadow_wallet_balance_observed"] is True
    assert result["shadow_wallet_input_balance_raw"] == 25
    assert result["paper_position_exceeds_shadow_balance"] is True
    assert result["paper_balance_artifact_proof_version"] == hardening.HARDENING_VERSION


def test_status_retains_zero_live_authority() -> None:
    payload = hardening.status()
    assert payload["broad_error_string_alone_sufficient"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
