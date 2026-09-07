from __future__ import annotations

import os
from typing import Any, Awaitable, Callable


HARDENING_VERSION = "v51-paper-balance-artifact-hardening-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_INSTALLED = False
_ORIGINAL_OBSERVE: Callable[..., Awaitable[dict[str, Any]]] | None = None
_ORIGINAL_CLASSIFIER: Callable[[dict[str, Any]], bool] | None = None
_BALANCE_READS = 0
_BALANCE_READ_FAILURES = 0
_MECHANICALLY_PROVEN = 0


async def _read_shadow_token_balance(adapter: Any, *, owner: str, token_mint: str) -> tuple[bool, int, int, str | None]:
    """Read the real shadow wallet's raw balance for one exact SPL mint.

    This is read-only chain evidence. It exists solely to prove that a failed
    unsigned simulation is caused by the virtual PAPER position not being present
    in the real shadow wallet. It cannot sign or submit anything.
    """

    global _BALANCE_READS, _BALANCE_READ_FAILURES
    _BALANCE_READS += 1
    try:
        payload = await adapter.discovery.rpc.call(
            "getTokenAccountsByOwner",
            [
                owner,
                {"mint": token_mint},
                {"encoding": "jsonParsed", "commitment": "processed"},
            ],
        )
        rows = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise RuntimeError("shadow_token_accounts_value_unavailable")
        total = 0
        accounts = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            account = row.get("account")
            data = account.get("data") if isinstance(account, dict) else None
            parsed = data.get("parsed") if isinstance(data, dict) else None
            info = parsed.get("info") if isinstance(parsed, dict) else None
            token_amount = info.get("tokenAmount") if isinstance(info, dict) else None
            raw = token_amount.get("amount") if isinstance(token_amount, dict) else None
            if raw is None:
                continue
            total += max(0, int(raw))
            accounts += 1
        return True, total, accounts, None
    except Exception as exc:
        _BALANCE_READ_FAILURES += 1
        return False, 0, 0, f"{type(exc).__name__}:{exc}"[:500]


async def _observe_exact_exit_order_with_balance_proof(
    adapter: Any,
    *,
    token_mint: str,
    actual_position_raw: int,
) -> dict[str, Any]:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("paper_balance_artifact_hardening_not_installed")

    evidence = dict(
        await _ORIGINAL_OBSERVE(
            adapter,
            token_mint=token_mint,
            actual_position_raw=actual_position_raw,
        )
    )
    owner = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
    observed = False
    balance_raw = 0
    account_count = 0
    balance_error: str | None = None
    if owner:
        observed, balance_raw, account_count, balance_error = await _read_shadow_token_balance(
            adapter,
            owner=owner,
            token_mint=token_mint,
        )
    else:
        balance_error = "shadow_wallet_public_key_unavailable"

    held_raw = max(0, int(actual_position_raw))
    evidence["shadow_wallet_balance_observed"] = observed
    evidence["shadow_wallet_input_balance_raw"] = balance_raw if observed else None
    evidence["shadow_wallet_input_account_count"] = account_count if observed else None
    evidence["shadow_wallet_balance_error"] = balance_error
    evidence["paper_position_exceeds_shadow_balance"] = bool(observed and held_raw > balance_raw)
    evidence["paper_balance_artifact_proof_version"] = HARDENING_VERSION
    return evidence


def _mechanically_proven_paper_balance_artifact(evidence: dict[str, Any]) -> bool:
    global _MECHANICALLY_PROVEN
    if _ORIGINAL_CLASSIFIER is None:
        return False
    base = bool(_ORIGINAL_CLASSIFIER(evidence))
    proven = bool(
        base
        and evidence.get("shadow_wallet_balance_observed") is True
        and evidence.get("paper_position_exceeds_shadow_balance") is True
        and evidence.get("shadow_wallet_input_balance_raw") is not None
    )
    if proven:
        _MECHANICALLY_PROVEN += 1
    return proven


def install_paper_balance_artifact_hardening() -> None:
    """Require independent on-chain balance proof before paper artifact override."""

    global _INSTALLED, _ORIGINAL_OBSERVE, _ORIGINAL_CLASSIFIER
    if _INSTALLED:
        return
    from . import v51_exact_exit_execution as exact
    from . import v51_paper_lifecycle_runtime as lifecycle

    if not bool(getattr(exact, "_INSTALLED", False)):
        raise RuntimeError("canonical_exact_exit_engine_must_be_installed_first")
    if not bool(getattr(lifecycle, "_INSTALLED", False)):
        raise RuntimeError("canonical_paper_lifecycle_must_be_installed_first")

    _ORIGINAL_OBSERVE = exact.observe_exact_exit_order
    _ORIGINAL_CLASSIFIER = lifecycle._proven_paper_balance_artifact
    exact.observe_exact_exit_order = _observe_exact_exit_order_with_balance_proof  # type: ignore[assignment]
    lifecycle._proven_paper_balance_artifact = _mechanically_proven_paper_balance_artifact  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": HARDENING_VERSION,
        "installed": _INSTALLED,
        "balance_reads": _BALANCE_READS,
        "balance_read_failures": _BALANCE_READ_FAILURES,
        "mechanically_proven_artifacts": _MECHANICALLY_PROVEN,
        "requires_observed_exact_mint_balance": True,
        "requires_virtual_position_exceed_real_balance": True,
        "broad_error_string_alone_sufficient": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "HARDENING_VERSION",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "install_paper_balance_artifact_hardening",
    "status",
]
