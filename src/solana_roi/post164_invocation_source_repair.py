from __future__ import annotations

import json
from collections import Counter
from typing import Any, Callable

from . import direct_transaction as tx
from . import post161_candidate_attribution_repair as post161
from . import venue_native_candidate_graph_repair as venue
from .direct_solana import DirectSolanaIngestionPlane


REPAIR_VERSION = "candidate-invocation-source-authority-v1"
SOURCE_AUTHORITY = "executed_supported_program_invocation_only"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None
_ORIGINAL_DIAGNOSTIC_FACTS: Callable[..., dict[str, Any]] | None = None


def _invoked_program_counts(result: Any) -> Counter[str]:
    """Return actual top-level/inner invoked programs, resolving v0 account indexes."""
    counts: Counter[str] = Counter()
    if not isinstance(result, dict):
        return counts
    keys = venue._account_keys(result)
    for _parent, row in venue._walk_instruction_rows(result):
        program_id = str(venue._instruction_program_id(row, keys) or "")
        if program_id:
            counts[program_id] += 1
    return counts


def _invoked_supported_sources(result: Any) -> set[str]:
    sources: set[str] = set()
    for program_id in _invoked_program_counts(result):
        source = venue._PROGRAM_SOURCE_BY_ID.get(program_id)
        if source:
            sources.add(str(source))
    return sources


def _indexed_transaction_sources_invocation_only(result: dict[str, Any]) -> set[str]:
    """Candidate venue proof comes from execution, never mere account-key presence."""
    return _invoked_supported_sources(result)


def _source_for_transaction_invocation_only(
    result: dict[str, Any], source_hint: str | None
) -> tuple[str | None, str | None]:
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"

    sources = _invoked_supported_sources(result)
    hint = str(source_hint or "").upper()
    if hint:
        if hint not in venue._PROGRAM_IDS_BY_SOURCE:
            return None, "unsupported_source_hint"
        if hint not in sources:
            return None, "source_hint_not_present"
        return hint, None
    if len(sources) == 1:
        return next(iter(sources)), None
    if not sources:
        return None, "supported_swap_source_missing"
    return None, "multiple_supported_swap_sources"


def _diagnostic_facts_with_invocation_authority(
    result: Any, *, wallet: str, source_hint: str | None, reason: str
) -> dict[str, Any]:
    if _ORIGINAL_DIAGNOSTIC_FACTS is None:
        raise RuntimeError("candidate invocation-source diagnostic wrapper not installed")

    facts = _ORIGINAL_DIAGNOSTIC_FACTS(
        result,
        wallet=wallet,
        source_hint=source_hint,
        reason=reason,
    )
    invoked_counts = _invoked_program_counts(result)
    invoked_sources = {
        str(venue._PROGRAM_SOURCE_BY_ID[program_id])
        for program_id in invoked_counts
        if program_id in venue._PROGRAM_SOURCE_BY_ID
    }
    # Legacy transaction_sources intentionally remains untouched because launch and
    # generic observation consumers may use account presence as context. Candidate
    # authority is narrower and uses actual invocation proof only.
    legacy_sources = tx.transaction_sources(result) if isinstance(result, dict) else set()
    account_key_only_sources = set(legacy_sources) - invoked_sources

    facts["candidate_source_authority"] = SOURCE_AUTHORITY
    facts["invoked_supported_sources"] = sorted(invoked_sources)
    facts["account_key_only_supported_sources"] = sorted(account_key_only_sources)
    facts["all_invoked_program_count"] = int(sum(invoked_counts.values()))
    facts["all_invoked_program_ids_sample"] = sorted(invoked_counts)[:16]
    facts["all_invoked_program_counts_sample"] = {
        program_id: int(invoked_counts[program_id])
        for program_id in sorted(invoked_counts)[:16]
    }

    # Keep the bounded shape useful across failures without persisting raw account
    # lists or transaction payloads. Only canonical source labels and counts are
    # added to the aggregation key.
    try:
        shape = json.loads(str(facts.get("shape") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        shape = {}
    if not isinstance(shape, dict):
        shape = {}
    shape["invoked_sources"] = sorted(invoked_sources)
    shape["account_key_only_sources"] = sorted(account_key_only_sources)
    shape["invoked_program_count"] = int(sum(invoked_counts.values()))
    facts["shape"] = json.dumps(shape, sort_keys=True, separators=(",", ":"))
    return facts


setattr(_diagnostic_facts_with_invocation_authority, "_roi_invocation_source_authority", True)


def _status_with_invocation_source_authority(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["candidate_invocation_source_repair"] = {
            "installed": True,
            "version": REPAIR_VERSION,
            "candidate_source_authority": SOURCE_AUTHORITY,
            "account_key_presence_has_candidate_source_authority": False,
            "top_level_program_invocation_supported": True,
            "inner_program_invocation_supported": True,
            "program_id_index_resolution_supported": True,
            "loaded_address_program_resolution_supported": True,
            "source_hint_requires_matching_invocation": True,
            "global_transaction_source_context_semantics_changed": False,
            "candidate_processing_target_seconds_unchanged": 5.0,
            "candidate_entry_window_seconds_unchanged": 20.0,
            "max_chase_fraction_unchanged": 0.15,
            "strategy_thresholds_changed": False,
            "certification_thresholds_changed": False,
            "full_market_scope_reduced": False,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "candidate_venue_source_requires_executed_program_invocation": True,
                    "candidate_account_key_presence_source_authority": False,
                    "candidate_source_program_id_index_resolution_preserved": True,
                    "candidate_source_loaded_address_resolution_preserved": True,
                    "candidate_thresholds_unchanged": True,
                    "full_raw_market_scope_preserved": True,
                    "paper_only_authority_unchanged": True,
                    "signing_or_submission_available": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_invocation_source_authority", True)
    return status


def install_post164_invocation_source_repair() -> None:
    """Align candidate source classification with the venue graph's proof model."""
    global _ORIGINAL_STATUS, _ORIGINAL_DIAGNOSTIC_FACTS

    # Do not change direct_transaction.transaction_sources globally. Candidate
    # attribution is the only authority narrowed by this repair.
    venue._indexed_transaction_sources = _indexed_transaction_sources_invocation_only  # type: ignore[assignment]
    venue._source_for_transaction = _source_for_transaction_invocation_only  # type: ignore[assignment]

    current_diagnostic = post161._diagnostic_facts
    if not bool(getattr(current_diagnostic, "_roi_invocation_source_authority", False)):
        _ORIGINAL_DIAGNOSTIC_FACTS = current_diagnostic
        post161._diagnostic_facts = _diagnostic_facts_with_invocation_authority  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_invocation_source_authority", False)):
        _ORIGINAL_STATUS = current_status
        DirectSolanaIngestionPlane.status = _status_with_invocation_source_authority(current_status)  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "SOURCE_AUTHORITY",
    "_indexed_transaction_sources_invocation_only",
    "_invoked_program_counts",
    "_invoked_supported_sources",
    "_source_for_transaction_invocation_only",
    "install_post164_invocation_source_repair",
]
