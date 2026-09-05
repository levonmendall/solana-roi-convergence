from __future__ import annotations

from typing import Any, Callable

from . import cross_regime_paper_allocator as cross_allocator
from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as v5
from . import risk_conditioned_alpha_v51 as v51
from . import strategy_specialist_wallet_allocator as specialist_allocator
from . import cross_release_learning_repair as repair


COMPAT_VERSION = "cross-release-learning-schema-compat-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

# Capture the pre-repair functions before installing the cross-release layer. These
# are retained only for legacy/minimal schemas used by direct research consumers and
# regression fixtures. Production schemas take the new compatibility-epoch path.
_ORIGINAL_SOLANA_CONTEXT: Callable[..., Any] = v51._context_returns_v51
_ORIGINAL_FOMO_CONTEXT: Callable[..., Any] = v51._fomo_context_returns_v51
_ORIGINAL_FOMO_FORWARD: Callable[..., Any] = fomo_paper._forward_fomo_rows
_ORIGINAL_V5_SPECIALISTS: Callable[..., Any] = specialist_allocator._v5_specialist_rows
_ORIGINAL_FOMO_SPECIALISTS: Callable[..., Any] = specialist_allocator._fomo_specialist_rows
_ORIGINAL_SEGMENT_RETURNS: Callable[..., Any] = cross_allocator._segment_returns
_INSTALLED = False


def _table_columns(store: Any, table: str) -> set[str]:
    try:
        with store._lock:
            rows = store.db.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row[1]) for row in rows}
    except Exception:
        return set()


def _has_columns(store: Any, table: str, required: set[str]) -> bool:
    columns = _table_columns(store, table)
    return bool(columns) and required.issubset(columns)


def _modern_solana_outcomes(store: Any) -> bool:
    return _has_columns(
        store,
        "risk_conditioned_alpha_v5_outcomes",
        {
            "release_commit",
            "strategy_version",
            "source_signature",
            "lane",
            "venue",
            "lifecycle",
            "regime",
            "risk_signature",
            "context_key",
            "net_return",
        },
    )


def _modern_solana_trials(store: Any) -> bool:
    return _has_columns(
        store,
        "risk_conditioned_alpha_v5_trials",
        {
            "release_commit",
            "strategy_version",
            "source_signature",
            "trigger_wallet",
            "lane",
            "venue",
            "lifecycle",
            "regime",
            "trigger_role",
            "risk_signature",
            "risk_severity",
            "decision",
        },
    )


def _modern_fomo_shadow(store: Any) -> bool:
    return (
        _has_columns(
            store,
            "fomo_shadow_observations",
            {"release_commit", "source_signature", "venue", "lifecycle", "regime", "state_json"},
        )
        and _has_columns(
            store,
            "fomo_shadow_outcomes",
            {"release_commit", "source_signature", "net_return"},
        )
        and _has_columns(
            store,
            "profit_first_final_trials",
            {"release_commit", "source_signature", "lane", "trigger_wallet"},
        )
    )


def _modern_fomo_paper(store: Any) -> bool:
    return _has_columns(
        store,
        "fomo_paper_outcomes",
        {
            "release_commit",
            "strategy_version",
            "source_signature",
            "venue",
            "lifecycle",
            "regime",
            "net_return",
        },
    )


def _modern_robinhood_allocator(store: Any) -> bool:
    outcome_columns = _table_columns(store, "robinhood_paper_outcomes")
    if not outcome_columns:
        return True
    return (
        {"release_commit", "trial_id", "net_return"}.issubset(outcome_columns)
        and _has_columns(
            store,
            "robinhood_paper_trials",
            {"id", "release_commit", "strategy_version", "venue", "lifecycle"},
        )
        and _has_columns(
            store,
            "robinhood_v5_trial_context",
            {"trial_id", "lane", "regime", "risk_signature"},
        )
    )


def _safe_solana_context(adapter: Any, **kwargs: Any) -> Any:
    if _modern_solana_outcomes(adapter.store):
        return repair._solana_context_returns_cross_release(adapter, **kwargs)
    return _ORIGINAL_SOLANA_CONTEXT(adapter, **kwargs)


def _safe_fomo_context(adapter: Any, **kwargs: Any) -> Any:
    if _modern_fomo_shadow(adapter.store):
        return repair._fomo_context_returns_cross_release(adapter, **kwargs)
    return _ORIGINAL_FOMO_CONTEXT(adapter, **kwargs)


def _safe_fomo_forward(adapter: Any) -> list[dict[str, Any]]:
    if _modern_fomo_shadow(adapter.store):
        return repair._fomo_forward_rows_cross_release(adapter)
    try:
        return list(_ORIGINAL_FOMO_FORWARD(adapter))
    except Exception:
        return []


def _safe_v5_specialists(universe: Any) -> list[dict[str, Any]]:
    if _modern_solana_trials(universe.store) and _modern_solana_outcomes(universe.store):
        return repair._v5_specialist_rows_cross_release(universe)
    return list(_ORIGINAL_V5_SPECIALISTS(universe))


def _safe_fomo_specialists(universe: Any) -> list[dict[str, Any]]:
    if _modern_fomo_shadow(universe.store):
        return repair._fomo_specialist_rows_cross_release(universe)
    return list(_ORIGINAL_FOMO_SPECIALISTS(universe))


def _safe_segment_returns(store: Any, release_commit: str) -> Any:
    solana_columns = _table_columns(store, "risk_conditioned_alpha_v5_outcomes")
    if solana_columns and not _modern_solana_outcomes(store):
        return _ORIGINAL_SEGMENT_RETURNS(store, release_commit)
    fomo_columns = _table_columns(store, "fomo_paper_outcomes")
    if fomo_columns and not _modern_fomo_paper(store):
        return _ORIGINAL_SEGMENT_RETURNS(store, release_commit)
    if not _modern_robinhood_allocator(store):
        return _ORIGINAL_SEGMENT_RETURNS(store, release_commit)
    return repair._segment_returns_cross_release(store, release_commit)


def install_cross_release_learning_schema_compat() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    repair.install_cross_release_learning_repair()

    # Production tables use the compatibility epoch. Minimal/legacy schemas remain
    # behaviorally identical to the pre-repair implementation instead of crashing
    # on columns that did not exist in those historical contracts.
    v51._context_returns_v51 = _safe_solana_context
    v5._context_returns = _safe_solana_context
    v51._fomo_context_returns_v51 = _safe_fomo_context
    fomo_paper._forward_fomo_rows = _safe_fomo_forward
    specialist_allocator._v5_specialist_rows = _safe_v5_specialists
    specialist_allocator._fomo_specialist_rows = _safe_fomo_specialists
    cross_allocator._segment_returns = _safe_segment_returns

    _INSTALLED = True


__all__ = [
    "COMPAT_VERSION",
    "install_cross_release_learning_schema_compat",
]
