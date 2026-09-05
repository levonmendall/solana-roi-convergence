from __future__ import annotations

from typing import Any

from . import robinhood_pumpfun_wallet_intelligence as intelligence


POLICY_VERSION = "robinhood-wallet-intelligence-risk-policy-v1"

# Match Pump.fun semantics: creator/insider participation is informative context,
# not a blanket manipulation veto. Only the actual manipulation/relationship
# conditions are treated as manipulation blockers for teacher qualification.
MANIPULATION_BLOCKERS = (
    "bundled_launch",
    "sniper_heavy",
    "common_funded_early_wallet_cluster",
    "scout_deployer_connection",
)


def _risk_context_as_of_with_token_fallback(
    self: Any,
    actor: str,
    token: str,
    observed_at: str,
) -> tuple[bool, float | None, bool]:
    """Use only risk evidence persisted by the observation time.

    Prefer the exact actor/token context. If that actor never triggered a paper trial,
    use the latest persisted context for the same token as of the observation. This is
    research-only and adds no provider call. Missing token risk remains fail-closed.
    """
    try:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT c.risk_severity,c.risk_json FROM robinhood_paper_trials t "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=t.id "
                "WHERE t.trigger_actor=? AND t.token=? AND t.opened_at<=? "
                "ORDER BY t.id DESC LIMIT 1",
                (actor, token, observed_at),
            ).fetchone()
            if row is None:
                row = self.store.db.execute(
                    "SELECT c.risk_severity,c.risk_json FROM robinhood_paper_trials t "
                    "JOIN robinhood_v5_trial_context c ON c.trial_id=t.id "
                    "WHERE t.token=? AND t.opened_at<=? ORDER BY t.id DESC LIMIT 1",
                    (token, observed_at),
                ).fetchone()
    except Exception:
        row = None
    if row is None:
        return False, None, True
    severity = intelligence._safe_float(row["risk_severity"])
    text = str(row["risk_json"] or "").lower()
    manipulation = bool(
        (severity is not None and severity >= 0.70)
        or any(term in text for term in MANIPULATION_BLOCKERS)
    )
    return True, severity, manipulation


def install_robinhood_wallet_intelligence_policy() -> None:
    if bool(getattr(intelligence, "_roi_robinhood_wallet_intelligence_policy_installed", False)):
        return
    intelligence.MANIPULATION_TERMS = MANIPULATION_BLOCKERS
    intelligence._risk_context_as_of = _risk_context_as_of_with_token_fallback
    setattr(intelligence, "_roi_robinhood_wallet_intelligence_policy_installed", True)
    setattr(intelligence, "_roi_robinhood_wallet_intelligence_policy_version", POLICY_VERSION)


__all__ = [
    "POLICY_VERSION",
    "MANIPULATION_BLOCKERS",
    "install_robinhood_wallet_intelligence_policy",
]
