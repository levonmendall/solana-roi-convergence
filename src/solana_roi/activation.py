from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .config import StrategyConfig
from .models import RiskSnapshot, WalletTier
from .observation import LatencyCertificationGate
from .observation_store import ObservationEventStore
from .quote import ExecutableQuote, QuoteCertificationGate
from .risk import RiskDimension, RiskPolicy

ARM_CONFIRMATION = "Arm ROI Convergence v3.1 paper cohort"
STATE_MACHINE_CERTIFICATION_ID = "roi-convergence-v3.1-paper-semantics-v1"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _release_commit_from_env() -> str | None:
    for name in ("SOLANA_ROI_RELEASE_COMMIT", "RENDER_GIT_COMMIT", "GITHUB_SHA"):
        value = os.getenv(name, "").strip().lower()
        if re.fullmatch(r"[0-9a-f]{40}", value):
            return value
    return None


@dataclass(frozen=True, slots=True)
class CoverageCertificationPolicy:
    min_samples: int = 100
    min_near_creation_fraction: float = 0.95
    min_early_buyer_complete_fraction: float = 0.95
    min_funding_complete_fraction: float = 0.95
    max_launch_lag_seconds: float = 3.0


class ProgramCoverageCertificationGate:
    """Certify observed launch/funding coverage; configuration alone is never evidence."""

    def __init__(
        self,
        store: ObservationEventStore,
        *,
        configured_fn: Callable[[], bool],
        policy: CoverageCertificationPolicy | None = None,
    ):
        self.store = store
        self.configured_fn = configured_fn
        self.policy = policy or CoverageCertificationPolicy()

    def status(self, *, limit: int = 500) -> dict[str, object]:
        rows = self.store.recent_program_coverage(limit)
        configured = bool(self.configured_fn())
        near = [row for row in rows if row["launch_near_creation"]]
        early = [row for row in rows if row["early_buyers_complete"]]
        funded = [row for row in rows if row["funding_complete"]]
        chronology_conflicts = self.store.first_touch_chronology_conflicts()
        total = len(rows)
        near_fraction = len(near) / total if total else 0.0
        early_fraction = len(early) / total if total else 0.0
        funding_fraction = len(funded) / total if total else 0.0
        certified = bool(
            configured
            and total >= self.policy.min_samples
            and near_fraction >= self.policy.min_near_creation_fraction
            and early_fraction >= self.policy.min_early_buyer_complete_fraction
            and funding_fraction >= self.policy.min_funding_complete_fraction
            and chronology_conflicts == 0
        )
        return {
            "certified": certified,
            "configured": configured,
            "configuration_is_not_certification": True,
            "sample_count": total,
            "near_creation_count": len(near),
            "near_creation_fraction": near_fraction,
            "early_buyer_complete_count": len(early),
            "early_buyer_complete_fraction": early_fraction,
            "funding_complete_count": len(funded),
            "funding_complete_fraction": funding_fraction,
            "first_touch_chronology_conflicts": chronology_conflicts,
            "requirements": asdict(self.policy),
        }


@dataclass(frozen=True, slots=True)
class ActivationDecision:
    authorized: bool
    code: str
    token_mint: str
    stage: str
    decision_at: datetime
    blockers: tuple[str, ...]


class ForwardCohortController:
    """Persist the immutable experiment manifest and one-time paper arming state."""

    def __init__(
        self,
        *,
        store: ObservationEventStore,
        engine: Any,
        config: StrategyConfig,
        risk_policy: RiskPolicy,
        latency_gate: LatencyCertificationGate,
        quote_gate: QuoteCertificationGate,
        coverage_gate: ProgramCoverageCertificationGate,
        release_commit_fn: Callable[[], str | None] = _release_commit_from_env,
        now_fn: Callable[[], datetime] = utcnow,
    ):
        self.store = store
        self.engine = engine
        self.config = config
        self.risk_policy = risk_policy
        self.latency_gate = latency_gate
        self.quote_gate = quote_gate
        self.coverage_gate = coverage_gate
        self.release_commit_fn = release_commit_fn
        self.now_fn = now_fn
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS forward_cohort_manifest ("
                "id INTEGER PRIMARY KEY CHECK(id=1), frozen_at TEXT NOT NULL, release_commit TEXT NOT NULL, "
                "manifest_json TEXT NOT NULL, manifest_sha256 TEXT NOT NULL UNIQUE)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS forward_cohort_arm_state ("
                "id INTEGER PRIMARY KEY CHECK(id=1), armed_at TEXT NOT NULL, manifest_sha256 TEXT NOT NULL)"
            )

    def _manifest_row(self) -> dict[str, Any] | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT frozen_at, release_commit, manifest_json, manifest_sha256 FROM forward_cohort_manifest WHERE id=1"
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["manifest"] = json.loads(str(item.pop("manifest_json")))
        return item

    def _arm_row(self) -> dict[str, Any] | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT armed_at, manifest_sha256 FROM forward_cohort_arm_state WHERE id=1"
            ).fetchone()
        return dict(row) if row is not None else None

    def is_armed(self) -> bool:
        arm = self._arm_row()
        manifest = self._manifest_row()
        return bool(arm and manifest and arm["manifest_sha256"] == manifest["manifest_sha256"])

    def _genesis_untouched(self) -> bool:
        portfolio = self.engine.portfolio
        return bool(
            abs(float(portfolio.initial_capital_usd) - self.config.initial_capital_usd) <= 1e-9
            and abs(float(portfolio.cash_usd) - self.config.initial_capital_usd) <= 1e-9
            and abs(float(self.engine.nav_usd) - self.config.initial_capital_usd) <= 1e-9
            and not portfolio.positions
            and not portfolio.closed
            and self.store.paper_entry_authorization_count() == 0
        )

    def runtime_continuity_ok(self) -> bool:
        """Never silently restart an armed experiment back at a fresh $500 in-memory ledger."""
        if not self.is_armed():
            return True
        prior_entries = self.store.paper_entry_authorization_count()
        if prior_entries == 0:
            return self._genesis_untouched()
        return bool(self.engine.portfolio.positions or self.engine.portfolio.closed)

    def _base_readiness(self) -> dict[str, Any]:
        coverage = self.coverage_gate.status()
        latency = self.latency_gate.status()
        quotes = self.quote_gate.status()
        release_commit = self.release_commit_fn()
        event_chain_valid = self.store.verify()
        genesis_untouched = self._genesis_untouched()
        state_machine_contract = STATE_MACHINE_CERTIFICATION_ID
        requirements = {
            "program_wide_coverage_verified": bool(coverage["certified"]),
            "latency_certified": bool(latency["certified"]),
            "jupiter_quote_certified": bool(quotes["certified"]),
            "six_dimension_risk_bundle_certified": bool(latency["certified"]),
            "event_chain_valid": event_chain_valid,
            "genesis_nav_untouched": genesis_untouched,
            "release_commit_bound": release_commit is not None,
            "paper_state_machine_contract": bool(state_machine_contract),
        }
        return {
            "passed": all(requirements.values()),
            "requirements": requirements,
            "coverage": coverage,
            "latency": latency,
            "execution_quotes": quotes,
            "release_commit": release_commit,
            "state_machine_certification_id": state_machine_contract,
        }

    def status(self) -> dict[str, Any]:
        base = self._base_readiness()
        manifest = self._manifest_row()
        armed = self.is_armed()
        forward_ready = bool(base["passed"] and manifest is not None and not armed)
        return {
            "forward_cohort_ready": forward_ready,
            "armed": armed,
            "paper_only": True,
            "live_money_authority": False,
            "manifest_frozen": manifest is not None,
            "manifest_sha256": manifest["manifest_sha256"] if manifest else None,
            "manifest_release_commit": manifest["release_commit"] if manifest else None,
            "runtime_continuity_ok": self.runtime_continuity_ok(),
            **base,
        }

    def _scout_cohort(self) -> list[dict[str, Any]]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT wallet, entity_id, tier, first_touch_sample_size, historically_eligible, updated_at "
                "FROM wallet_profiles WHERE historically_eligible=1 AND tier IN ('S','A') ORDER BY tier, wallet"
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["historically_eligible"] = bool(item["historically_eligible"])
            result.append(item)
        return result

    def freeze_manifest(self) -> dict[str, Any]:
        existing = self._manifest_row()
        if existing is not None:
            return existing
        if self.is_armed():
            raise RuntimeError("cannot freeze or replace a manifest after cohort arming")
        base = self._base_readiness()
        if not base["passed"]:
            failed = [name for name, passed in base["requirements"].items() if not passed]
            raise RuntimeError("cannot freeze manifest before certification: " + ",".join(failed))
        release_commit = str(base["release_commit"])
        frozen_at = self.now_fn()
        manifest = {
            "schema": "roi-convergence-forward-cohort-manifest.v1",
            "strategy_name": "ROI Convergence v3.1",
            "strategy_version": self.config.version,
            "genesis_nav_usd": self.config.initial_capital_usd,
            "paper_only": True,
            "release_commit": release_commit,
            "scout_cohort": self._scout_cohort(),
            "wallet_scoring_rules": {
                "eligible_tiers": ["S", "A"],
                "history_boundary": "profile must be historically_eligible before candidate first touch; no future evidence",
                "entity_independence": "unknown or same economic entity fails closed for confirmation",
            },
            "risk_thresholds": asdict(self.risk_policy),
            "required_risk_dimensions": [dimension.value for dimension in RiskDimension],
            "strategy_parameters": asdict(self.config),
            "confirmation_window_seconds": self.config.confirmation_window_seconds,
            "chase_ceiling_fraction": self.config.max_chase_fraction,
            "full_position_fraction_of_nav": self.config.full_position_fraction_of_nav,
            "catastrophic_stop_fraction": self.config.catastrophic_stop_fraction,
            "harvest_fraction": self.config.harvest_fraction,
            "runner_fraction": self.config.runner_fraction,
            "runner_trailing_drawdown_fraction": self.config.runner_trailing_drawdown_fraction,
            "execution_cost_model": {
                "execution_drag_per_side_fraction": self.config.execution_drag_per_side_fraction,
                "amount_specific_jupiter_quote_required": True,
            },
            "latency_limits": asdict(self.latency_gate.policy),
            "quote_certification_limits": asdict(self.quote_gate.policy),
            "coverage_certification_limits": asdict(self.coverage_gate.policy),
            "state_machine_certification_id": STATE_MACHINE_CERTIFICATION_ID,
            "frozen_at": frozen_at.isoformat(),
        }
        raw = _canonical_json(manifest)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO forward_cohort_manifest(id, frozen_at, release_commit, manifest_json, manifest_sha256) "
                "VALUES (1, ?, ?, ?, ?)",
                (frozen_at.isoformat(), release_commit, raw, digest),
            )
        self.store.append(
            "forward_cohort_manifest_frozen",
            frozen_at.isoformat(),
            {"manifest_sha256": digest, "release_commit": release_commit, "strategy_version": self.config.version},
        )
        return self._manifest_row() or {}

    def arm(self, confirmation: str) -> dict[str, Any]:
        if confirmation != ARM_CONFIRMATION:
            raise ValueError(f"arming requires exact confirmation: {ARM_CONFIRMATION}")
        if self.is_armed():
            return self._arm_row() or {}
        status = self.status()
        if not status["forward_cohort_ready"]:
            raise RuntimeError("forward cohort is not ready")
        if not self._genesis_untouched():
            raise RuntimeError("$500 genesis is not untouched")
        manifest = self._manifest_row()
        if manifest is None:
            raise RuntimeError("frozen manifest unavailable")
        armed_at = self.now_fn()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO forward_cohort_arm_state(id, armed_at, manifest_sha256) VALUES (1, ?, ?)",
                (armed_at.isoformat(), manifest["manifest_sha256"]),
            )
        self.store.append(
            "forward_cohort_armed",
            armed_at.isoformat(),
            {"manifest_sha256": manifest["manifest_sha256"], "genesis_nav_usd": self.config.initial_capital_usd},
        )
        return self._arm_row() or {}


class CandidateActivationGate:
    """The only authority that may emit PAPER_ENTRY_AUTHORIZED."""

    def __init__(
        self,
        *,
        controller: ForwardCohortController,
        engine: Any,
        store: ObservationEventStore,
        max_quote_age_seconds: float = 2.0,
    ):
        self.controller = controller
        self.engine = engine
        self.store = store
        self.max_quote_age_seconds = max_quote_age_seconds

    def evaluate(
        self,
        *,
        token_mint: str,
        stage: str,
        fraction_of_full_position: float,
        scout_profile: Any,
        first_touch: dict[str, Any],
        risk: RiskSnapshot | None,
        risk_readiness: dict[str, Any],
        quote: ExecutableQuote | None,
        decision_at: datetime,
        confirmation_observed_at: datetime | None = None,
        confirmation_entity_id: str | None = None,
    ) -> ActivationDecision:
        blockers: list[str] = []
        if not self.controller.is_armed():
            blockers.append("cohort_not_armed")
        if not self.controller.runtime_continuity_ok():
            blockers.append("runtime_portfolio_continuity_unproven")
        if scout_profile is None or not scout_profile.historically_eligible or scout_profile.tier not in {WalletTier.S, WalletTier.A}:
            blockers.append("scout_not_frozen_eligible_s_or_a")
        if str(first_touch.get("wallet") or "") != getattr(scout_profile, "wallet", None):
            blockers.append("first_touch_scout_mismatch")
        if self.store.token_first_touch_has_earlier_eligible_swap(token_mint):
            blockers.append("first_touch_chronology_conflict")
        if not bool(risk_readiness.get("complete")) or not bool(risk_readiness.get("fresh")):
            blockers.append("six_dimension_risk_bundle_incomplete_or_stale")
        fresh_dimensions = risk_readiness.get("fresh_dimensions")
        if not isinstance(fresh_dimensions, dict) or any(not fresh_dimensions.get(d.value) for d in RiskDimension):
            blockers.append("six_dimension_freshness_unproven")
        if risk is None or not risk.clean:
            blockers.append("hard_risk_veto")
        first_at = datetime.fromisoformat(str(first_touch["observed_at"]))
        if stage in {"confirmation_add", "confirmed_full"}:
            if confirmation_observed_at is None or confirmation_entity_id is None:
                blockers.append("independent_confirmation_missing")
            else:
                age = (confirmation_observed_at - first_at).total_seconds()
                if age < 0 or age > self.engine.config.confirmation_window_seconds:
                    blockers.append("confirmation_outside_frozen_window")
                if confirmation_entity_id == str(first_touch.get("entity_id") or ""):
                    blockers.append("confirmation_not_independent")
        if quote is None:
            blockers.append("amount_specific_jupiter_quote_missing")
        else:
            if not quote.usable or quote.drift_fraction > self.engine.config.max_chase_fraction:
                blockers.append("executable_quote_exceeds_chase_ceiling")
            if quote.received_at < decision_at:
                blockers.append("quote_precedes_final_risk_decision")
            quote_age = max(0.0, (decision_at - quote.received_at).total_seconds())
            if quote_age > self.max_quote_age_seconds:
                blockers.append("executable_quote_stale")
            expected = self.engine.portfolio.full_position_notional(self.engine.marks) * fraction_of_full_position
            tolerance = max(0.01, expected * 0.001)
            if abs(quote.requested_notional_usd - expected) > tolerance:
                blockers.append("quote_not_sized_to_current_nav")
            if quote.requested_notional_usd > self.engine.portfolio.cash_usd + 1e-9:
                blockers.append("insufficient_paper_buying_power")
            if quote.chain_to_quote_ms > self.engine.config.confirmation_window_seconds * 1000.0:
                blockers.append("chain_to_quote_exhausts_confirmation_window")
        global_status = self.controller.status()
        if not global_status["coverage"]["certified"]:
            blockers.append("program_wide_coverage_not_certified")
        if not global_status["latency"]["certified"]:
            blockers.append("system_latency_not_certified")
        if not global_status["execution_quotes"]["certified"]:
            blockers.append("jupiter_quote_path_not_certified")
        authorized = not blockers
        code = "PAPER_ENTRY_AUTHORIZED" if authorized else "record_only"
        decision = ActivationDecision(authorized, code, token_mint, stage, decision_at, tuple(blockers))
        self.store.append(
            "candidate_activation_decision",
            decision_at.isoformat(),
            {
                "authorized": authorized,
                "code": code,
                "token_mint": token_mint,
                "stage": stage,
                "fraction_of_full_position": fraction_of_full_position,
                "blockers": list(blockers),
                "quote_received_at": quote.received_at.isoformat() if quote else None,
                "quote_price_sol": quote.effective_price_sol if quote else None,
                "scout_reference_price_sol": first_touch.get("reference_price_sol"),
            },
        )
        return decision
