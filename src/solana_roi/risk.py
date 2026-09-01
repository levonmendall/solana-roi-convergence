from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol

from .models import RiskSnapshot
from .storage import AppendOnlyEventStore


class RiskDimension(str, Enum):
    AUTHORITY = "authority"
    LIQUIDITY = "liquidity"
    LAUNCH = "launch"
    FLOW = "flow"
    FUNDING = "funding"
    DEPLOYER = "deployer"


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    """Pre-forward-cohort evidence-quality policy, not an optimized return model."""

    authority_max_age_seconds: float = 60.0
    liquidity_max_age_seconds: float = 5.0
    launch_max_age_seconds: float = 30.0
    flow_max_age_seconds: float = 5.0
    funding_max_age_seconds: float = 30.0
    deployer_max_age_seconds: float = 60.0
    min_liquidity_usd: float = 1_500.0
    min_liquidity_market_cap_fraction: float = 0.02
    confirmed_entity_link_confidence: float = 0.95

    def max_age(self, dimension: RiskDimension) -> float:
        return {
            RiskDimension.AUTHORITY: self.authority_max_age_seconds,
            RiskDimension.LIQUIDITY: self.liquidity_max_age_seconds,
            RiskDimension.LAUNCH: self.launch_max_age_seconds,
            RiskDimension.FLOW: self.flow_max_age_seconds,
            RiskDimension.FUNDING: self.funding_max_age_seconds,
            RiskDimension.DEPLOYER: self.deployer_max_age_seconds,
        }[dimension]


@dataclass(frozen=True, slots=True)
class AuthorityEvidence:
    mint_authority_active: bool
    freeze_authority_active: bool


@dataclass(frozen=True, slots=True)
class LiquidityEvidence:
    liquidity_usd: float
    market_cap_usd: float | None = None


@dataclass(frozen=True, slots=True)
class LaunchEvidence:
    bundled_launch: bool
    sniper_heavy: bool


@dataclass(frozen=True, slots=True)
class FlowEvidence:
    early_buyers_exiting: bool
    abnormal_sell_pressure: bool


@dataclass(frozen=True, slots=True)
class FundingEvidence:
    early_buyer_wallets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DeployerEvidence:
    deployer_wallet: str | None


@dataclass(frozen=True, slots=True)
class EntityLink:
    wallet_a: str
    wallet_b: str
    relationship: str
    confidence: float
    observed_at: datetime
    received_at: datetime
    source: str


class WalletRegistry(Protocol):
    def get(self, wallet: str): ...


class EntityResolver:
    """Collapse addresses only from explicit identity or high-confidence graph links."""

    def __init__(self, store: AppendOnlyEventStore, registry: WalletRegistry, *, min_confidence: float = 0.95):
        self.store = store
        self.registry = registry
        self.min_confidence = min_confidence

    def record_link(self, link: EntityLink) -> bool:
        if link.wallet_a == link.wallet_b:
            return False
        if not 0.0 <= link.confidence <= 1.0:
            raise ValueError("entity-link confidence must be between 0 and 1")
        inserted = self.store.record_entity_link(
            wallet_a=link.wallet_a,
            wallet_b=link.wallet_b,
            relationship=link.relationship,
            confidence=link.confidence,
            observed_at=link.observed_at.isoformat(),
            received_at=link.received_at.isoformat(),
            source=link.source,
        )
        if inserted:
            self.store.append("entity_link", link.received_at.isoformat(), asdict(link))
        return inserted

    def component(self, wallet: str, *, as_of: datetime) -> set[str]:
        seen = {wallet}
        pending = [wallet]
        while pending:
            current = pending.pop()
            rows = self.store.entity_neighbors(
                current,
                as_of_received_at=as_of.isoformat(),
                min_confidence=self.min_confidence,
            )
            for row in rows:
                other = str(row["wallet_b"] if row["wallet_a"] == current else row["wallet_a"])
                if other not in seen:
                    seen.add(other)
                    pending.append(other)
        return seen

    def entity_id_for(self, wallet: str, *, fallback_entity_id: str | None, as_of: datetime) -> str:
        component = self.component(wallet, as_of=as_of)
        anchors: set[str] = set()
        for member in component:
            profile = self.registry.get(member)
            if profile is not None and profile.entity_id:
                anchors.add(profile.entity_id)
        if fallback_entity_id:
            anchors.add(fallback_entity_id)
        return sorted(anchors)[0] if anchors else "graph:" + sorted(component)[0]

    def same_entity(
        self,
        wallet_a: str,
        wallet_b: str,
        *,
        as_of: datetime,
        fallback_entity_a: str | None = None,
        fallback_entity_b: str | None = None,
    ) -> bool:
        if wallet_a == wallet_b:
            return True
        if fallback_entity_a and fallback_entity_b and fallback_entity_a == fallback_entity_b:
            return True
        return wallet_b in self.component(wallet_a, as_of=as_of)

    def component_summary(self, wallet: str, *, as_of: datetime) -> dict[str, object]:
        component = sorted(self.component(wallet, as_of=as_of))
        profile = self.registry.get(wallet)
        return {
            "wallet": wallet,
            "entity_id": self.entity_id_for(
                wallet,
                fallback_entity_id=profile.entity_id if profile is not None else None,
                as_of=as_of,
            ),
            "wallets": component,
        }


class TokenRiskIntelligence:
    """Compose complete, fresh, no-lookahead evidence into the strategy RiskSnapshot."""

    REQUIRED_DIMENSIONS = tuple(RiskDimension)

    def __init__(
        self,
        store: AppendOnlyEventStore,
        *,
        entity_resolver: EntityResolver,
        registry: WalletRegistry,
        policy: RiskPolicy | None = None,
    ):
        self.store = store
        self.registry = registry
        self.policy = policy or RiskPolicy()
        self.entity_resolver = entity_resolver

    def _record(
        self,
        token_mint: str,
        dimension: RiskDimension,
        value: object,
        *,
        observed_at: datetime,
        received_at: datetime | None,
        source: str,
    ) -> bool:
        received_at = received_at or datetime.now(timezone.utc)
        payload = asdict(value)
        inserted = self.store.record_risk_evidence(
            token_mint=token_mint,
            dimension=dimension.value,
            observed_at=observed_at.isoformat(),
            received_at=received_at.isoformat(),
            source=source,
            payload=payload,
        )
        if inserted:
            self.store.append(
                "risk_evidence",
                received_at.isoformat(),
                {
                    "token_mint": token_mint,
                    "dimension": dimension.value,
                    "observed_at": observed_at.isoformat(),
                    "received_at": received_at.isoformat(),
                    "source": source,
                    "payload": payload,
                },
            )
        return inserted

    def record_authority(self, token_mint: str, value: AuthorityEvidence, *, observed_at: datetime, received_at: datetime | None = None, source: str) -> bool:
        return self._record(token_mint, RiskDimension.AUTHORITY, value, observed_at=observed_at, received_at=received_at, source=source)

    def record_liquidity(self, token_mint: str, value: LiquidityEvidence, *, observed_at: datetime, received_at: datetime | None = None, source: str) -> bool:
        if value.liquidity_usd < 0 or (value.market_cap_usd is not None and value.market_cap_usd <= 0):
            raise ValueError("invalid liquidity/market-cap evidence")
        return self._record(token_mint, RiskDimension.LIQUIDITY, value, observed_at=observed_at, received_at=received_at, source=source)

    def record_launch(self, token_mint: str, value: LaunchEvidence, *, observed_at: datetime, received_at: datetime | None = None, source: str) -> bool:
        return self._record(token_mint, RiskDimension.LAUNCH, value, observed_at=observed_at, received_at=received_at, source=source)

    def record_flow(self, token_mint: str, value: FlowEvidence, *, observed_at: datetime, received_at: datetime | None = None, source: str) -> bool:
        return self._record(token_mint, RiskDimension.FLOW, value, observed_at=observed_at, received_at=received_at, source=source)

    def record_funding(self, token_mint: str, value: FundingEvidence, *, observed_at: datetime, received_at: datetime | None = None, source: str) -> bool:
        return self._record(token_mint, RiskDimension.FUNDING, value, observed_at=observed_at, received_at=received_at, source=source)

    def record_deployer(self, token_mint: str, value: DeployerEvidence, *, observed_at: datetime, received_at: datetime | None = None, source: str) -> bool:
        return self._record(token_mint, RiskDimension.DEPLOYER, value, observed_at=observed_at, received_at=received_at, source=source)

    def record_bundle(
        self,
        token_mint: str,
        *,
        authority: AuthorityEvidence,
        liquidity: LiquidityEvidence,
        launch: LaunchEvidence,
        flow: FlowEvidence,
        funding: FundingEvidence,
        deployer: DeployerEvidence,
        observed_at: datetime,
        received_at: datetime | None = None,
        source: str,
    ) -> None:
        received_at = received_at or datetime.now(timezone.utc)
        self.record_authority(token_mint, authority, observed_at=observed_at, received_at=received_at, source=source)
        self.record_liquidity(token_mint, liquidity, observed_at=observed_at, received_at=received_at, source=source)
        self.record_launch(token_mint, launch, observed_at=observed_at, received_at=received_at, source=source)
        self.record_flow(token_mint, flow, observed_at=observed_at, received_at=received_at, source=source)
        self.record_funding(token_mint, funding, observed_at=observed_at, received_at=received_at, source=source)
        self.record_deployer(token_mint, deployer, observed_at=observed_at, received_at=received_at, source=source)

    def _latest_complete(self, token_mint: str, *, decision_at: datetime) -> dict[RiskDimension, dict[str, Any]] | None:
        rows: dict[RiskDimension, dict[str, Any]] = {}
        for dimension in self.REQUIRED_DIMENSIONS:
            raw = self.store.latest_risk_evidence(
                token_mint,
                dimension.value,
                as_of_received_at=decision_at.isoformat(),
            )
            if raw is None:
                return None
            observed = datetime.fromisoformat(str(raw["observed_at"]))
            age = (decision_at - observed).total_seconds()
            if age < 0 or age > self.policy.max_age(dimension):
                return None
            rows[dimension] = raw
        return rows

    @staticmethod
    def _bool(payload: dict[str, Any], key: str) -> bool:
        if key not in payload or not isinstance(payload[key], bool):
            raise ValueError(f"risk evidence missing boolean field {key}")
        return bool(payload[key])

    async def snapshot(
        self,
        token_mint: str,
        observed_at: datetime,
        *,
        scout_wallet: str | None = None,
        scout_entity_id: str | None = None,
    ) -> RiskSnapshot | None:
        rows = self._latest_complete(token_mint, decision_at=observed_at)
        if rows is None:
            return None
        authority = dict(rows[RiskDimension.AUTHORITY]["payload"])
        liquidity = dict(rows[RiskDimension.LIQUIDITY]["payload"])
        launch = dict(rows[RiskDimension.LAUNCH]["payload"])
        flow = dict(rows[RiskDimension.FLOW]["payload"])
        funding = dict(rows[RiskDimension.FUNDING]["payload"])
        deployer = dict(rows[RiskDimension.DEPLOYER]["payload"])

        liquidity_usd = float(liquidity["liquidity_usd"])
        market_cap_raw = liquidity.get("market_cap_usd")
        market_cap_usd = float(market_cap_raw) if market_cap_raw is not None else None
        thin = liquidity_usd < self.policy.min_liquidity_usd
        if market_cap_usd is not None and market_cap_usd > 0:
            thin = thin or liquidity_usd / market_cap_usd < self.policy.min_liquidity_market_cap_fraction

        early_wallets = list(dict.fromkeys(str(item) for item in funding.get("early_buyer_wallets", ()) if str(item)))
        common_cluster = False
        for index, wallet_a in enumerate(early_wallets):
            profile_a = self.registry.get(wallet_a)
            for wallet_b in early_wallets[index + 1:]:
                profile_b = self.registry.get(wallet_b)
                if self.entity_resolver.same_entity(
                    wallet_a,
                    wallet_b,
                    as_of=observed_at,
                    fallback_entity_a=profile_a.entity_id if profile_a else None,
                    fallback_entity_b=profile_b.entity_id if profile_b else None,
                ):
                    common_cluster = True
                    break
            if common_cluster:
                break

        deployer_wallet = str(deployer.get("deployer_wallet")) if deployer.get("deployer_wallet") else None
        scout_deployer = False
        if scout_wallet and deployer_wallet:
            deployer_profile = self.registry.get(deployer_wallet)
            scout_deployer = self.entity_resolver.same_entity(
                scout_wallet,
                deployer_wallet,
                as_of=observed_at,
                fallback_entity_a=scout_entity_id,
                fallback_entity_b=deployer_profile.entity_id if deployer_profile else None,
            )

        latest_observed = max(datetime.fromisoformat(str(row["observed_at"])) for row in rows.values())
        return RiskSnapshot(
            observed_at=latest_observed,
            early_buyers_exiting=self._bool(flow, "early_buyers_exiting"),
            bundled_launch=self._bool(launch, "bundled_launch"),
            sniper_heavy=self._bool(launch, "sniper_heavy"),
            abnormal_sell_pressure=self._bool(flow, "abnormal_sell_pressure"),
            common_funded_early_wallet_cluster=common_cluster,
            scout_deployer_connection=scout_deployer,
            dangerous_authority=(self._bool(authority, "mint_authority_active") or self._bool(authority, "freeze_authority_active")),
            unacceptable_liquidity=thin,
            stale=False,
        )

    def readiness(self, token_mint: str, *, as_of: datetime) -> dict[str, object]:
        present: dict[str, bool] = {}
        fresh: dict[str, bool] = {}
        for dimension in self.REQUIRED_DIMENSIONS:
            raw = self.store.latest_risk_evidence(token_mint, dimension.value, as_of_received_at=as_of.isoformat())
            present[dimension.value] = raw is not None
            if raw is None:
                fresh[dimension.value] = False
                continue
            observed = datetime.fromisoformat(str(raw["observed_at"]))
            age = (as_of - observed).total_seconds()
            fresh[dimension.value] = 0 <= age <= self.policy.max_age(dimension)
        return {
            "token_mint": token_mint,
            "complete": all(present.values()),
            "fresh": all(fresh.values()),
            "present": present,
            "fresh_dimensions": fresh,
        }
