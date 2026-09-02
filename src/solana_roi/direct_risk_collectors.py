from __future__ import annotations

from datetime import datetime
from typing import Any

from .live_collectors import JsonRpc, _fresh
from .risk import AuthorityEvidence, DeployerEvidence, RiskDimension, TokenRiskIntelligence


class SolanaAuthorityCollector:
    def __init__(self, risk: TokenRiskIntelligence, rpc: JsonRpc):
        self.risk, self.rpc = risk, rpc

    async def collect(self, mint: str, at: datetime) -> bool:
        if _fresh(self.risk, mint, RiskDimension.AUTHORITY, at):
            return True
        result = await self.rpc.call("getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        value = result.get("value") if isinstance(result, dict) else None
        data = value.get("data") if isinstance(value, dict) else None
        parsed = data.get("parsed") if isinstance(data, dict) else None
        info = parsed.get("info") if isinstance(parsed, dict) else None
        if not isinstance(info, dict):
            return False
        self.risk.record_authority(
            mint,
            AuthorityEvidence(bool(info.get("mintAuthority")), bool(info.get("freezeAuthority"))),
            observed_at=at,
            received_at=at,
            source="solana-standard-rpc:getAccountInfo:jsonParsed",
        )
        return True


class SolanaDeployerCollector:
    """Record creator only if bounded standard-RPC history reaches mint origin."""

    def __init__(self, risk: TokenRiskIntelligence, rpc: JsonRpc, *, page_limit: int = 1000, max_pages: int = 3):
        self.risk, self.rpc = risk, rpc
        self.page_limit, self.max_pages = page_limit, max_pages

    async def collect(self, mint: str, at: datetime) -> bool:
        if _fresh(self.risk, mint, RiskDimension.DEPLOYER, at):
            return True
        before: str | None = None
        oldest: str | None = None
        exhausted = False
        for _ in range(self.max_pages):
            config: dict[str, Any] = {"limit": self.page_limit, "commitment": "confirmed"}
            if before:
                config["before"] = before
            rows = await self.rpc.call("getSignaturesForAddress", [mint, config])
            if not isinstance(rows, list) or not rows:
                exhausted = True
                break
            valid = [row for row in rows if isinstance(row, dict) and row.get("signature")]
            if not valid:
                return False
            oldest = str(valid[-1]["signature"])
            if len(rows) < self.page_limit:
                exhausted = True
                break
            before = oldest
        if not exhausted or not oldest:
            return False
        tx = await self.rpc.call(
            "getTransaction",
            [oldest, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}],
        )
        if not isinstance(tx, dict) or (isinstance(tx.get("meta"), dict) and tx["meta"].get("err")):
            return False
        transaction = tx.get("transaction")
        message = transaction.get("message") if isinstance(transaction, dict) else None
        keys = message.get("accountKeys") if isinstance(message, dict) else None
        if not isinstance(keys, list) or not keys:
            return False
        first = keys[0]
        creator = str(first.get("pubkey") or "") if isinstance(first, dict) else str(first or "")
        if not creator:
            return False
        self.risk.record_deployer(
            mint,
            DeployerEvidence(creator),
            observed_at=at,
            received_at=at,
            source="solana-standard-rpc:bounded-mint-history:first-fee-payer",
        )
        return True
