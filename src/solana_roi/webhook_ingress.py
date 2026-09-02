from __future__ import annotations

from datetime import datetime
from typing import Any

from .pump_enhanced import PumpFunEnhancedWebhookParser
from .pump_raw import PumpFunRawWebhookParser


class CompositeHeliusWebhookIngestion:
    """Normalize Enhanced traffic plus optional raw Pump.fun deliveries."""

    def __init__(self, service: Any):
        self.service = service
        self.pump_enhanced = PumpFunEnhancedWebhookParser()
        self.pump_raw = PumpFunRawWebhookParser()

    async def ingest_webhook(self, payload: Any, *, received_at: datetime | None = None) -> list[Any]:
        if self.pump_raw.looks_raw(payload):
            decisions = []
            for swap in self.pump_raw.parse(payload, received_at=received_at):
                decisions.append(await self.service.ingest_swap(swap))
            return decisions

        # Classify Pump.fun locally from Enhanced instruction data before the
        # generic parser sees the same transaction as UNKNOWN. The normalized
        # swap store is idempotent, so any later generic duplicate is harmless
        # while the canonical first insert retains the empirical PUMP_FUN label.
        decisions = []
        for swap in self.pump_enhanced.parse(payload, received_at=received_at):
            decisions.append(await self.service.ingest_swap(swap))
        decisions.extend(await self.service.ingest_webhook(payload, received_at=received_at))
        return decisions
