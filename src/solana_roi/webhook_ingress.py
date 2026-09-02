from __future__ import annotations

from datetime import datetime
from typing import Any

from .pump_raw import PumpFunRawWebhookParser


class CompositeHeliusWebhookIngestion:
    """Route enhanced and Pump.fun raw deliveries into one normalized service."""

    def __init__(self, service: Any):
        self.service = service
        self.pump_raw = PumpFunRawWebhookParser()

    async def ingest_webhook(self, payload: Any, *, received_at: datetime | None = None) -> list[Any]:
        if self.pump_raw.looks_raw(payload):
            decisions = []
            for swap in self.pump_raw.parse(payload, received_at=received_at):
                decisions.append(await self.service.ingest_swap(swap))
            return decisions
        return await self.service.ingest_webhook(payload, received_at=received_at)
