from __future__ import annotations

import asyncio
from pathlib import Path

from solana_roi import production
from solana_roi.direct_solana import DirectSolanaIngestionPlane


def test_full_scope_notification_handler_forces_cooperative_scheduler_handoff(monkeypatch):
    events: list[object] = []

    async def original(_self, provider, subscription_targets, message):
        events.append((provider, subscription_targets, message))

    async def fake_sleep(delay):
        events.append(("yield", delay))

    monkeypatch.setattr(production.asyncio, "sleep", fake_sleep)
    handler = production._cooperative_handler(original)
    asyncio.run(handler(object(), "rpc-a", {1: object()}, {"method": "logsNotification"}))

    assert events[0][0] == "rpc-a"
    assert events[-1] == ("yield", 0)
    assert bool(getattr(handler, "_roi_cooperative_yield", False))


def test_production_fairness_install_is_idempotent():
    first = DirectSolanaIngestionPlane._handle_notification
    assert bool(getattr(first, "_roi_cooperative_yield", False))
    production.install_direct_stream_fairness()
    second = DirectSolanaIngestionPlane._handle_notification
    assert second is first


def test_render_liveness_uses_constant_time_strategy_route():
    blueprint = Path("render.yaml").read_text()
    assert "startCommand: uvicorn solana_roi.production:app" in blueprint
    assert "healthCheckPath: /v1/strategy/baseline" in blueprint
    assert "healthCheckPath: /health" not in blueprint
