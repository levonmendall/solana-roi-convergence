from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .direct_solana import DirectSolanaIngestionPlane


NotificationHandler = Callable[[Any, str, dict[int, Any], dict[str, Any]], Awaitable[None]]


def _cooperative_handler(original: NotificationHandler) -> NotificationHandler:
    """Force a scheduler handoff after every raw Solana notification.

    The direct feed intentionally observes the complete frozen seven-program
    universe. During bursts, ``websockets.recv()`` can remain immediately ready
    for long stretches, while the notification handler performs synchronous
    SQLite journaling. An explicit ``sleep(0)`` preserves that scope and receipt
    depth while preventing the single Uvicorn event loop from being starved.
    """

    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, Any],
        message: dict[str, Any],
    ) -> None:
        await original(self, provider, subscription_targets, message)
        await asyncio.sleep(0)

    setattr(handle, "_roi_cooperative_yield", True)
    return handle


def install_direct_stream_fairness() -> None:
    """Install the production scheduling guard exactly once."""

    current = DirectSolanaIngestionPlane._handle_notification
    if bool(getattr(current, "_roi_cooperative_yield", False)):
        return
    DirectSolanaIngestionPlane._handle_notification = _cooperative_handler(current)  # type: ignore[method-assign]


# Install before importing the FastAPI runtime so every production instance uses
# the fair-scheduling handler without changing strategy, sampling, or scope.
install_direct_stream_fairness()

from .api import app as app  # noqa: E402  (installation must happen first)

__all__ = ["app", "install_direct_stream_fairness"]
