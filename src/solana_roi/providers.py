from __future__ import annotations

from typing import AsyncIterator, Protocol

from .models import Confirmation, RiskSnapshot, WalletTouch


class SolanaSignalProvider(Protocol):
    """Vendor-neutral boundary for real-time Solana wallet and token-risk evidence."""

    async def first_touches(self) -> AsyncIterator[tuple[WalletTouch, RiskSnapshot]]: ...
    async def confirmations(self) -> AsyncIterator[tuple[Confirmation, RiskSnapshot]]: ...


class MarketPriceProvider(Protocol):
    async def reference_price(self, token_mint: str) -> float | None: ...


# Helius webhooks / transactionSubscribe are the intended first concrete ingestion adapters.
# A higher-speed Yellowstone-compatible stream can replace them later without changing
# strategy, portfolio, or certification code.
