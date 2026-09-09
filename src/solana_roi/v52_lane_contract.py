from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LaneDescriptor:
    lane: str
    economic_surface: str
    venue: str
    lifecycle: str
    strategy_lane: str
    family: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "economic_surface": self.economic_surface,
            "venue": self.venue,
            "lifecycle": self.lifecycle,
            "strategy_lane": self.strategy_lane,
            "family": self.family,
        }


LANE_DESCRIPTORS: dict[str, LaneDescriptor] = {
    "pump_fun": LaneDescriptor("pump_fun", "SOLANA", "PUMP_FUN", "bonding_curve", "elite_wallet_continuation", "PUMP_FUN"),
    "pump_amm": LaneDescriptor("pump_amm", "SOLANA", "PUMP_AMM", "early_post_graduation", "graduation_continuation", "PUMP_AMM"),
    "raydium": LaneDescriptor("raydium", "SOLANA", "RAYDIUM", "post_migration", "migration_continuation", "RAYDIUM"),
    "fomo": LaneDescriptor("fomo", "FOMO", "PUMP_AMM", "early_post_graduation", "fomo_continuation", "FOMO_CLEAN"),
    "robinhood": LaneDescriptor("robinhood", "ROBINHOOD_CHAIN", "UNISWAP_V3", "new_weth_pool", "robinhood_entity_continuation", "ROBINHOOD_ENTITY"),
}

CANONICAL_LANES: tuple[str, ...] = tuple(LANE_DESCRIPTORS)


def descriptor(lane: str) -> LaneDescriptor:
    try:
        return LANE_DESCRIPTORS[str(lane)]
    except KeyError as exc:
        raise ValueError("lane_unsupported") from exc


__all__ = ["CANONICAL_LANES", "LANE_DESCRIPTORS", "LaneDescriptor", "descriptor"]
