from __future__ import annotations

from dataclasses import dataclass
from typing import Any


LANE_CONTRACT_VERSION = "v52-five-lane-production-contract-v2"


@dataclass(frozen=True)
class LaneDescriptor:
    lane: str
    economic_surface: str
    venue: str
    lifecycle: str
    strategy_lane: str
    family: str
    surface_aliases: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "economic_surface": self.economic_surface,
            "venue": self.venue,
            "lifecycle": self.lifecycle,
            "strategy_lane": self.strategy_lane,
            "family": self.family,
            "surface_aliases": list(self.surface_aliases),
        }


LANE_DESCRIPTORS: dict[str, LaneDescriptor] = {
    "pump_fun": LaneDescriptor(
        "pump_fun", "SOLANA", "PUMP_FUN", "bonding_curve",
        "elite_wallet_continuation", "PUMP_FUN", ("PUMP_FUN",),
    ),
    "pump_amm": LaneDescriptor(
        "pump_amm", "SOLANA", "PUMP_AMM", "early_post_graduation",
        "graduation_continuation", "PUMP_AMM", ("PUMP_AMM", "PUMPSWAP"),
    ),
    "raydium": LaneDescriptor(
        "raydium", "SOLANA", "RAYDIUM", "post_migration",
        "raydium_cross_venue_persistence", "RAYDIUM", ("RAYDIUM",),
    ),
    "fomo": LaneDescriptor(
        "fomo", "FOMO", "PUMP_AMM", "early_post_graduation",
        "fomo_continuation", "FOMO_CLEAN", ("FOMO",),
    ),
    "robinhood": LaneDescriptor(
        "robinhood", "ROBINHOOD_CHAIN", "UNISWAP_V3", "new_weth_pool",
        "robinhood_entity_continuation", "ROBINHOOD_ENTITY", ("ROBINHOOD_CHAIN",),
    ),
}

CANONICAL_LANES: tuple[str, ...] = tuple(LANE_DESCRIPTORS)


def descriptor(lane: str) -> LaneDescriptor:
    try:
        return LANE_DESCRIPTORS[str(lane)]
    except KeyError as exc:
        raise ValueError("lane_unsupported") from exc


def canonical_lane_for_surface(surface: str) -> str:
    value = str(surface or "").strip().upper()
    for lane, item in LANE_DESCRIPTORS.items():
        if value in item.surface_aliases:
            return lane
    raise ValueError("surface_unsupported")


def lane_contract() -> dict[str, Any]:
    return {
        "version": LANE_CONTRACT_VERSION,
        "canonical_lanes": list(CANONICAL_LANES),
        "lane_count": len(CANONICAL_LANES),
        "pumpswap_authority_lane": "pump_amm",
        "lanes": {lane: item.as_dict() for lane, item in LANE_DESCRIPTORS.items()},
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "CANONICAL_LANES",
    "LANE_CONTRACT_VERSION",
    "LANE_DESCRIPTORS",
    "LaneDescriptor",
    "canonical_lane_for_surface",
    "descriptor",
    "lane_contract",
]
