from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi.direct_funding import SolanaRpcFundingCollector, _native_inbound_transfers
from solana_roi.launch_funding import LaunchFundingPolicy


WALLET = "wallet-a"
FUNDER = "funder-a"


def test_standard_rpc_parser_extracts_native_inbound_system_transfer():
    transaction = {
        "transaction": {
            "message": {
                "instructions": [
                    {
                        "program": "system",
                        "parsed": {
                            "type": "transfer",
                            "info": {"source": FUNDER, "destination": WALLET, "lamports": 250_000_000},
                        },
                    }
                ]
            }
        },
        "meta": {"innerInstructions": []},
    }
    assert _native_inbound_transfers(transaction, WALLET) == [(FUNDER, 250_000_000)]


def test_standard_rpc_parser_reads_inner_system_transfer_and_ignores_outbound():
    transaction = {
        "transaction": {"message": {"instructions": []}},
        "meta": {
            "innerInstructions": [
                {
                    "index": 0,
                    "instructions": [
                        {
                            "program": "system",
                            "parsed": {
                                "type": "transfer",
                                "info": {"source": FUNDER, "destination": WALLET, "lamports": 100_000_000},
                            },
                        },
                        {
                            "program": "system",
                            "parsed": {
                                "type": "transfer",
                                "info": {"source": WALLET, "destination": FUNDER, "lamports": 50_000_000},
                            },
                        },
                    ],
                }
            ]
        },
    }
    assert _native_inbound_transfers(transaction, WALLET) == [(FUNDER, 100_000_000)]


class NeverReachesBoundaryRpc:
    async def get_signatures_for_address(self, _wallet, *, before=None, limit=1000, hedge=False):
        # One recent row on every page. A one-page policy therefore cannot prove
        # complete coverage of the configured lookback and must fail closed.
        return ([{"signature": "recent", "blockTime": 1_788_321_600, "err": None}], "rpc-a", 1.0)


def test_funding_history_is_incomplete_when_bounded_pagination_cannot_reach_lookback():
    collector = SolanaRpcFundingCollector(
        SimpleNamespace(store=None),
        NeverReachesBoundaryRpc(),
        policy=LaunchFundingPolicy(max_history_pages=1),
    )
    rows, covered = asyncio.run(
        collector._signatures(WALLET, datetime(2026, 9, 2, tzinfo=timezone.utc))
    )
    assert rows
    assert covered is False
