from __future__ import annotations

import sqlite3
import threading

from solana_roi.strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from solana_roi.v51_execution_evidence_integrity import persist_complete_settlement_lineage


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _lineage(release: str, settlement: str, candidate: str, measurement: str) -> dict[str, str]:
    return {
        "settlement_id": settlement,
        "exit_quote_or_reason": "exit-quote",
        "position_id": "position",
        "entry_id": "entry",
        "entry_quote_id": "entry-quote",
        "authorization_id": "authorization",
        "sizing_id": "sizing",
        "strategy_evaluation_id": "evaluation",
        "candidate_id": candidate,
        "wallet_entity_source_signal_id": "source-signal",
        "normalized_event_id": "normalized-event",
        "source_observation_signature": candidate,
        "release_commit": release,
        "strategy_authority_id": AUTHORITY_ID,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": measurement,
        "economic_event_root_id": "same-economic-root",
        "token_mint": "TOKEN",
        "lifecycle": "continuation",
    }


def test_same_economic_root_cannot_be_persisted_twice_across_releases_or_measurements() -> None:
    store = Store()
    assert persist_complete_settlement_lineage(store, _lineage("release-a", "settle-a", "candidate-a", "m1")) is True
    assert persist_complete_settlement_lineage(store, _lineage("release-b", "settle-b", "candidate-b", "m2")) is False
    row = store.db.execute("SELECT COUNT(*) AS n FROM v51_evidence_settlement_lineage").fetchone()
    assert int(row["n"]) == 1
