"""Missing continuity evidence is not affirmative permission to retire receipts.

These unit tests use the actual retention module and disposable in-memory SQLite.
They exercise the shared selector used for pruning and rollover, not a mock of it.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Literal

import pytest

from solana_roi.raw_receipt_retention import (
    prune_recent_receipts,
    recent_receipt_acknowledgement_boundary,
)

NOW = datetime(2026, 9, 16, 4, 0, tzinfo=timezone.utc)
Mode = Literal["raw", "normalized"]


def _fixture(mode: Mode, continuity: str, *, age_seconds: int = 121) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        "CREATE TABLE direct_solana_recent_receipts("
        "id INTEGER PRIMARY KEY,signature TEXT NOT NULL,received_at TEXT NOT NULL);"
        "CREATE TABLE wallet_discovery_state("
        "id INTEGER PRIMARY KEY,last_raw_receipt_id INTEGER NOT NULL);"
        "INSERT INTO wallet_discovery_state VALUES(1,1);"
    )
    connection.execute(
        "INSERT INTO direct_solana_recent_receipts VALUES(1,'receipt-1',?)",
        ((NOW - timedelta(seconds=age_seconds)).isoformat(),),
    )
    if mode == "normalized":
        connection.executescript(
            "ALTER TABLE wallet_discovery_state "
            "ADD COLUMN last_normalized_swap_id INTEGER NOT NULL DEFAULT 1;"
            "CREATE TABLE direct_solana_hydration_queue("
            "signature TEXT PRIMARY KEY,status TEXT NOT NULL);"
            "CREATE TABLE direct_solana_hydration_metrics("
            "signature TEXT PRIMARY KEY,normalized INTEGER NOT NULL);"
            "CREATE TABLE normalized_swaps(id INTEGER PRIMARY KEY,signature TEXT NOT NULL);"
            "INSERT INTO direct_solana_hydration_queue VALUES('receipt-1','complete');"
            "INSERT INTO direct_solana_hydration_metrics VALUES('receipt-1',1);"
            "INSERT INTO normalized_swaps VALUES(1,'receipt-1');"
        )
    if continuity != "missing_table":
        connection.execute(
            "CREATE TABLE direct_solana_global_state("
            "id INTEGER PRIMARY KEY,unresolved_gap INTEGER NOT NULL)"
        )
        if continuity != "missing_row":
            connection.execute(
                "INSERT INTO direct_solana_global_state VALUES(1,?)",
                (int(continuity == "unresolved_gap"),),
            )
    connection.commit()
    return connection


def _selected(connection: sqlite3.Connection) -> list[int]:
    boundary = recent_receipt_acknowledgement_boundary(connection, now=NOW)
    return [
        int(row[0])
        for row in connection.execute(
            "SELECT r.id FROM direct_solana_recent_receipts r WHERE "
            + str(boundary["eligible_predicate"]),
            tuple(boundary["eligible_args"]),
        )
    ]


@pytest.mark.parametrize("mode", ["raw", "normalized"])
@pytest.mark.parametrize(
    ("continuity", "reason"),
    [
        ("missing_table", "direct_solana_global_state_missing"),
        ("missing_row", "direct_solana_global_state_row_missing"),
        ("unresolved_gap", "unresolved_continuity_gap"),
    ],
)
def test_uncertain_continuity_retains_receipts_in_shared_selector_and_pruner(
    mode: Mode, continuity: str, reason: str
) -> None:
    with closing(_fixture(mode, continuity)) as connection:
        selected = _selected(connection)
        result = prune_recent_receipts(connection, now=NOW)
        remaining = connection.execute(
            "SELECT COUNT(*) FROM direct_solana_recent_receipts"
        ).fetchone()[0]
        assert result["deleted"] == 0, (mode, continuity, result)
        assert remaining == 1
        assert selected == [], "The rollover selector must also retain this receipt"
        assert reason in result["acknowledgement_blockers"]


@pytest.mark.parametrize("mode", ["raw", "normalized"])
def test_complete_continuity_and_durable_ack_still_allow_retirement(mode: Mode) -> None:
    with closing(_fixture(mode, "clear")) as connection:
        assert _selected(connection) == [1]
        result = prune_recent_receipts(connection, now=NOW)
        assert result["deleted"] == 1
        assert result["acknowledgement_blockers"] == []


@pytest.mark.parametrize("mode", ["raw", "normalized"])
@pytest.mark.parametrize("age_seconds", [119, 120])
def test_two_minute_floor_is_unchanged(mode: Mode, age_seconds: int) -> None:
    with closing(_fixture(mode, "clear", age_seconds=age_seconds)) as connection:
        assert _selected(connection) == []
        assert prune_recent_receipts(connection, now=NOW)["deleted"] == 0


@pytest.mark.parametrize("mode", ["raw", "normalized"])
@pytest.mark.parametrize("continuity", ["missing_table", "missing_row"])
def test_retirement_resumes_after_continuity_evidence_is_durably_restored(
    mode: Mode, continuity: str
) -> None:
    with closing(_fixture(mode, continuity)) as connection:
        first = prune_recent_receipts(connection, now=NOW)
        assert first["deleted"] == 0
        if continuity == "missing_table":
            connection.execute(
                "CREATE TABLE direct_solana_global_state("
                "id INTEGER PRIMARY KEY,unresolved_gap INTEGER NOT NULL)"
            )
        connection.execute("INSERT INTO direct_solana_global_state VALUES(1,0)")
        connection.commit()
        assert _selected(connection) == [1]
        assert prune_recent_receipts(connection, now=NOW)["deleted"] == 1
        assert prune_recent_receipts(connection, now=NOW)["deleted"] == 0


@pytest.mark.parametrize("mode", ["raw", "normalized"])
def test_missing_wallet_cursor_row_stays_protected(mode: Mode) -> None:
    with closing(_fixture(mode, "clear")) as connection:
        connection.execute("DELETE FROM wallet_discovery_state")
        connection.commit()
        assert _selected(connection) == []
        result = prune_recent_receipts(connection, now=NOW)
        assert result["deleted"] == 0
        assert "wallet_discovery_state_row_missing" in result["acknowledgement_blockers"]


@pytest.mark.parametrize(
    "interruption",
    ["queue", "metric", "normalized_swap", "wallet_cursor"],
)
def test_interrupted_normalization_acknowledgements_stay_protected(interruption: str) -> None:
    with closing(_fixture("normalized", "clear")) as connection:
        command = {
            "queue": "DELETE FROM direct_solana_hydration_queue",
            "metric": "DELETE FROM direct_solana_hydration_metrics",
            "normalized_swap": "DELETE FROM normalized_swaps",
            "wallet_cursor": "UPDATE wallet_discovery_state SET last_normalized_swap_id=0",
        }[interruption]
        connection.execute(command)
        connection.commit()
        assert _selected(connection) == []
        assert prune_recent_receipts(connection, now=NOW)["deleted"] == 0
