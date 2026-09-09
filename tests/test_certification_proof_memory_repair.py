from __future__ import annotations

import json
import sqlite3
import threading
from types import SimpleNamespace

from solana_roi import certification_proof_memory_repair as repair
from solana_roi.strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _fomo_store() -> _Store:
    store = _Store()
    store.db.executescript(
        """
        CREATE TABLE v51_economic_freeze_releases (
            release_commit TEXT NOT NULL,
            economic_freeze_epoch TEXT NOT NULL,
            authority_id TEXT NOT NULL
        );
        CREATE TABLE fomo_paper_outcomes (
            id INTEGER PRIMARY KEY,
            release_commit TEXT NOT NULL,
            source_signature TEXT NOT NULL,
            token_mint TEXT,
            trigger_wallet TEXT,
            venue TEXT,
            lifecycle TEXT,
            regime TEXT,
            position_fraction REAL,
            net_return REAL,
            settled_at TEXT
        );
        CREATE TABLE fomo_paper_trials (
            release_commit TEXT NOT NULL,
            source_signature TEXT NOT NULL,
            fomo_state TEXT,
            signal_to_entry_seconds REAL,
            entry_cost_sol REAL
        );
        CREATE TABLE fomo_shadow_observations (
            release_commit TEXT NOT NULL,
            source_signature TEXT NOT NULL,
            state_json TEXT NOT NULL,
            UNIQUE(release_commit, source_signature)
        );
        """
    )
    store.db.execute(
        "INSERT INTO v51_economic_freeze_releases VALUES (?,?,?)",
        ("release-current", ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
    )
    store.db.execute(
        "INSERT INTO fomo_paper_outcomes VALUES (1,?,?,?,?,?,?,?,?,?,?)",
        (
            "release-current",
            "target-signature",
            "mint-target",
            "wallet-target",
            "PUMP_AMM",
            "continuation",
            "risk_on",
            0.10,
            0.25,
            "2026-09-08T23:00:00+00:00",
        ),
    )
    store.db.execute(
        "INSERT INTO fomo_paper_trials VALUES (?,?,?,?,?)",
        ("release-current", "target-signature", "qualified", 4.0, 0.01),
    )
    store.db.execute(
        "INSERT INTO fomo_shadow_observations VALUES (?,?,?)",
        (
            "release-current",
            "target-signature",
            json.dumps({"liquidity_fragile": True}, sort_keys=True),
        ),
    )
    store.db.executemany(
        "INSERT INTO fomo_shadow_observations VALUES (?,?,?)",
        [
            (
                f"unrelated-release-{index // 50}",
                f"unrelated-signature-{index}",
                json.dumps({"noise": index}),
            )
            for index in range(2500)
        ],
    )
    store.db.commit()
    return store


def test_bounded_fomo_shadow_join_preserves_canonical_records_without_full_scan() -> None:
    store = _fomo_store()
    expected = repair._ORIGINAL_ECONOMIC_RECORDS(store)

    statements: list[str] = []
    store.db.set_trace_callback(statements.append)
    actual = repair._bounded_records(store)
    store.db.set_trace_callback(None)

    assert actual == expected
    assert len(actual) == 1
    assert actual[0]["surface"] == "FOMO"
    assert actual[0]["source_signature"] == "target-signature"

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    fomo_reads = [statement for statement in normalized if "fomo_shadow_observations" in statement]
    assert any("left join fomo_shadow_observations" in statement for statement in fomo_reads)
    assert not any(
        statement.startswith("select release_commit,source_signature,state_json from fomo_shadow_observations")
        for statement in normalized
    )


def test_promotion_population_is_read_once_per_proof_generation(monkeypatch) -> None:
    calls: list[tuple[object, object | None]] = []
    store = object()

    def source(value: object, robinhood_proof: object | None):
        calls.append((value, robinhood_proof))
        return [{"sample": 1}]

    monkeypatch.setattr(repair, "_ORIGINAL_COMBINED_PROMOTION_RECORDS", source)
    previous = getattr(repair._LOCAL, "promotion_cache", None)
    repair._LOCAL.promotion_cache = {}
    try:
        first = repair._shared_combined_promotion_records(store)
        second = repair._shared_combined_promotion_records(store)
    finally:
        if previous is None:
            delattr(repair._LOCAL, "promotion_cache")
        else:
            repair._LOCAL.promotion_cache = previous

    assert first is second
    assert first == [{"sample": 1}]
    assert calls == [(store, None)]


def test_shared_population_forwards_robinhood_proof_and_does_not_alias_distinct_snapshots(monkeypatch) -> None:
    calls: list[tuple[object, dict[str, object] | None]] = []
    store = object()
    robinhood_a: dict[str, object] = {
        "promotion_records": [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-a"}]
    }
    robinhood_b: dict[str, object] = {
        "promotion_records": [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-b"}]
    }

    def source(value: object, robinhood_proof: dict[str, object] | None):
        calls.append((value, robinhood_proof))
        records = list((robinhood_proof or {}).get("promotion_records", []))
        return [dict(row) for row in records if isinstance(row, dict)]

    monkeypatch.setattr(repair, "_ORIGINAL_COMBINED_PROMOTION_RECORDS", source)
    previous = getattr(repair._LOCAL, "promotion_cache", None)
    repair._LOCAL.promotion_cache = {}
    try:
        first = repair._shared_combined_promotion_records(store, robinhood_a)
        second = repair._shared_combined_promotion_records(store, robinhood_a)
        third = repair._shared_combined_promotion_records(store, robinhood_b)
    finally:
        if previous is None:
            delattr(repair._LOCAL, "promotion_cache")
        else:
            repair._LOCAL.promotion_cache = previous

    assert first is second
    assert first == [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-a"}]
    assert third == [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-b"}]
    assert calls == [(store, robinhood_a), (store, robinhood_b)]


def test_phase14_installed_reader_accepts_robinhood_proof_contract(monkeypatch) -> None:
    store = object()
    robinhood_proof = {
        "promotion_records": [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-contract"}]
    }
    calls: list[tuple[object, object]] = []

    def source(value: object, proof: object):
        calls.append((value, proof))
        return [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-contract"}]

    monkeypatch.setattr(repair, "_ORIGINAL_COMBINED_PROMOTION_RECORDS", source)
    monkeypatch.setattr(repair.phase14, "combined_promotion_records", repair._shared_combined_promotion_records)

    records = repair.phase14.combined_promotion_records(store, robinhood_proof)

    assert records == [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-contract"}]
    assert calls == [(store, robinhood_proof)]


def test_proof_wrapper_scopes_shared_population_and_preserves_authority(monkeypatch) -> None:
    store = object()
    robinhood_proof = {
        "promotion_records": [{"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-proof"}]
    }
    calls: list[tuple[object, object]] = []

    def source(value: object, proof: object):
        calls.append((value, proof))
        return [{"sample": 1}, {"surface": "ROBINHOOD_CHAIN", "source_signature": "rh-proof"}]

    def proof_builder():
        left = repair._shared_combined_promotion_records(store, robinhood_proof)
        right = repair._shared_combined_promotion_records(store, robinhood_proof)
        return {"same_population": left is right, "robinhood_preserved": right[-1]["surface"]}

    monkeypatch.setattr(repair, "_ORIGINAL_COMBINED_PROMOTION_RECORDS", source)
    monkeypatch.setattr(repair, "_ORIGINAL_PROOF_BUILDER", proof_builder)

    payload = repair._proof_with_shared_promotion_population()
    status = repair.status()

    assert payload == {"same_population": True, "robinhood_preserved": "ROBINHOOD_CHAIN"}
    assert calls == [(store, robinhood_proof)]
    assert status["resource_guard_relaxed"] is False
    assert status["stale_gate_relaxed"] is False
    assert status["continuity_gate_relaxed"] is False
    assert status["economic_thresholds_changed"] is False
    assert status["canonical_evidence_reset"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
