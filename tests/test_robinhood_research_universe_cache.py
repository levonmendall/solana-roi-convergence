from __future__ import annotations

import sqlite3
import threading
from types import SimpleNamespace

from solana_roi import robinhood_research_universe_cache as cache


def _plane() -> SimpleNamespace:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "CREATE TABLE robinhood_launches ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, protocol TEXT NOT NULL, "
        "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, token TEXT NOT NULL, pool TEXT, curve TEXT, "
        "deployer TEXT, pair_token TEXT, fee INTEGER, launch_block INTEGER NOT NULL, "
        "restrictions_end_block INTEGER NOT NULL DEFAULT 0, graduation_threshold TEXT, "
        "paper_eligible INTEGER NOT NULL)"
    )
    store = SimpleNamespace(db=db, _lock=threading.RLock())
    return SimpleNamespace(store=store, release_commit="sha-a")


def _insert(plane: SimpleNamespace, **values) -> None:
    defaults = {
        "release_commit": "sha-a",
        "protocol": "uniswap_v3",
        "venue": "UNISWAP_V3_DIRECT",
        "lifecycle": "new_weth_pool",
        "token": "0x" + "aa" * 20,
        "pool": "0x" + "11" * 20,
        "curve": None,
        "deployer": "0x" + "bb" * 20,
        "pair_token": "0x" + "cc" * 20,
        "fee": None,
        "launch_block": 123,
        "restrictions_end_block": 0,
        "graduation_threshold": None,
        "paper_eligible": 1,
    }
    defaults.update(values)
    plane.store.db.execute(
        "INSERT INTO robinhood_launches("
        "release_commit,protocol,venue,lifecycle,token,pool,curve,deployer,pair_token,fee,launch_block,"
        "restrictions_end_block,graduation_threshold,paper_eligible) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        tuple(defaults[key] for key in (
            "release_commit", "protocol", "venue", "lifecycle", "token", "pool", "curve", "deployer",
            "pair_token", "fee", "launch_block", "restrictions_end_block", "graduation_threshold", "paper_eligible"
        )),
    )
    plane.store.db.commit()


def test_initial_load_preserves_canonical_descriptor_semantics() -> None:
    plane = _plane()
    try:
        _insert(plane, fee=None)
        result = cache._incremental_candidate_universe(plane)
        address = "0x" + "11" * 20
        assert list(result) == [address]
        assert result[address] == {
            "address": address,
            "kind": "v3",
            "protocol": "uniswap_v3",
            "venue": "UNISWAP_V3_DIRECT",
            "lifecycle": "new_weth_pool",
            "token": "0x" + "aa" * 20,
            "deployer": "0x" + "bb" * 20,
            "pair_token": "0x" + "cc" * 20,
            "fee": 10_000,
            "launch_block": 123,
            "restrictions_end_block": 0,
            "graduation_threshold": 0,
        }
        assert plane._roi_research_universe_rows_examined_last_pass == 1
        assert plane._roi_research_universe_eligible_loaded_last_pass == 1
    finally:
        plane.store.db.close()


def test_second_pass_reads_only_rows_inserted_after_cached_frontier() -> None:
    plane = _plane()
    try:
        _insert(plane)
        first = cache._incremental_candidate_universe(plane)
        assert len(first) == 1
        assert plane._roi_research_universe_rows_examined_last_pass == 1

        unchanged = cache._incremental_candidate_universe(plane)
        assert unchanged == first
        assert plane._roi_research_universe_rows_examined_last_pass == 0

        curve = "0x" + "22" * 20
        _insert(
            plane,
            protocol="pons_v2",
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            token="0x" + "dd" * 20,
            pool=None,
            curve=curve,
            fee=0,
            graduation_threshold="500",
        )
        updated = cache._incremental_candidate_universe(plane)
        assert set(updated) == {"0x" + "11" * 20, curve}
        assert updated[curve]["kind"] == "v2"
        assert updated[curve]["graduation_threshold"] == 500
        assert plane._roi_research_universe_rows_examined_last_pass == 1
        assert plane._roi_research_universe_rows_examined_total == 2
    finally:
        plane.store.db.close()


def test_ineligible_release_rows_advance_frontier_without_entering_cache() -> None:
    plane = _plane()
    try:
        _insert(plane, paper_eligible=0, pool="0x" + "44" * 20, token="0x" + "ee" * 20)
        assert cache._incremental_candidate_universe(plane) == {}
        assert plane._roi_research_universe_last_id == 1
        assert plane._roi_research_universe_rows_examined_last_pass == 1
        assert plane._roi_research_universe_eligible_loaded_last_pass == 0

        assert cache._incremental_candidate_universe(plane) == {}
        assert plane._roi_research_universe_rows_examined_last_pass == 0
    finally:
        plane.store.db.close()


def test_other_release_rows_never_enter_or_advance_current_release_cache() -> None:
    plane = _plane()
    try:
        _insert(plane, release_commit="sha-b", pool="0x" + "33" * 20)
        assert cache._incremental_candidate_universe(plane) == {}
        assert plane._roi_research_universe_last_id == 0
    finally:
        plane.store.db.close()


def test_release_change_resets_cache() -> None:
    plane = _plane()
    try:
        _insert(plane, release_commit="sha-a", pool="0x" + "11" * 20)
        _insert(plane, release_commit="sha-b", pool="0x" + "55" * 20, token="0x" + "ff" * 20)
        assert set(cache._incremental_candidate_universe(plane)) == {"0x" + "11" * 20}
        plane.release_commit = "sha-b"
        assert set(cache._incremental_candidate_universe(plane)) == {"0x" + "55" * 20}
    finally:
        plane.store.db.close()
