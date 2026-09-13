from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPLAY = ROOT / "diagnostics" / "portable_repro" / "solana_replay.py"


def _load():
    spec = importlib.util.spec_from_file_location("portable_solana_replay", REPLAY)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_structural_http_results_cover_canonical_hot_methods():
    mod = _load()
    assert mod.result_for("getTransaction", ["sig"], slot=123) is None
    assert mod.result_for("getSignaturesForAddress", ["addr"], slot=123) == []
    assert mod.result_for("getSlot", [], slot=123) == 123
    assert mod.result_for("getBlockHeight", [], slot=123) == 123


def test_latest_blockhash_shape_is_valid_structural_response():
    mod = _load()
    result = mod.result_for("getLatestBlockhash", [], slot=900)
    assert result["context"]["slot"] == 900
    assert result["value"]["lastValidBlockHeight"] == 1050
    assert result["value"]["blockhash"]


def test_replay_state_counts_and_advances_deterministically():
    mod = _load()
    state = mod.ReplayState(slot=100)
    assert state.count("a", "http", "getTransaction") == 1
    assert state.count("a", "http", "getTransaction") == 2
    assert state.next_slot() == 101
    assert state.next_slot() == 102
