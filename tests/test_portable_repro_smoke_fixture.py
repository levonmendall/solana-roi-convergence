from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "diagnostics" / "portable_repro" / "generate_smoke_fixture.py"


def _load():
    spec = importlib.util.spec_from_file_location("portable_smoke_fixture", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_smoke_fixture_uses_canonical_schema_and_is_explicitly_non_production_scale():
    mod = _load()
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "smoke.sqlite3"
        manifest = mod.build_fixture(db, wallets=4, tokens=6, swaps_per_wallet=3)
        assert manifest["production_scale_claimed"] is False
        assert manifest["purpose"] == "HARNESS_SMOKE_ONLY_NOT_CAUSAL_REPRODUCTION"
        assert manifest["counts"]["wallet_profiles"] == 4
        assert manifest["counts"]["normalized_swaps"] == 12
        assert manifest["counts"]["risk_evidence"] == 6
        assert manifest["counts"]["price_marks"] == 6
        assert manifest["counts"]["program_coverage_observations"] == 6
        with sqlite3.connect(db) as conn:
            names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"events", "wallet_profiles", "normalized_swaps", "token_first_touches", "risk_evidence", "entity_links", "risk_refresh_measurements", "price_marks", "program_coverage_observations"} <= names


def test_smoke_fixture_is_deterministic_at_logical_content_level():
    mod = _load()
    with tempfile.TemporaryDirectory() as td:
        left = Path(td) / "a.sqlite3"
        right = Path(td) / "b.sqlite3"
        mod.build_fixture(left, wallets=3, tokens=5, swaps_per_wallet=2)
        mod.build_fixture(right, wallets=3, tokens=5, swaps_per_wallet=2)
        with sqlite3.connect(left) as a, sqlite3.connect(right) as b:
            for table in ("wallet_profiles", "normalized_swaps", "token_first_touches", "risk_evidence", "price_marks", "program_coverage_observations"):
                assert a.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall() == b.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
