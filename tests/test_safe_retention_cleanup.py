from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI

from solana_roi import safe_retention_cleanup as cleanup


def _make_old(path: Path, *, age_seconds: float = 7200.0) -> None:
    path.write_bytes(b"sqlite-export-placeholder")
    old = time.time() - age_seconds
    os.utime(path, (old, old))


def test_removes_only_old_regular_unopened_export(tmp_path: Path) -> None:
    old_export = tmp_path / ".certification-export-old.sqlite3"
    young_export = tmp_path / ".certification-export-young.sqlite3"
    unrelated = tmp_path / "paper-history.sqlite3"
    _make_old(old_export)
    young_export.write_bytes(b"young")
    _make_old(unrelated)

    result = cleanup.cleanup_stale_certification_exports(tmp_path / "solana-roi.sqlite3")

    assert result["removed"] == 1
    assert not old_export.exists()
    assert young_export.exists()
    assert unrelated.exists()
    assert result["bounded"] is True


def test_preserves_symlink_even_when_name_and_age_match(tmp_path: Path) -> None:
    target = tmp_path / "real.sqlite3"
    _make_old(target)
    link = tmp_path / ".certification-export-link.sqlite3"
    link.symlink_to(target)

    result = cleanup.cleanup_stale_certification_exports(tmp_path / "solana-roi.sqlite3", now=time.time() + 7200.0)

    assert result["removed"] == 0
    assert link.is_symlink()
    assert target.exists()
    assert result["outcomes"].get("symlink") == 1


def test_preserves_export_already_open_by_process(tmp_path: Path) -> None:
    candidate = tmp_path / ".certification-export-open.sqlite3"
    _make_old(candidate)

    with candidate.open("rb"):
        result = cleanup.cleanup_stale_certification_exports(tmp_path / "solana-roi.sqlite3")

    assert result["removed"] == 0
    assert candidate.exists()
    assert result["outcomes"].get("open_by_process") == 1


def test_preserves_empty_incomplete_export(tmp_path: Path) -> None:
    candidate = tmp_path / ".certification-export-empty.sqlite3"
    candidate.touch()
    old = time.time() - 7200.0
    os.utime(candidate, (old, old))

    result = cleanup.cleanup_stale_certification_exports(tmp_path / "solana-roi.sqlite3")

    assert result["removed"] == 0
    assert candidate.exists()
    assert result["outcomes"].get("empty_or_incomplete") == 1


def test_install_exposes_read_only_scope_and_does_not_claim_ambiguous_deletion(tmp_path: Path) -> None:
    candidate = tmp_path / ".certification-export-old.sqlite3"
    _make_old(candidate)
    app = FastAPI()
    runtime = SimpleNamespace(store=SimpleNamespace(path=tmp_path / "solana-roi.sqlite3"))

    state = cleanup.install_safe_retention_cleanup(app, runtime)

    assert state["startup_stale_export_cleanup"]["removed"] == 1
    assert state["scope"] == ["stale_certification_exports"]
    assert state["candidate_selection_changed"] is False
    assert state["provenance_ambiguous_data_deleted"] is False
    assert state["replication_journal_deleted"] is False
    assert state["robinhood_history_deleted"] is False
    assert state["event_ledger_deleted"] is False
    assert state["wallet_history_deleted"] is False
    assert state["strategy_thresholds_changed"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert any(getattr(route, "path", None) == "/v1/operations/safe-retention-cleanup" for route in app.routes)
