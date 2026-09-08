from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_proof_snapshot_orphan_cleanup as cleanup


@pytest.fixture(autouse=True)
def _reset_cleanup_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleanup, "_COMPLETED", False)
    monkeypatch.setattr(cleanup, "_SCHEDULED", False)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-test")
    monkeypatch.setenv(cleanup._EXPECTED_RELEASE_ENV, "release-test")
    monkeypatch.delenv(cleanup._ENABLED_ENV, raising=False)


def _old(path: Path, payload: bytes = b"orphan") -> Path:
    path.write_bytes(payload)
    stamp = time.time() - cleanup._MIN_AGE_SECONDS - 10.0
    os.utime(path, (stamp, stamp))
    return path


def test_exact_historical_base_and_standalone_companions_are_deleted(tmp_path: Path) -> None:
    names = [
        ".robinhood-proof-snapshot-ab12_cd3.sqlite3",
        ".robinhood-proof-snapshot-0opk0gmm.sqlite3-wal",
        ".robinhood-proof-snapshot-_4cgl358.sqlite3-shm",
        ".robinhood-proof-snapshot-9m5in_bb.sqlite3-journal",
    ]
    total = 0
    for index, name in enumerate(names, start=1):
        payload = b"x" * index
        total += len(payload)
        _old(tmp_path / name, payload)

    result = cleanup._run_cleanup_once(root=tmp_path)

    assert result["status"] == "completed"
    assert result["deleted_files"] == 4
    assert result["deleted_bytes"] == total
    assert all(not (tmp_path / name).exists() for name in names)
    assert result["sqlite_opened"] is False
    assert result["file_contents_read"] is False
    assert result["canonical_databases_touched"] is False
    assert result["active_snapshot_touched"] is False


def test_active_work_path_canonical_databases_and_near_misses_are_untouched(tmp_path: Path) -> None:
    protected = [
        ".robinhood-proof-snapshot-active-solana-roi-robinhood-chain.sqlite3",
        "solana-roi-robinhood-chain.sqlite3",
        "solana-roi.sqlite3",
        ".robinhood-proof-snapshot-abcdefg.sqlite3",
        ".robinhood-proof-snapshot-abcdefghi.sqlite3",
        ".robinhood-proof-snapshot-ABCDEFGH.sqlite3",
        ".robinhood-proof-snapshot-abcdefgh.sqlite3-backup",
    ]
    for name in protected:
        _old(tmp_path / name, b"keep")

    result = cleanup._run_cleanup_once(root=tmp_path)

    assert result["status"] == "completed"
    assert result["deleted_files"] == 0
    for name in protected:
        assert (tmp_path / name).read_bytes() == b"keep"


def test_fresh_historical_match_is_not_deleted(tmp_path: Path) -> None:
    path = tmp_path / ".robinhood-proof-snapshot-abcdefgh.sqlite3"
    path.write_bytes(b"fresh")

    result = cleanup._run_cleanup_once(root=tmp_path, now=time.time())

    assert result["status"] == "completed"
    assert result["too_young"] == 1
    assert result["deleted_files"] == 0
    assert path.exists()


def test_release_mismatch_refuses_without_deleting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    orphan = _old(tmp_path / ".robinhood-proof-snapshot-abcdefgh.sqlite3")
    monkeypatch.setenv(cleanup._EXPECTED_RELEASE_ENV, "other-release")

    result = cleanup._run_cleanup_once(root=tmp_path)

    assert result["status"] == "refused_release_mismatch"
    assert result["deleted_files"] == 0
    assert orphan.exists()


def test_exact_match_symlink_fails_closed_and_preserves_target(tmp_path: Path) -> None:
    target = _old(tmp_path / "target.sqlite3", b"target")
    link = tmp_path / ".robinhood-proof-snapshot-abcdefgh.sqlite3"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink unsupported")

    result = cleanup._run_cleanup_once(root=tmp_path)

    assert result["status"] == "refused_unsafe_historical_match_type"
    assert result["deleted_files"] == 0
    assert link.is_symlink()
    assert target.read_bytes() == b"target"


def test_exact_match_directory_fails_closed(tmp_path: Path) -> None:
    directory = tmp_path / ".robinhood-proof-snapshot-abcdefgh.sqlite3"
    directory.mkdir()

    result = cleanup._run_cleanup_once(root=tmp_path)

    assert result["status"] == "refused_unsafe_historical_match_type"
    assert result["deleted_files"] == 0
    assert directory.is_dir()


def test_symlink_root_is_refused(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    _old(actual / ".robinhood-proof-snapshot-abcdefgh.sqlite3")
    root = tmp_path / "root"
    try:
        root.symlink_to(actual, target_is_directory=True)
    except OSError:
        pytest.skip("symlink unsupported")

    result = cleanup._run_cleanup_once(root=root)

    assert result["status"] == "refused_root_is_symlink"
    assert (actual / ".robinhood-proof-snapshot-abcdefgh.sqlite3").exists()


def test_disabled_installer_never_creates_timer(monkeypatch: pytest.MonkeyPatch) -> None:
    class ForbiddenTimer:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("timer must not be created while cleanup is disabled")

    monkeypatch.setattr(cleanup.threading, "Timer", ForbiddenTimer)
    app = SimpleNamespace(state=SimpleNamespace(roi_robinhood_proof_snapshot_lifecycle_repair=True))

    cleanup.install_robinhood_proof_snapshot_orphan_cleanup(app)

    assert app.state.roi_robinhood_proof_snapshot_orphan_cleanup_enabled is False


def test_enabled_installer_requires_lifecycle_and_exact_release(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(cleanup._ENABLED_ENV, "true")
    started: list[bool] = []

    class FakeTimer:
        daemon = False
        name = ""

        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def start(self) -> None:
            started.append(True)

    monkeypatch.setattr(cleanup.threading, "Timer", FakeTimer)
    app = SimpleNamespace(state=SimpleNamespace(roi_robinhood_proof_snapshot_lifecycle_repair=True))

    cleanup.install_robinhood_proof_snapshot_orphan_cleanup(app)

    assert started == [True]
    assert app.state.roi_robinhood_proof_snapshot_orphan_cleanup_enabled is True


def test_candidate_limit_fails_closed_before_any_unlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleanup, "_MAX_CANDIDATES", 2)
    names = [
        ".robinhood-proof-snapshot-aaaaaaa1.sqlite3",
        ".robinhood-proof-snapshot-aaaaaaa2.sqlite3",
        ".robinhood-proof-snapshot-aaaaaaa3.sqlite3",
    ]
    for name in names:
        _old(tmp_path / name)

    result = cleanup._run_cleanup_once(root=tmp_path)

    assert result["status"] == "refused_candidate_limit_exceeded"
    assert result["deleted_files"] == 0
    assert all((tmp_path / name).exists() for name in names)


def test_authority_boundaries_remain_paper_only() -> None:
    assert cleanup.PAPER_ONLY is True
    assert cleanup.LIVE_MONEY_AUTHORITY is False
    assert cleanup.SIGNING_AVAILABLE is False
    assert cleanup.TRANSACTION_SUBMISSION_AVAILABLE is False


def test_batch9_finalizer_composes_cleanup_after_lifecycle_repair() -> None:
    from solana_roi import batch9_finalization_repair

    source = Path(batch9_finalization_repair.__file__).read_text(encoding="utf-8")
    assert "robinhood_proof_snapshot_orphan_cleanup as proof_orphan_cleanup" in source
    lifecycle = "proof_snapshot_lifecycle.install_robinhood_proof_snapshot_lifecycle_repair(app)"
    orphan = "proof_orphan_cleanup.install_robinhood_proof_snapshot_orphan_cleanup(app)"
    assert lifecycle in source
    assert orphan in source
    assert source.index(lifecycle) < source.index(orphan)
