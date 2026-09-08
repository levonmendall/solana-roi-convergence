from __future__ import annotations

from pathlib import Path

from solana_roi import robinhood_proof_snapshot_lifecycle_repair as repair


def _companions(path: Path) -> tuple[Path, ...]:
    return tuple(Path(str(path) + suffix) for suffix in ("", "-wal", "-shm", "-journal"))


def test_work_snapshot_path_is_deterministic_per_store(tmp_path: Path) -> None:
    live_a = tmp_path / "robinhood-a.sqlite3"
    live_b = tmp_path / "robinhood-b.sqlite3"

    first = repair._work_snapshot_path(str(live_a))
    second = repair._work_snapshot_path(str(live_a))
    other = repair._work_snapshot_path(str(live_b))

    assert first == second
    assert first != other
    assert first.parent == live_a.parent
    assert first.name == ".robinhood-proof-snapshot-active-robinhood-a.sqlite3"
    assert first != live_a


def test_next_refresh_strictly_reclaims_crash_residue_without_touching_canonical(
    tmp_path: Path,
) -> None:
    live = tmp_path / "robinhood.sqlite3"
    live.write_bytes(b"canonical-evidence")
    work = repair._work_snapshot_path(str(live))
    for index, candidate in enumerate(_companions(work)):
        candidate.write_bytes(f"stale-{index}".encode())

    resolved = repair._crash_bounded_snapshot_path(str(live))

    assert resolved == work
    assert live.read_bytes() == b"canonical-evidence"
    assert all(not candidate.exists() for candidate in _companions(work))


def test_terminal_cleanup_is_exact_and_includes_sqlite_journal(tmp_path: Path) -> None:
    live = tmp_path / "robinhood.sqlite3"
    unrelated = tmp_path / ".robinhood-proof-snapshot-old-orphan.sqlite3"
    unrelated.write_bytes(b"historical-orphan")
    work = repair._work_snapshot_path(str(live))
    for candidate in _companions(work):
        candidate.write_bytes(b"work")

    repair._cleanup_work_snapshot(work)

    assert all(not candidate.exists() for candidate in _companions(work))
    assert unrelated.read_bytes() == b"historical-orphan"


def test_crash_bounded_repair_preserves_authority_boundaries() -> None:
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False


def test_batch9_finalizer_composes_snapshot_lifecycle_repair() -> None:
    from solana_roi import batch9_finalization_repair

    source = Path(batch9_finalization_repair.__file__).read_text(encoding="utf-8")
    assert "robinhood_proof_snapshot_lifecycle_repair as proof_snapshot_lifecycle" in source
    assert "proof_snapshot_lifecycle.install_robinhood_proof_snapshot_lifecycle_repair(app)" in source
