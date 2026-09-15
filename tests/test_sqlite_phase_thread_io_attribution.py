from __future__ import annotations

from pathlib import Path

from solana_roi import sqlite_phase_observability as phase


def test_read_proc_io_parses_linux_io_file(tmp_path: Path) -> None:
    path = tmp_path / "io"
    path.write_text(
        "rchar: 101\n"
        "wchar: 202\n"
        "syscr: 3\n"
        "syscw: 4\n"
        "read_bytes: 505\n"
        "write_bytes: 606\n"
        "cancelled_write_bytes: 7\n",
        encoding="utf-8",
    )

    assert phase._read_proc_io(path) == {
        "rchar": 101,
        "wchar": 202,
        "syscr": 3,
        "syscw": 4,
        "read_bytes": 505,
        "write_bytes": 606,
        "cancelled_write_bytes": 7,
    }


def test_pressure_reader_extracts_cumulative_some_and_full_totals(tmp_path: Path) -> None:
    path = tmp_path / "memory.pressure"
    path.write_text(
        "some avg10=1.00 avg60=2.00 avg300=3.00 total=12345\n"
        "full avg10=0.10 avg60=0.20 avg300=0.30 total=678\n",
        encoding="utf-8",
    )

    assert phase._read_pressure_totals(path) == {"some": 12345, "full": 678}


def test_resource_snapshot_keeps_process_and_thread_io_separate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test.sqlite3"
    db_path.write_bytes(b"")
    monkeypatch.setattr(phase, "_db_path", lambda store: db_path)
    monkeypatch.setattr(
        phase,
        "_proc_io",
        lambda: {
            "rchar": 1000,
            "wchar": 2000,
            "syscr": 30,
            "syscw": 40,
            "read_bytes": 5000,
            "write_bytes": 6000,
            "cancelled_write_bytes": 70,
        },
    )
    monkeypatch.setattr(
        phase,
        "_thread_io",
        lambda: {
            "rchar": 101,
            "wchar": 202,
            "syscr": 3,
            "syscw": 4,
            "read_bytes": 505,
            "write_bytes": 606,
            "cancelled_write_bytes": 7,
        },
    )

    snapshot = phase.resource_snapshot()

    assert snapshot["proc_rchar"] == 1000
    assert snapshot["proc_write_bytes"] == 6000
    assert snapshot["thread_rchar"] == 101
    assert snapshot["thread_write_bytes"] == 606
    assert snapshot["thread_cancelled_write_bytes"] == 7


def test_numeric_delta_preserves_independent_thread_attribution() -> None:
    before = {
        "proc_rchar": 1_000,
        "thread_rchar": 100,
        "proc_write_bytes": 2_000,
        "thread_write_bytes": 200,
    }
    after = {
        "proc_rchar": 9_000,
        "thread_rchar": 160,
        "proc_write_bytes": 12_000,
        "thread_write_bytes": 240,
    }

    delta = phase._numeric_delta(before, after)

    assert delta["proc_rchar"] == 8_000
    assert delta["thread_rchar"] == 60
    assert delta["proc_write_bytes"] == 10_000
    assert delta["thread_write_bytes"] == 40


def test_sync_phase_declares_thread_io_as_causal_signal(monkeypatch) -> None:
    snapshots = iter(
        [
            {"proc_rchar": 1_000, "thread_rchar": 100},
            {"proc_rchar": 9_000, "thread_rchar": 160},
        ]
    )
    emitted: dict[str, object] = {}
    monkeypatch.setattr(phase, "resource_snapshot", lambda store=None: next(snapshots))

    def capture(name, *, before, after, duration_ms, detail=None):
        emitted["name"] = name
        emitted["before"] = before
        emitted["after"] = after
        emitted["detail"] = detail

    monkeypatch.setattr(phase, "emit_phase", capture)

    wrapped = phase._sync_phase("test-phase", lambda self: (0, 0))
    assert wrapped(object()) == (0, 0)

    assert emitted["name"] == "test-phase"
    assert emitted["detail"]["queue_rows"] == 0
    assert emitted["detail"]["metric_rows"] == 0
    assert (
        emitted["detail"]["io_attribution"]
        == "thread_exact_for_sync_phase;process_concurrent_context_only"
    )
