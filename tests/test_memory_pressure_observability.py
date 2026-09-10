from __future__ import annotations

from pathlib import Path

from solana_roi import memory_pressure_observability as memory


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_capture_detail_attributes_full_cgroup_and_process_memory(tmp_path: Path) -> None:
    cgroup = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    cgroup.mkdir()
    proc.mkdir()

    _write(cgroup / "memory.current", "150\n")
    _write(cgroup / "memory.max", "200\n")
    _write(
        cgroup / "memory.stat",
        "anon 40\n"
        "file 60\n"
        "file_mapped 7\n"
        "file_dirty 8\n"
        "file_writeback 9\n"
        "active_anon 11\n"
        "inactive_anon 12\n"
        "active_file 13\n"
        "inactive_file 14\n"
        "shmem 15\n"
        "slab 16\n"
        "slab_reclaimable 17\n"
        "slab_unreclaimable 18\n"
        "kernel 19\n"
        "kernel_stack 20\n"
        "pagetables 21\n"
        "sock 22\n"
        "pgfault 23\n"
        "pgmajfault 24\n"
        "workingset_refault_anon 25\n"
        "workingset_refault_file 26\n"
        "workingset_activate_anon 27\n"
        "workingset_activate_file 28\n",
    )
    _write(cgroup / "memory.events", "high 3\nmax 4\noom 0\noom_kill 0\n")
    _write(cgroup / "memory.pressure", "some avg10=1.50 avg60=0.50 avg300=0.10 total=123\nfull avg10=0.20 avg60=0.10 avg300=0.00 total=45\n")
    _write(cgroup / "pids.current", "9\n")
    _write(cgroup / "pids.max", "512\n")
    _write(cgroup / "pids.events", "max 2\n")
    _write(cgroup / "cgroup.procs", "123\n")
    _write(proc / "123" / "comm", "python\n")
    _write(
        proc / "123" / "status",
        "Name:\tpython\n"
        "VmSize:\t2048 kB\n"
        "VmRSS:\t1000 kB\n"
        "RssAnon:\t700 kB\n"
        "RssFile:\t300 kB\n"
        "Threads:\t7\n",
    )
    _write(
        proc / "123" / "smaps_rollup",
        "Rss: 1000 kB\n"
        "Pss: 800 kB\n"
        "Pss_Anon: 600 kB\n"
        "Pss_File: 200 kB\n"
        "Anonymous: 610 kB\n"
        "Private_Clean: 30 kB\n"
        "Private_Dirty: 500 kB\n",
    )

    detail = memory.capture_detail(cgroup, proc)

    assert detail["memory_current_bytes"] == 150
    assert detail["memory_max_bytes"] == 200
    assert detail["memory_headroom_bytes"] == 50
    assert detail["memory_fraction"] == 0.75
    assert detail["memory_stat"]["file_mapped"] == 7
    assert detail["memory_stat"]["slab_unreclaimable"] == 18
    assert detail["memory_stat"]["workingset_refault_file"] == 26
    assert detail["memory_events"]["max"] == 4
    assert detail["memory_pressure"]["some"]["total"] == 123
    assert detail["pids_current"] == 9
    assert detail["pids_max"] == 512
    assert detail["pids_events"]["max"] == 2
    assert detail["processes"] == [
        {
            "pid": 123,
            "comm": "python",
            "threads": 7,
            "vm_size_kib": 2048,
            "vm_rss_kib": 1000,
            "rss_anon_kib": 700,
            "rss_file_kib": 300,
            "rss_kib": 1000,
            "pss_kib": 800,
            "pss_anon_kib": 600,
            "pss_file_kib": 200,
            "anonymous_kib": 610,
            "private_clean_kib": 30,
            "private_dirty_kib": 500,
        }
    ]
    assert detail["paper_only"] is True
    assert detail["live_money_authority"] is False
    assert detail["signing_available"] is False
    assert detail["transaction_submission_available"] is False


def test_unlimited_pids_max_is_reported_as_none(tmp_path: Path) -> None:
    cgroup = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    cgroup.mkdir()
    proc.mkdir()
    _write(cgroup / "memory.current", "1\n")
    _write(cgroup / "memory.max", "100\n")
    _write(cgroup / "memory.stat", "anon 1\n")
    _write(cgroup / "memory.events", "oom 0\n")
    _write(cgroup / "memory.pressure", "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
    _write(cgroup / "pids.current", "3\n")
    _write(cgroup / "pids.max", "max\n")
    _write(cgroup / "pids.events", "max 0\n")
    _write(cgroup / "cgroup.procs", "")

    detail = memory.capture_detail(cgroup, proc)

    assert detail["pids_current"] == 3
    assert detail["pids_max"] is None
    assert detail["pids_events"]["max"] == 0


def test_process_rows_are_bounded_and_sorted_by_pss(tmp_path: Path) -> None:
    cgroup = tmp_path / "cgroup"
    proc = tmp_path / "proc"
    cgroup.mkdir()
    proc.mkdir()
    _write(cgroup / "memory.current", "1\n")
    _write(cgroup / "memory.max", "100\n")
    _write(cgroup / "memory.stat", "anon 1\n")
    _write(cgroup / "memory.events", "oom 0\n")
    _write(cgroup / "memory.pressure", "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
    _write(cgroup / "pids.current", "21\n")
    _write(cgroup / "pids.max", "512\n")
    _write(cgroup / "pids.events", "max 0\n")

    pids = list(range(1, memory.MAX_PROCESS_ROWS + 6))
    _write(cgroup / "cgroup.procs", "\n".join(str(pid) for pid in pids) + "\n")
    for pid in pids:
        _write(proc / str(pid) / "comm", f"p{pid}\n")
        _write(proc / str(pid) / "status", f"VmSize:\t{pid * 2} kB\nVmRSS:\t{pid} kB\nThreads:\t1\n")
        _write(proc / str(pid) / "smaps_rollup", f"Rss: {pid} kB\nPss: {pid} kB\n")

    rows = memory.capture_detail(cgroup, proc)["processes"]

    assert len(rows) == memory.MAX_PROCESS_ROWS
    assert rows[0]["pid"] == max(pids)
    assert rows[-1]["pid"] == sorted(pids, reverse=True)[memory.MAX_PROCESS_ROWS - 1]
    assert all(row["threads"] == 1 for row in rows)
