from __future__ import annotations

from solana_roi import v51_resource_pressure as pressure


MIB = 1024 * 1024


def _write(path, value: str) -> None:
    path.write_text(value, encoding="utf-8")


def test_raw_sample_subtracts_reclaimable_inactive_file(tmp_path, monkeypatch) -> None:
    _write(tmp_path / "memory.current", str(900 * MIB))
    _write(tmp_path / "memory.max", str(1000 * MIB))
    _write(tmp_path / "memory.stat", f"anon {200 * MIB}\ninactive_file {300 * MIB}\n")
    _write(tmp_path / "memory.events", "oom 0\noom_kill 0\n")
    monkeypatch.setattr(pressure, "_proc_rss_bytes", lambda: 123 * MIB)

    sample = pressure._raw_sample(cgroup_root=tmp_path)

    assert sample["memory_current_bytes"] == 900 * MIB
    assert sample["memory_inactive_file_bytes"] == 300 * MIB
    assert sample["memory_working_set_bytes"] == 600 * MIB
    assert sample["process_rss_bytes"] == 123 * MIB


def test_working_set_falls_back_to_raw_current_without_valid_memory_stat(tmp_path) -> None:
    _write(tmp_path / "memory.current", str(900 * MIB))
    _write(tmp_path / "memory.max", str(1000 * MIB))
    _write(tmp_path / "memory.stat", "inactive_file not-an-integer\n")

    sample = pressure._raw_sample(cgroup_root=tmp_path)

    assert sample["memory_current_bytes"] == 900 * MIB
    assert sample["memory_inactive_file_bytes"] is None
    assert sample["memory_working_set_bytes"] == 900 * MIB


def test_working_set_supports_total_inactive_file_and_clamps_at_zero(tmp_path) -> None:
    _write(tmp_path / "memory.current", str(100 * MIB))
    _write(tmp_path / "memory.max", str(1000 * MIB))
    _write(tmp_path / "memory.stat", f"total_inactive_file {150 * MIB}\n")

    sample = pressure._raw_sample(cgroup_root=tmp_path)

    assert sample["memory_inactive_file_bytes"] == 150 * MIB
    assert sample["memory_working_set_bytes"] == 0


def test_memory_growth_trend_uses_working_set_not_raw_cgroup_charge() -> None:
    samples = [
        {
            "monotonic": 100.0,
            "memory_current_bytes": 500 * MIB,
            "memory_working_set_bytes": 400 * MIB,
        },
        {
            "monotonic": 400.0,
            "memory_current_bytes": 900 * MIB,
            "memory_working_set_bytes": 450 * MIB,
        },
    ]

    trend = pressure._trend(samples)

    assert trend["trend_window_sufficient"] is True
    assert trend["memory_growth_bytes_per_minute"] == 10 * MIB
    # Raw cgroup growth would be 80 MiB/min and would incorrectly cross the
    # warning threshold. The working-set trend correctly remains below it.
    assert trend["memory_growth_bytes_per_minute"] < pressure.MEMORY_GROWTH_WARN_BYTES_PER_MINUTE


def test_snapshot_guard_uses_working_set_while_preserving_raw_telemetry(monkeypatch) -> None:
    latest = {
        "sampled_at": "2026-09-09T00:00:00+00:00",
        "monotonic": 1.0,
        "memory_current_bytes": 950 * MIB,
        "memory_working_set_bytes": 600 * MIB,
        "memory_inactive_file_bytes": 350 * MIB,
        "memory_max_bytes": 1000 * MIB,
        "process_rss_bytes": 500 * MIB,
        "memory_events": {"oom": 0, "oom_kill": 0},
        "cpu_usage_usec": None,
        "cpu_throttled_usec": None,
        "cpu_nr_throttled": None,
        "cpu_limit_cores": None,
        "load_average": None,
    }
    monkeypatch.setattr(pressure, "sample_resource_pressure", lambda: dict(latest))
    with pressure._lock:
        pressure._samples.clear()

    snapshot = pressure.resource_pressure_snapshot()

    assert snapshot["state"] == "healthy"
    assert snapshot["memory"]["current_bytes"] == 950 * MIB
    assert snapshot["memory"]["working_set_bytes"] == 600 * MIB
    assert snapshot["memory"]["inactive_file_bytes"] == 350 * MIB
    assert snapshot["memory"]["utilization_fraction"] == 0.6
    assert snapshot["memory"]["utilization_basis"] == "working_set_excluding_inactive_file"
    assert "memory_utilization_critical" not in snapshot["critical"]


def test_snapshot_retains_fail_safe_raw_usage_fallback(monkeypatch) -> None:
    latest = {
        "sampled_at": "2026-09-09T00:00:00+00:00",
        "monotonic": 1.0,
        "memory_current_bytes": 950 * MIB,
        "memory_working_set_bytes": 950 * MIB,
        "memory_inactive_file_bytes": None,
        "memory_max_bytes": 1000 * MIB,
        "process_rss_bytes": 500 * MIB,
        "memory_events": {"oom": 0, "oom_kill": 0},
        "cpu_usage_usec": None,
        "cpu_throttled_usec": None,
        "cpu_nr_throttled": None,
        "cpu_limit_cores": None,
        "load_average": None,
    }
    monkeypatch.setattr(pressure, "sample_resource_pressure", lambda: dict(latest))
    with pressure._lock:
        pressure._samples.clear()

    snapshot = pressure.resource_pressure_snapshot()

    assert snapshot["state"] == "critical"
    assert snapshot["memory"]["utilization_fraction"] == 0.95
    assert snapshot["memory"]["utilization_basis"] == "raw_cgroup_usage_fallback"
    assert "memory_utilization_critical" in snapshot["critical"]


def test_oom_signal_remains_critical_even_when_working_set_is_low(monkeypatch) -> None:
    latest = {
        "sampled_at": "2026-09-09T00:00:00+00:00",
        "monotonic": 1.0,
        "memory_current_bytes": 900 * MIB,
        "memory_working_set_bytes": 400 * MIB,
        "memory_inactive_file_bytes": 500 * MIB,
        "memory_max_bytes": 1000 * MIB,
        "process_rss_bytes": 350 * MIB,
        "memory_events": {"oom": 1, "oom_kill": 0},
        "cpu_usage_usec": None,
        "cpu_throttled_usec": None,
        "cpu_nr_throttled": None,
        "cpu_limit_cores": None,
        "load_average": None,
    }
    monkeypatch.setattr(pressure, "sample_resource_pressure", lambda: dict(latest))
    with pressure._lock:
        pressure._samples.clear()

    snapshot = pressure.resource_pressure_snapshot()

    assert snapshot["state"] == "critical"
    assert "cgroup_oom_observed" in snapshot["critical"]
