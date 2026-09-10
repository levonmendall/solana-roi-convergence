from __future__ import annotations

import subprocess
import threading

import pytest

from solana_roi import certification_logical_bootstrap_client as logical_bootstrap
from solana_roi import certifier_service


class _FakeProcess:
    def __init__(self, *, pid: int = 4321, returncode: int = 0, wait_timeouts: int = 0) -> None:
        self.pid = pid
        self.returncode = None
        self._terminal_returncode = returncode
        self._wait_timeouts = wait_timeouts
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait_calls = 0

    def poll(self):
        return self.returncode

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1

    def wait(self, timeout=None):
        _ = timeout
        self.wait_calls += 1
        if self._wait_timeouts > 0:
            self._wait_timeouts -= 1
            raise subprocess.TimeoutExpired(cmd="certifier", timeout=timeout or 0)
        self.returncode = self._terminal_returncode
        return self.returncode


def _reset_child_state(pid: int | None = None) -> None:
    with certifier_service._LOCK:
        certifier_service._STATE.update(
            {
                "active_child_pid": pid,
                "last_child_pid": pid,
                "last_child_returncode": None,
                "child_launches": 0,
                "child_reaps": 0,
                "child_terminate_requests": 0,
                "child_forced_kills": 0,
                "child_reap_failures": 0,
                "cycle_overlap_rejections": 0,
            }
        )


def test_child_environment_bounds_native_thread_fanout(tmp_path, monkeypatch) -> None:
    snapshot = tmp_path / "snapshot.sqlite3"
    for key in certifier_service._NATIVE_THREAD_ENV_KEYS:
        monkeypatch.setenv(key, "64")
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME", "true")

    env = certifier_service._child_environment(snapshot, "exact-release")

    assert env["SOLANA_ROI_DB_PATH"] == str(snapshot)
    assert env["SOLANA_ROI_RELEASE_COMMIT"] == "exact-release"
    assert "SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME" not in env
    for key in certifier_service._NATIVE_THREAD_ENV_KEYS:
        assert env[key] == "1"


def test_reap_child_waits_before_clearing_active_pid() -> None:
    process = _FakeProcess(pid=7001, returncode=0)
    _reset_child_state(process.pid)

    returncode = certifier_service._reap_child(process)

    assert returncode == 0
    assert process.wait_calls == 1
    with certifier_service._LOCK:
        assert certifier_service._STATE["active_child_pid"] is None
        assert certifier_service._STATE["child_reaps"] == 1
        assert certifier_service._STATE["last_child_returncode"] == 0
        assert certifier_service._STATE["child_forced_kills"] == 0


def test_reap_child_terminate_escalates_to_kill_and_still_waits() -> None:
    process = _FakeProcess(pid=7002, returncode=-9, wait_timeouts=1)
    _reset_child_state(process.pid)

    returncode = certifier_service._reap_child(process, terminate_first=True)

    assert returncode == -9
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert process.wait_calls == 2
    with certifier_service._LOCK:
        assert certifier_service._STATE["active_child_pid"] is None
        assert certifier_service._STATE["child_reaps"] == 1
        assert certifier_service._STATE["child_terminate_requests"] == 1
        assert certifier_service._STATE["child_forced_kills"] == 1


def test_reap_child_timeout_kill_is_waited_before_pid_clear() -> None:
    process = _FakeProcess(pid=7003, returncode=-9)
    _reset_child_state(process.pid)

    returncode = certifier_service._reap_child(process, kill_first=True)

    assert returncode == -9
    assert process.kill_calls == 1
    assert process.wait_calls == 1
    with certifier_service._LOCK:
        assert certifier_service._STATE["active_child_pid"] is None
        assert certifier_service._STATE["child_reaps"] == 1
        assert certifier_service._STATE["child_forced_kills"] == 1


def test_cycle_single_flight_rejects_duplicate_without_starting_replica_sync(monkeypatch) -> None:
    _reset_child_state()
    called = False

    def forbidden_sync(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("overlapping cycle must not reach replica synchronization")

    monkeypatch.setattr(certifier_service, "synchronize_replica", forbidden_sync)
    acquired = certifier_service._CYCLE_SINGLE_FLIGHT.acquire(blocking=False)
    assert acquired is True
    try:
        assert certifier_service._run_cycle_sync(threading.Event()) is False
    finally:
        certifier_service._CYCLE_SINGLE_FLIGHT.release()

    assert called is False
    with certifier_service._LOCK:
        assert certifier_service._STATE["cycle_overlap_rejections"] == 1


def test_logical_bootstrap_transient_retries_are_serial_on_one_thread(monkeypatch) -> None:
    caller_tid = threading.get_ident()
    attempts: list[int] = []
    sleeps: list[float] = []

    def always_pause(request, *, timeout=30.0):
        _ = request, timeout
        attempts.append(threading.get_ident())
        raise logical_bootstrap.LogicalBootstrapPause("transient")

    monkeypatch.setattr(logical_bootstrap, "_open_json", always_pause)
    monkeypatch.setattr(logical_bootstrap, "_transient_retry_attempts", lambda: 4)
    monkeypatch.setattr(logical_bootstrap, "_transient_retry_seconds", lambda: 0.25)
    monkeypatch.setattr(logical_bootstrap.time, "sleep", lambda seconds: sleeps.append(seconds))

    with pytest.raises(logical_bootstrap.LogicalBootstrapPause):
        logical_bootstrap._open_json_resumable(object(), timeout=1.0)

    assert attempts == [caller_tid, caller_tid, caller_tid, caller_tid]
    assert sleeps == [0.25, 0.5, 0.75]


def test_health_exposes_child_lifecycle_guards_without_authority_change() -> None:
    payload = certifier_service.health()
    integrity = payload["snapshot_transfer_integrity"]

    assert integrity["child_output_uses_bounded_file_tail_not_pipes"] is True
    assert integrity["child_native_thread_limit"] == 1
    assert integrity["cycle_single_flight"] is True
    assert integrity["child_reap_required_before_pid_clear"] is True
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
