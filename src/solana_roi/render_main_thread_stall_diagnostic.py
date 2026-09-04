from __future__ import annotations

import faulthandler
import os
import sys
import threading
import time
import traceback
from types import FrameType
from typing import Any


DIAGNOSTIC_VERSION = "render-main-thread-stall-diagnostic-v1"
SAMPLE_SECONDS = 0.25
STALL_SECONDS = 2.5
STARTUP_GRACE_SECONDS = 15.0
DUMP_COOLDOWN_SECONDS = 10.0

_THREAD: threading.Thread | None = None
_STOP = threading.Event()
_INSTALLED = False


def _render_runtime() -> bool:
    return bool(os.getenv("RENDER_GIT_COMMIT", "").strip())


def _stack(frame: FrameType) -> list[traceback.FrameSummary]:
    try:
        return traceback.extract_stack(frame)
    except Exception:
        return []


def _looks_idle(stack: list[traceback.FrameSummary]) -> bool:
    """Recognize the normal asyncio selector wait as healthy loop idleness."""

    for row in stack[-8:]:
        filename = str(row.filename).replace("\\", "/")
        function = str(row.name)
        if filename.endswith("/selectors.py") and function == "select":
            return True
        if filename.endswith("/asyncio/selector_events.py") and function in {"_read_ready", "_write_ready"}:
            return False
    return False


def _compact_top(stack: list[traceback.FrameSummary]) -> str:
    rows: list[str] = []
    for item in stack[-8:]:
        rows.append(f"{item.filename}:{item.lineno}:{item.name}")
    return " <- ".join(rows)


def _dump_stall(*, busy_seconds: float, stack: list[traceback.FrameSummary]) -> None:
    header = (
        f"ROI_MAIN_THREAD_STALL diagnostic_version={DIAGNOSTIC_VERSION} "
        f"busy_seconds={busy_seconds:.3f} top={_compact_top(stack)}"
    )
    try:
        print(header, file=sys.stderr, flush=True)
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        sys.stderr.flush()
    except Exception as exc:
        try:
            print(
                f"ROI_MAIN_THREAD_STALL_DUMP_FAILED type={type(exc).__name__}",
                file=sys.stderr,
                flush=True,
            )
        except Exception:
            pass


def _watch_main_thread() -> None:
    main_ident = threading.main_thread().ident
    if main_ident is None:
        return

    started = time.monotonic()
    busy_started: float | None = None
    last_dump = 0.0

    while not _STOP.wait(SAMPLE_SECONDS):
        now = time.monotonic()
        if now - started < STARTUP_GRACE_SECONDS:
            continue

        frame = sys._current_frames().get(main_ident)
        if frame is None:
            busy_started = None
            continue
        stack = _stack(frame)
        if not stack or _looks_idle(stack):
            busy_started = None
            continue

        if busy_started is None:
            busy_started = now
            continue
        busy_seconds = max(0.0, now - busy_started)
        if busy_seconds < STALL_SECONDS:
            continue
        if now - last_dump < DUMP_COOLDOWN_SECONDS:
            continue

        last_dump = now
        _dump_stall(busy_seconds=busy_seconds, stack=stack)


def install_render_main_thread_stall_diagnostic() -> bool:
    """Start a zero-authority watchdog only on actual Render releases.

    The watchdog samples the Python main thread from a daemon thread. It does not
    acquire the canonical SQLite lock, make RPC calls, alter worker scheduling, or
    authorize any paper/live action. If the main thread fails to return to the normal
    asyncio selector wait for 2.5 seconds, it emits all Python thread stacks to
    stderr so the next Render health failure identifies the exact blocking path.
    """

    global _THREAD, _INSTALLED
    if _INSTALLED:
        return bool(_THREAD is not None)
    _INSTALLED = True
    if not _render_runtime():
        return False

    thread = threading.Thread(
        target=_watch_main_thread,
        name="render-main-thread-stall-diagnostic",
        daemon=True,
    )
    _THREAD = thread
    thread.start()
    return True


def diagnostic_status() -> dict[str, Any]:
    thread = _THREAD
    return {
        "version": DIAGNOSTIC_VERSION,
        "render_runtime": _render_runtime(),
        "installed": _INSTALLED,
        "thread_alive": bool(thread is not None and thread.is_alive()),
        "sample_seconds": SAMPLE_SECONDS,
        "stall_seconds": STALL_SECONDS,
        "startup_grace_seconds": STARTUP_GRACE_SECONDS,
        "dump_cooldown_seconds": DUMP_COOLDOWN_SECONDS,
        "read_only": True,
        "strategy_changed": False,
        "paper_authority_changed": False,
        "live_money_authority": False,
    }


__all__ = [
    "DIAGNOSTIC_VERSION",
    "diagnostic_status",
    "install_render_main_thread_stall_diagnostic",
]
