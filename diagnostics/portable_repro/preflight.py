#!/usr/bin/env python3
"""Fail-closed cgroup-v2 preflight for the portable 2 GiB reproduction harness.

This module is deliberately stdlib-only and MUST run before importing
``solana_roi.production``. It validates the memory boundary and emits a JSON
record that can be preserved with reproduction evidence.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

TARGET_BYTES = 2 * 1024**3
DEFAULT_TOLERANCE_BYTES = 16 * 1024**2


class PreflightError(RuntimeError):
    pass


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise PreflightError(f"cannot read {path}: {exc}") from exc


def _parse_int(value: str, *, field: str) -> int:
    if value == "max":
        raise PreflightError(f"{field} is unlimited ('max')")
    try:
        return int(value)
    except ValueError as exc:
        raise PreflightError(f"{field} is not an integer: {value!r}") from exc


def _proc_cmdline(pid: int, proc_root: Path) -> str:
    path = proc_root / str(pid) / "cmdline"
    try:
        raw = path.read_bytes()
    except OSError:
        return "<unreadable>"
    return raw.replace(b"\x00", b" ").decode("utf-8", "replace").strip()


def inspect_boundary(
    cgroup_root: Path,
    *,
    proc_root: Path = Path("/proc"),
    target_bytes: int = TARGET_BYTES,
    tolerance_bytes: int = DEFAULT_TOLERANCE_BYTES,
) -> dict[str, Any]:
    controllers = cgroup_root / "cgroup.controllers"
    if not controllers.exists():
        raise PreflightError("cgroup v2 not detected at requested cgroup root")

    memory_max = _parse_int(_read_text(cgroup_root / "memory.max"), field="memory.max")
    lower = target_bytes - tolerance_bytes
    upper = target_bytes + tolerance_bytes
    if not lower <= memory_max <= upper:
        raise PreflightError(
            f"memory.max={memory_max} outside required 2 GiB window [{lower},{upper}]"
        )

    pids = []
    for raw in _read_text(cgroup_root / "cgroup.procs").splitlines():
        raw = raw.strip()
        if raw:
            pids.append(int(raw))
    if not pids:
        raise PreflightError("cgroup.procs is empty; isolation cannot be proven")

    current = int(_read_text(cgroup_root / "memory.current"))
    events = {}
    for line in _read_text(cgroup_root / "memory.events").splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                events[parts[0]] = int(parts[1])
            except ValueError:
                pass

    members = [
        {"pid": pid, "cmdline": _proc_cmdline(pid, proc_root)}
        for pid in sorted(set(pids))
    ]
    return {
        "cgroup_root": str(cgroup_root),
        "memory_max": memory_max,
        "memory_current": current,
        "target_bytes": target_bytes,
        "tolerance_bytes": tolerance_bytes,
        "member_count": len(members),
        "members": members,
        "memory_events": events,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cgroup-root", default="/sys/fs/cgroup")
    parser.add_argument("--output")
    parser.add_argument("--target-bytes", type=int, default=TARGET_BYTES)
    parser.add_argument("--tolerance-bytes", type=int, default=DEFAULT_TOLERANCE_BYTES)
    args = parser.parse_args()
    try:
        report = inspect_boundary(
            Path(args.cgroup_root),
            target_bytes=args.target_bytes,
            tolerance_bytes=args.tolerance_bytes,
        )
    except PreflightError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 2
    report["ok"] = True
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
