#!/usr/bin/env python3
"""Low-overhead cgroup/process collector for disposable reproduction runs."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any


def _text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _kv_ints(path: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    raw = _text(path)
    if raw is None:
        return out
    for line in raw.splitlines():
        parts = line.replace(":", "").split()
        if len(parts) >= 2:
            try:
                value = int(parts[1])
            except ValueError:
                continue
            if len(parts) >= 3 and parts[2] == "kB":
                value *= 1024
            out[parts[0]] = value
    return out


def sample(cgroup_root: Path, proc_root: Path = Path("/proc")) -> dict[str, Any]:
    record: dict[str, Any] = {"ts_unix": time.time()}
    for name in ("memory.current", "memory.max", "pids.current"):
        raw = _text(cgroup_root / name)
        if raw is not None:
            try:
                record[name] = int(raw)
            except ValueError:
                record[name] = raw
    record["memory.stat"] = _kv_ints(cgroup_root / "memory.stat")
    record["memory.events"] = _kv_ints(cgroup_root / "memory.events")

    raw_pids = _text(cgroup_root / "cgroup.procs") or ""
    processes = []
    for raw in raw_pids.splitlines():
        try:
            pid = int(raw)
        except ValueError:
            continue
        proc = proc_root / str(pid)
        processes.append(
            {
                "pid": pid,
                "status": _kv_ints(proc / "status"),
                "smaps_rollup": _kv_ints(proc / "smaps_rollup"),
                "io": _kv_ints(proc / "io"),
                "thread_count": len(list((proc / "task").glob("[0-9]*"))) if (proc / "task").exists() else None,
            }
        )
    record["processes"] = processes
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cgroup-root", default="/sys/fs/cgroup")
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--samples", type=int, default=120)
    args = parser.parse_args()
    if args.interval <= 0 or args.samples <= 0:
        raise SystemExit("interval and samples must be positive")
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for idx in range(args.samples):
            handle.write(json.dumps(sample(Path(args.cgroup_root)), sort_keys=True) + "\n")
            handle.flush()
            if idx + 1 < args.samples:
                time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
