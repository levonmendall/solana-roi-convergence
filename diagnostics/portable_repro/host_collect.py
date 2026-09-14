#!/usr/bin/env python3
"""Host-side collector for a container's cgroup-v2 memory evidence.

Run this outside the target container. It resolves the container init PID's cgroup
through the host /proc tree, then samples the cgroup while the process exists.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("portable_collector", HERE / "collector.py")
_collector = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_collector)


class HostCollectError(RuntimeError):
    pass


def resolve_cgroup_root(pid: int, *, proc_root: Path = Path("/proc"), sys_cgroup_root: Path = Path("/sys/fs/cgroup")) -> Path:
    path = proc_root / str(pid) / "cgroup"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise HostCollectError(f"cannot read {path}: {exc}") from exc
    for line in lines:
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0" and parts[1] == "":
            rel = parts[2].lstrip("/")
            return sys_cgroup_root / rel
    raise HostCollectError("cgroup v2 unified membership not found")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--proc-root", default="/proc")
    parser.add_argument("--sys-cgroup-root", default="/sys/fs/cgroup")
    args = parser.parse_args()
    if args.interval <= 0:
        raise SystemExit("interval must be positive")
    proc_root = Path(args.proc_root)
    cgroup_root = resolve_cgroup_root(
        args.pid,
        proc_root=proc_root,
        sys_cgroup_root=Path(args.sys_cgroup_root),
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a", encoding="utf-8") as handle:
        while (proc_root / str(args.pid)).exists():
            try:
                record = _collector.sample(cgroup_root, proc_root=proc_root)
            except Exception as exc:
                record = {"ts_unix": time.time(), "collector_error": f"{type(exc).__name__}:{exc}"}
            record["resolved_cgroup_root"] = str(cgroup_root)
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
