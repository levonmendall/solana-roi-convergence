from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRE = ROOT / "diagnostics" / "portable_repro" / "preflight.py"
FID = ROOT / "diagnostics" / "portable_repro" / "fidelity.py"
HOST = ROOT / "diagnostics" / "portable_repro" / "host_collect.py"
OBS = ROOT / "diagnostics" / "portable_repro" / "observer.py"


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


preflight = load(PRE, "portable_preflight")
fidelity = load(FID, "portable_fidelity")
host_collect = load(HOST, "portable_host_collect")
observer = load(OBS, "portable_observer")


class PreflightTests(unittest.TestCase):
    def _tree(self, memory_max: str = str(2 * 1024**3)):
        td = tempfile.TemporaryDirectory()
        root = Path(td.name)
        (root / "cgroup.controllers").write_text("cpu memory pids\n")
        (root / "memory.max").write_text(memory_max + "\n")
        (root / "memory.current").write_text("1024\n")
        (root / "memory.events").write_text("oom 0\noom_kill 0\n")
        (root / "cgroup.procs").write_text("123\n")
        return td, root

    def test_accepts_two_gib(self):
        td, root = self._tree()
        with td:
            report = preflight.inspect_boundary(root, proc_root=root / "missing-proc")
            self.assertEqual(report["memory_max"], 2 * 1024**3)

    def test_rejects_unlimited(self):
        td, root = self._tree("max")
        with td, self.assertRaises(preflight.PreflightError):
            preflight.inspect_boundary(root)

    def test_rejects_fourteen_gib(self):
        td, root = self._tree(str(14 * 1024**3))
        with td, self.assertRaises(preflight.PreflightError):
            preflight.inspect_boundary(root)

    def test_rejects_empty_membership(self):
        td, root = self._tree()
        with td:
            (root / "cgroup.procs").write_text("")
            with self.assertRaises(preflight.PreflightError):
                preflight.inspect_boundary(root)


class FidelityTests(unittest.TestCase):
    def valid(self):
        return {
            "canonical_sha": fidelity.CANONICAL_SHA,
            "network_mode": "replay-only",
            "production_entrypoint": "uvicorn solana_roi.production:app --host 0.0.0.0 --port $PORT",
            "state_families": {
                name: {"fixture_path": f"/fixtures/{name}.json", "provenance": "UNKNOWN"}
                for name in fidelity.REQUIRED_STATE_FAMILIES
            },
            "provider_paths": {
                name: {
                    "classification": "SYNTHETIC STRUCTURAL REPLAY",
                    "live_network_allowed": False,
                }
                for name in fidelity.REQUIRED_PROVIDER_PATHS
            },
        }

    def test_accepts_complete_manifest(self):
        fidelity.validate_manifest(self.valid())

    def test_rejects_missing_state(self):
        data = self.valid()
        data["state_families"].pop(next(iter(fidelity.REQUIRED_STATE_FAMILIES)))
        with self.assertRaises(fidelity.FidelityError):
            fidelity.validate_manifest(data)

    def test_rejects_not_represented_provider(self):
        data = self.valid()
        first = next(iter(fidelity.REQUIRED_PROVIDER_PATHS))
        data["provider_paths"][first]["classification"] = "NOT REPRESENTED"
        with self.assertRaises(fidelity.FidelityError):
            fidelity.validate_manifest(data)

    def test_rejects_live_network(self):
        data = self.valid()
        first = next(iter(fidelity.REQUIRED_PROVIDER_PATHS))
        data["provider_paths"][first]["live_network_allowed"] = True
        with self.assertRaises(fidelity.FidelityError):
            fidelity.validate_manifest(data)

    def test_rejects_wrong_sha(self):
        data = self.valid()
        data["canonical_sha"] = "deadbeef"
        with self.assertRaises(fidelity.FidelityError):
            fidelity.validate_manifest(data)


class NetworkGuardTests(unittest.TestCase):
    def _check(self, address_expr: str) -> str:
        guard = ROOT / "diagnostics" / "portable_repro" / "network_guard"
        code = (
            "import importlib.util; "
            f"p={str(guard / 'sitecustomize.py')!r}; "
            "s=importlib.util.spec_from_file_location('portable_network_guard', p); "
            "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
            f"print(m._allowed_address({address_expr}))"
        )
        return subprocess.check_output([sys.executable, "-S", "-c", code], text=True).strip()

    def test_allows_loopback(self):
        self.assertEqual(self._check("('127.0.0.1', 80)"), "True")

    def test_blocks_external_hostname(self):
        self.assertEqual(self._check("('api.mainnet.solana.com', 443)"), "False")


class HostCollectorTests(unittest.TestCase):
    def test_resolves_unified_cgroup(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proc = root / "proc"
            syscg = root / "cgroup"
            (proc / "44").mkdir(parents=True)
            syscg.mkdir()
            (proc / "44" / "cgroup").write_text("0::/docker/example\n")
            self.assertEqual(
                host_collect.resolve_cgroup_root(44, proc_root=proc, sys_cgroup_root=syscg),
                syscg / "docker/example",
            )

    def test_rejects_non_v2_membership(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proc = root / "proc"
            (proc / "44").mkdir(parents=True)
            (proc / "44" / "cgroup").write_text("5:memory:/legacy\n")
            with self.assertRaises(host_collect.HostCollectError):
                host_collect.resolve_cgroup_root(44, proc_root=proc, sys_cgroup_root=root / "cgroup")


class ObserverTests(unittest.TestCase):
    def test_accepts_loopback(self):
        observer._validate_base_url("http://127.0.0.1:10000")

    def test_rejects_external_host(self):
        with self.assertRaises(ValueError):
            observer._validate_base_url("https://example.com")


if __name__ == "__main__":
    unittest.main()
