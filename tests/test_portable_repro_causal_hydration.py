from __future__ import annotations

import importlib.util
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OVERLAY = ROOT / "diagnostics" / "portable_repro" / "causal_overlay.py"
RUNNER = ROOT / "diagnostics" / "portable_repro" / "run.sh"


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


overlay = load(OVERLAY, "portable_causal_overlay")


class CausalHydrationOverlayTests(unittest.TestCase):
    def _module(self):
        calls = {"scan": 0, "ensure": 0, "meta": 0}

        def original(_self):
            calls["scan"] += 1
            return True, 5000, 99999

        def ensure(_self):
            calls["ensure"] += 1

        def meta(_self):
            calls["meta"] += 1
            return {"bootstrap_rowid": 89655, "bootstrap_complete": False}

        return (
            types.SimpleNamespace(
                _advance_bootstrap=original,
                _ensure_state=ensure,
                _meta=meta,
            ),
            calls,
            original,
        )

    def test_normal_mode_does_not_patch_production_behavior(self):
        module, calls, original = self._module()
        evidence = overlay.apply_hydration_causal_overlay(module, mode=overlay.NORMAL_MODE)
        self.assertIs(module._advance_bootstrap, original)
        self.assertFalse(evidence["overlay_applied"])
        self.assertFalse(evidence["canonical_source_mutated"])
        self.assertEqual(calls["scan"], 0)

    def test_paused_mode_suppresses_only_scan_and_stays_incomplete(self):
        module, calls, original = self._module()
        evidence = overlay.apply_hydration_causal_overlay(
            module, mode=overlay.HYDRATION_BOOTSTRAP_PAUSED_MODE
        )
        self.assertIsNot(module._advance_bootstrap, original)
        self.assertIs(module._advance_bootstrap._portable_repro_original, original)
        complete, rows, cursor = module._advance_bootstrap(object())
        self.assertFalse(complete)
        self.assertEqual(rows, 0)
        self.assertEqual(cursor, 89655)
        self.assertEqual(calls["scan"], 0)
        self.assertEqual(calls["ensure"], 1)
        self.assertEqual(calls["meta"], 1)
        self.assertTrue(evidence["overlay_applied"])
        self.assertTrue(evidence["fail_closed"])
        self.assertFalse(evidence["bootstrap_completion_fabricated"])
        self.assertFalse(evidence["source_history_mutated"])
        self.assertFalse(evidence["live_money_authority"])

    def test_unknown_mode_fails_closed(self):
        module, _calls, _original = self._module()
        with self.assertRaises(RuntimeError):
            overlay.apply_hydration_causal_overlay(module, mode="skip-everything")


class CausalRunnerWiringTests(unittest.TestCase):
    def test_runner_uses_harness_target_and_exports_mode(self):
        text = RUNNER.read_text(encoding="utf-8")
        self.assertIn('CAUSAL_MODE="${PORTABLE_REPRO_CAUSAL_MODE:-normal}"', text)
        self.assertIn('export PORTABLE_REPRO_CAUSAL_MODE="$CAUSAL_MODE"', text)
        self.assertIn('uvicorn --app-dir "$ROOT/diagnostics/portable_repro" causal_target:app', text)
        self.assertIn('PORTABLE_REPRO_SOURCE_SHA', text)
        self.assertIn('production source-tree hash mismatch', text)


if __name__ == "__main__":
    unittest.main()
