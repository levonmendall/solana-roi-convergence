from __future__ import annotations

import subprocess
import sys


def test_phase16_exact_exit_is_reachable_beneath_fresh_production_sell_composition() -> None:
    """Model a real process start instead of relying on shared pytest wrapper globals.

    Package import installs Phase 16 before later strategy-learning/risk wrappers.
    The risk-v5 sell wrapper must therefore capture a chain whose delegated sell
    still carries the exact-exit marker. Running this assertion in a fresh Python
    interpreter prevents unrelated tests from changing module-level captured
    originals and gives us the same one-shot composition semantics as Render.
    """
    script = r'''
import solana_roi.production  # noqa: F401
from solana_roi import risk_conditioned_alpha_v5 as risk_v5
from solana_roi import v51_exact_exit_execution as exact

assert exact._INSTALLED is True
assert exact._ORIGINAL_SELL is not None
assert risk_v5._ORIGINAL_FINAL_SELL is not None
assert getattr(risk_v5._ORIGINAL_FINAL_SELL, "_roi_exact_exit_execution", False) is True
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        "fresh production composition lost the Phase 16 exact-exit sell path\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
