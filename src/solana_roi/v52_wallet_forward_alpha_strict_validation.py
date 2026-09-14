from __future__ import annotations

from typing import Any, Sequence

from .v52_wallet_forward_alpha import (
    STATUS_NO_VALUE,
    ReplayComparisonObservation,
    WalletForwardAlphaEngine,
    WalletForwardValidationReport,
)

STRICT_VALIDATION_VERSION = "v52-wallet-forward-alpha-strict-validation-v1"
_INSTALLED = False
_BASE_EVALUATE: Any = None


def _strict_evaluate(cls: type[WalletForwardAlphaEngine], rows: Sequence[ReplayComparisonObservation]) -> WalletForwardValidationReport:
    if _BASE_EVALUATE is None:
        raise RuntimeError("Wallet Forward Alpha strict-validation predecessor unavailable")
    report = _BASE_EVALUATE(cls, rows)
    if not report.strategy_influence_enabled:
        return report
    # Material promotion means the new Wallet Forward Alpha layer itself added
    # value over the already-wallet-aware v5.2 control. Merely tying current wallet
    # intelligence while beating the no-wallet baseline is not incremental alpha.
    incremental = bool(
        len(report.windows) == 3
        and all(
            window.forward_vs_current_mean > 0.0
            and window.forward_vs_current_lower_95 > 0.0
            for window in report.windows
        )
    )
    if incremental:
        return report
    reasons = tuple(dict.fromkeys((*report.reasons, "no_statistically_positive_incremental_value_vs_current_wallet_intelligence")))
    return WalletForwardValidationReport(
        status=STATUS_NO_VALUE,
        windows=report.windows,
        strategy_influence_enabled=False,
        influence_scope=(),
        reasons=reasons,
    )


def install_strict_wallet_forward_alpha_validation() -> None:
    global _INSTALLED, _BASE_EVALUATE
    if _INSTALLED:
        return
    descriptor = WalletForwardAlphaEngine.__dict__["evaluate_replay"]
    _BASE_EVALUATE = descriptor.__func__
    WalletForwardAlphaEngine.evaluate_replay = classmethod(_strict_evaluate)  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "installed": _INSTALLED,
        "version": STRICT_VALIDATION_VERSION,
        "material_requires_positive_increment_vs_current_wallet_intelligence": True,
        "tie_with_current_wallet_is_not_material_value": True,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["STRICT_VALIDATION_VERSION", "install_strict_wallet_forward_alpha_validation", "status"]
