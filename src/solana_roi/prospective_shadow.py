from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any

from .shadow_execution import ShadowExecutionCertificationGate, _percentile, validate_solana_public_key


class ProspectiveShadowExecutionCertificationGate(ShadowExecutionCertificationGate):
    """Certify only shadow simulations produced by the exact live release."""

    def __init__(self, *args: Any, prospective_start_at: datetime | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.prospective_start_at = prospective_start_at

    def status(self, limit: int = 500) -> dict[str, Any]:
        rows = self.ledger.recent(limit)
        if self.prospective_start_at is not None:
            rows = [
                row for row in rows
                if datetime.fromisoformat(str(row["completed_at"])) >= self.prospective_start_at
            ]
        configured = False
        if self.shadow_wallet_public_key:
            try:
                validate_solana_public_key(self.shadow_wallet_public_key)
                configured = True
            except ValueError:
                configured = False
        successful = [
            row
            for row in rows
            if row["transaction_built"]
            and row["simulation_ok"]
            and row.get("transaction_size_bytes") is not None
            and int(row["transaction_size_bytes"]) <= self.policy.max_transaction_size_bytes
        ]
        success_fraction = len(successful) / len(rows) if rows else 0.0
        p95_latency = _percentile([float(row["total_latency_ms"]) for row in rows], 0.95)
        certified = bool(
            configured
            and len(rows) >= self.policy.min_samples
            and success_fraction >= self.policy.min_simulation_success_fraction
            and p95_latency is not None
            and p95_latency <= self.policy.max_p95_shadow_latency_ms
        )
        return {
            "certified": certified,
            "configured": configured,
            "private_key_access": False,
            "signing_available": False,
            "submission_available": False,
            "sample_count": len(rows),
            "simulation_success_count": len(successful),
            "simulation_success_fraction": success_fraction,
            "p95_shadow_execution_latency_ms": p95_latency,
            "prospective_start_at": self.prospective_start_at.isoformat() if self.prospective_start_at else None,
            "requirements": {
                **asdict(self.policy),
                "prospective_release_boundary_required": True,
            },
        }
