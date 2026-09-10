from __future__ import annotations

"""Production-only transport bound for certification replica delta pages.

Exact production evidence on 2026-09-10 showed 2,000-row delta requests could take
40-60+ seconds while the isolated certifier has a 30-second request deadline. The
server would later return 200 after the client had already failed closed. Keep the
same canonical evidence and replication semantics, but bound each authoritative
request to substantially less work so catch-up advances through more, smaller pages.
"""

import os

PRODUCTION_CERTIFICATION_DELTA_MAX_ROWS = 250

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CONTINUITY_SEMANTICS_CHANGED = False


def configure_production_certification_delta_bound() -> None:
    """Apply a conservative default without overriding an explicit operator value."""
    os.environ.setdefault(
        "SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS",
        str(PRODUCTION_CERTIFICATION_DELTA_MAX_ROWS),
    )
