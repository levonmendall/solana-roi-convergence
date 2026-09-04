from __future__ import annotations

# Import the complete canonical production composition first. This guarantees the
# candidate hot-path wrapper, PR96-PR103 continuity stack, final V4 handoff, and
# runtime-bootstrap composition are already installed before this final semantic
# correction becomes the outer TimedRiskCollectors.refresh wrapper.
from .production import app as app
from .candidate_risk_window_repair import install_candidate_risk_window_repair

# Five seconds remains the latency-certification target. Twenty seconds remains the
# strategy's maximum executable-entry ceiling. This installer only prevents the
# former from cancelling otherwise prospective evidence still inside the latter.
install_candidate_risk_window_repair()

__all__ = ["app"]
