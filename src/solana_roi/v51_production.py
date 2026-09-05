from __future__ import annotations

"""Canonical production entrypoint for the frozen v5.1 economic authority.

Legacy modules imported by ``production`` remain transport/reliability compatibility
internals. This module is the one final strategy-composition boundary: it installs
no live-money capability and owns the economic authority and proof APIs that sit
above those internals.
"""

from .production import app as app
from .api import ingestion_runtime
from .v51_consolidated_strategy import install_v51_consolidated_strategy
from .v51_strategy_api import install_v51_strategy_api

install_v51_consolidated_strategy()
install_v51_strategy_api(app, ingestion_runtime)

__all__ = ["app"]
