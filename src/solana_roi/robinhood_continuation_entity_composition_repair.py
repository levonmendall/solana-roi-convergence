from __future__ import annotations

from typing import Any

from . import continuation_market_recalibration as continuation
from . import robinhood_entity_resolution_repair as entity_repair


REPAIR_VERSION = "robinhood-continuation-entity-composition-v1"


def install_robinhood_continuation_entity_composition_repair(plane_cls: type[Any]) -> None:
    """Keep PR146 continuation authority while replacing only its identity substrate.

    ``install_robinhood_entity_resolution_repair`` intentionally patches the concrete
    production plane after all mixins exist. PR146, however, installed its continuation
    flow wrapper earlier on ``RobinhoodProfitMaximizerMixin`` and captured the pre-
    continuation flow implementation in ``continuation._ORIGINAL_RH_FLOW``.

    Rebind that captured substrate to the repaired entity resolver and restore the
    continuation wrapper as the final class method. The result keeps PR146's
    bootstrap/extended-continuation state machine, while unresolved raw addresses
    still cannot count as independent evidence and decision-critical identities still
    fail closed.
    """

    if bool(getattr(plane_cls, "_roi_continuation_entity_composition_repair", False)):
        return
    if not bool(getattr(continuation, "_INSTALLED", False)):
        # Direct/test imports that do not install the continuation policy retain the
        # entity repair's own flow method. Do not invent continuation authority.
        setattr(plane_cls, "_roi_continuation_entity_composition_repair", True)
        setattr(plane_cls, "_roi_continuation_entity_composition_version", REPAIR_VERSION)
        return
    if continuation._ORIGINAL_RH_FLOW is None:
        raise RuntimeError("Robinhood continuation flow substrate is unavailable")

    continuation._ORIGINAL_RH_FLOW = entity_repair._v5_flow_metrics
    plane_cls._v5_flow_metrics = continuation._rh_flow_without_sniper_cap
    setattr(plane_cls, "_roi_continuation_entity_composition_repair", True)
    setattr(plane_cls, "_roi_continuation_entity_composition_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "install_robinhood_continuation_entity_composition_repair",
]
