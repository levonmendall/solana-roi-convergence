from __future__ import annotations

import json

from solana_roi.deployment import DEFAULT_SCOUT_PROFILES, default_scout_profiles_json
from solana_roi.direct_deployment import deployment_preflight


SHADOW_WALLET = DEFAULT_SCOUT_PROFILES[0]["wallet"]


def _env():
    return {
        "PAPER_ONLY": "true",
        "SOLANA_NETWORK": "mainnet-beta",
        "SOLANA_ROI_PROGRAM_WIDE_SWAP_COVERAGE": "true",
        "SOLANA_ROI_DIRECT_SOLANA_ENABLED": "true",
        "SOLANA_ROI_SHADOW_CLOCK_ENABLED": "true",
        "SOLANA_ROI_WALLET_PROFILES_JSON": default_scout_profiles_json(),
        "JUPITER_API_KEY": "configured",
        "SOLANA_ROI_COHORT_ARM_AUTH": "configured",
        "SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY": SHADOW_WALLET,
        "SOLANA_ROI_ALCHEMY_API_KEY": "configured",
    }


def _checks(status):
    return {row["name"]: row for row in status["checks"]}


def test_direct_preflight_is_ready_without_helius_credentials():
    status = deployment_preflight(_env())
    assert status["ready_for_live_shadow_collection"] is True
    assert status["data_plane"] == "direct-standard-solana"
    assert status["provider_enhanced_webhook_required"] is False
    assert status["strategy_scope_reduced"] is False
    assert len(status["program_addresses"]) == 7
    assert len(status["rpc_endpoints"]) >= 3
    assert "HELIUS_API_KEY" not in repr(status)
    assert "configured" not in repr(status["rpc_endpoints"])


def test_two_rpc_endpoints_fail_independent_quorum():
    env = _env()
    env.pop("SOLANA_ROI_ALCHEMY_API_KEY")
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is False
    assert _checks(status)["redundant_standard_rpc"]["ok"] is True
    assert _checks(status)["independent_standard_rpc_quorum"]["ok"] is False


def test_direct_disabled_fails_preflight_closed():
    env = _env()
    env["SOLANA_ROI_DIRECT_SOLANA_ENABLED"] = "false"
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is False
    assert _checks(status)["direct_solana_enabled"]["ok"] is False


def test_single_rpc_endpoint_fails_redundancy_preflight():
    env = _env()
    env.pop("SOLANA_ROI_ALCHEMY_API_KEY")
    env["SOLANA_ROI_RPC_ENDPOINTS_JSON"] = json.dumps([
        {"name": "only", "http": "https://only.example", "ws": "wss://only.example"},
    ])
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is False
    assert _checks(status)["redundant_standard_rpc"]["ok"] is False
    assert _checks(status)["independent_standard_rpc_quorum"]["ok"] is False


def test_private_key_material_still_fails_closed():
    env = _env()
    env["SOLANA_ROI_PRIVATE_KEY"] = "must-never-be-present"
    status = deployment_preflight(env)
    assert status["ready_for_live_shadow_collection"] is False
    assert _checks(status)["no_private_key_material"]["ok"] is False
