from __future__ import annotations

from solana_roi import direct_transaction as tx
from solana_roi import post161_candidate_attribution_repair as post161
from solana_roi import post164_invocation_source_repair as repair
from solana_roi import venue_native_candidate_graph_repair as venue


WALLET = "Scout1111111111111111111111111111111111111"
PUMP_FUN = next(iter(venue._PROGRAM_IDS_BY_SOURCE["PUMP_FUN"]))
PUMP_AMM = next(iter(venue._PROGRAM_IDS_BY_SOURCE["PUMP_AMM"]))
OTHER_PROGRAM = "Other11111111111111111111111111111111111"


def _result(*, account_keys: list[str], instructions: list[dict], inner: list[dict] | None = None, loaded: dict | None = None) -> dict:
    meta: dict = {
        "err": None,
        "innerInstructions": inner or [],
        "preTokenBalances": [],
        "postTokenBalances": [],
        "preBalances": [1_000_000_000 for _ in account_keys],
        "postBalances": [1_000_000_000 for _ in account_keys],
        "fee": 0,
    }
    if loaded is not None:
        meta["loadedAddresses"] = loaded
    return {
        "slot": 1,
        "blockTime": 1,
        "transaction": {
            "message": {
                "accountKeys": account_keys,
                "header": {"numRequiredSignatures": 1},
                "instructions": instructions,
            }
        },
        "meta": meta,
    }


def test_account_key_presence_alone_cannot_authorize_candidate_source() -> None:
    result = _result(account_keys=[WALLET, PUMP_FUN], instructions=[])

    # Prove the production failure mechanism: the legacy context helper sees the
    # program merely because it is an account key.
    assert tx.transaction_sources(result) == {"PUMP_FUN"}

    source, error = repair._source_for_transaction_invocation_only(result, None)
    assert source is None
    assert error == "supported_swap_source_missing"
    assert repair._invoked_supported_sources(result) == set()


def test_top_level_program_id_index_is_authoritative() -> None:
    result = _result(
        account_keys=[WALLET, PUMP_FUN],
        instructions=[{"programIdIndex": 1, "accounts": [0], "data": "1"}],
    )

    source, error = repair._source_for_transaction_invocation_only(result, None)
    assert (source, error) == ("PUMP_FUN", None)
    assert len(venue._venue_instruction_groups(result, "PUMP_FUN")) == 1


def test_inner_program_id_index_is_authoritative() -> None:
    result = _result(
        account_keys=[WALLET, OTHER_PROGRAM, PUMP_FUN],
        instructions=[{"programIdIndex": 1, "accounts": [0], "data": "1"}],
        inner=[
            {
                "index": 0,
                "instructions": [
                    {"programIdIndex": 2, "accounts": [0], "data": "1"}
                ],
            }
        ],
    )

    source, error = repair._source_for_transaction_invocation_only(result, None)
    assert (source, error) == ("PUMP_FUN", None)
    assert len(venue._venue_instruction_groups(result, "PUMP_FUN")) == 1


def test_loaded_address_program_id_index_remains_supported() -> None:
    result = _result(
        account_keys=[WALLET],
        instructions=[{"programIdIndex": 1, "accounts": [0], "data": "1"}],
        loaded={"writable": [PUMP_FUN], "readonly": []},
    )

    source, error = repair._source_for_transaction_invocation_only(result, None)
    assert (source, error) == ("PUMP_FUN", None)
    assert len(venue._venue_instruction_groups(result, "PUMP_FUN")) == 1


def test_multiple_actual_supported_venue_invocations_fail_closed() -> None:
    result = _result(
        account_keys=[WALLET, PUMP_FUN, PUMP_AMM],
        instructions=[
            {"programIdIndex": 1, "accounts": [0], "data": "1"},
            {"programIdIndex": 2, "accounts": [0], "data": "1"},
        ],
    )

    source, error = repair._source_for_transaction_invocation_only(result, None)
    assert source is None
    assert error == "multiple_supported_swap_sources"


def test_source_hint_requires_matching_actual_invocation() -> None:
    result = _result(account_keys=[WALLET, PUMP_FUN], instructions=[])

    source, error = repair._source_for_transaction_invocation_only(result, "PUMP_FUN")
    assert source is None
    assert error == "source_hint_not_present"


def test_diagnostics_distinguish_account_key_only_pseudo_source() -> None:
    repair.install_post164_invocation_source_repair()
    result = _result(account_keys=[WALLET, PUMP_FUN], instructions=[])

    facts = post161._diagnostic_facts(
        result,
        wallet=WALLET,
        source_hint=None,
        reason="supported_swap_source_missing",
    )

    assert facts["candidate_source_authority"] == repair.SOURCE_AUTHORITY
    assert facts["invoked_supported_sources"] == []
    assert facts["account_key_only_supported_sources"] == ["PUMP_FUN"]
    assert facts["all_invoked_program_count"] == 0
    shape = facts["shape"]
    assert '"account_key_only_sources":["PUMP_FUN"]' in shape
    assert '"invoked_sources":[]' in shape


def test_install_patches_only_candidate_source_authority() -> None:
    legacy = tx.transaction_sources
    repair.install_post164_invocation_source_repair()

    assert tx.transaction_sources is legacy
    assert venue._indexed_transaction_sources is repair._indexed_transaction_sources_invocation_only
    assert venue._source_for_transaction is repair._source_for_transaction_invocation_only
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
