from __future__ import annotations

from typing import Any

import pytest

from solana_roi import production_proof_read_boundary_repair as repair


def test_proof_cycle_reuses_one_underlying_promotion_read_and_preserves_row_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    store = object()
    calls: list[Any] = []

    def loader(observed_store: Any) -> list[dict[str, Any]]:
        calls.append(observed_store)
        return [
            {"source_signature": "one", "net_return": 0.10},
            {"source_signature": "two", "net_return": -0.05},
        ]

    monkeypatch.setattr(repair, "_ORIGINAL_PROMOTION_RECORDS", loader)

    def builder() -> dict[str, Any]:
        phase14 = repair._proof_scoped_promotion_records(store)
        phase14[0]["net_return"] = 999.0
        phase17 = repair._proof_scoped_promotion_records(store)
        batch6 = repair._proof_scoped_promotion_records(store)
        assert phase17[0]["net_return"] == 0.10
        assert batch6[0]["net_return"] == 0.10
        assert phase14 is not phase17
        assert phase17 is not batch6
        assert phase14[0] is not phase17[0]
        assert phase17[0] is not batch6[0]
        return {"paper_only": True, "live_money_authority": False}

    payload = repair._with_proof_scoped_promotion_records(builder)()

    assert payload == {"paper_only": True, "live_money_authority": False}
    assert calls == [store]
    state = repair._promotion_cache_state()
    assert state["last_cycle_underlying_reads"] == 1
    assert state["last_cycle_cache_hits"] == 2
    assert state["last_cycle_row_count"] == 2
    assert state["proof_cycle_scoped"] is True
    assert state["ttl_cache"] is False
    assert state["persistent_cache"] is False
    assert state["strategy_semantics_changed"] is False
    assert state["economic_thresholds_changed"] is False


def test_promotion_records_outside_proof_context_are_never_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    store = object()
    calls = 0

    def loader(_: Any) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return [{"call": calls}]

    monkeypatch.setattr(repair, "_ORIGINAL_PROMOTION_RECORDS", loader)

    first = repair._proof_scoped_promotion_records(store)
    second = repair._proof_scoped_promotion_records(store)

    assert first == [{"call": 1}]
    assert second == [{"call": 2}]
    assert calls == 2


def test_proof_cycle_cache_is_store_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    first_store = object()
    second_store = object()
    calls: list[Any] = []

    def loader(store: Any) -> list[dict[str, Any]]:
        calls.append(store)
        return [{"store_number": 1 if store is first_store else 2}]

    monkeypatch.setattr(repair, "_ORIGINAL_PROMOTION_RECORDS", loader)

    def builder() -> dict[str, Any]:
        assert repair._proof_scoped_promotion_records(first_store) == [{"store_number": 1}]
        assert repair._proof_scoped_promotion_records(first_store) == [{"store_number": 1}]
        assert repair._proof_scoped_promotion_records(second_store) == [{"store_number": 2}]
        assert repair._proof_scoped_promotion_records(second_store) == [{"store_number": 2}]
        return {}

    repair._with_proof_scoped_promotion_records(builder)()

    assert calls == [first_store, second_store]
    state = repair._promotion_cache_state()
    assert state["last_cycle_underlying_reads"] == 2
    assert state["last_cycle_cache_hits"] == 2
    assert state["last_cycle_row_count"] == 2


def test_failed_proof_resets_cycle_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    store = object()
    calls = 0

    def loader(_: Any) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return [{"call": calls}]

    monkeypatch.setattr(repair, "_ORIGINAL_PROMOTION_RECORDS", loader)

    def builder() -> dict[str, Any]:
        repair._proof_scoped_promotion_records(store)
        repair._proof_scoped_promotion_records(store)
        raise RuntimeError("expected-test-failure")

    with pytest.raises(RuntimeError, match="expected-test-failure"):
        repair._with_proof_scoped_promotion_records(builder)()

    # A failed proof cycle cannot leak its cache into later work.
    assert repair._proof_scoped_promotion_records(store) == [{"call": 2}]
    assert calls == 2
    state = repair._promotion_cache_state()
    assert state["last_cycle_underlying_reads"] == 1
    assert state["last_cycle_cache_hits"] == 1
