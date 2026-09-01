from solana_roi.storage import AppendOnlyEventStore


def test_event_chain_verifies(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "test.sqlite3")
    store.append("first_touch", "2026-09-01T00:00:00+00:00", {"mint": "A"})
    store.append("confirmation", "2026-09-01T00:00:05+00:00", {"mint": "A"})
    assert store.verify() is True
