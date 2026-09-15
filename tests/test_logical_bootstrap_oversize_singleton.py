from __future__ import annotations

import pytest
from fastapi import HTTPException

from solana_roi import certification_logical_bootstrap as bootstrap


class _Cursor:
    def __init__(self, rows: list[tuple[int, str]]) -> None:
        self._rows = iter(rows)

    def fetchone(self):
        return next(self._rows, None)


class _SchemaVersionCursor:
    def fetchone(self):
        return (1,)


class _Reader:
    def execute(self, _sql: str):
        return _SchemaVersionCursor()


def _record(row: tuple[int, str]) -> dict[str, object]:
    return {"rowid": int(row[0]), "values": [row[1]]}


def _cursor_for(record: dict[str, object]) -> str:
    return str(record["rowid"])


def test_default_page_budget_remains_four_mib() -> None:
    assert bootstrap.DEFAULT_PAGE_BYTES == 4 * 1024 * 1024
    assert bootstrap.MAX_PAGE_BYTES == 16 * 1024 * 1024
    assert bootstrap.OVERSIZE_SINGLE_ROW_TABLES == frozenset({"checkpoint_current"})


def test_checkpoint_current_may_emit_one_bounded_oversize_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bootstrap, "MAX_SINGLE_ROW_BYTES", 1024)
    cursor = _Cursor([(1, "x" * 256), (2, "y" * 16)])

    rows, payload_bytes, done, next_cursor = bootstrap._stream_page_records(
        cursor,
        table_name="checkpoint_current",
        bounded_limit=250,
        max_bytes=128,
        row_to_record=_record,
        cursor_for=_cursor_for,
    )

    assert rows == [{"rowid": 1, "values": ["x" * 256]}]
    assert 128 < payload_bytes <= 1024
    assert done is False
    assert next_cursor == "1"


def test_oversize_row_is_not_allowed_for_other_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bootstrap, "MAX_SINGLE_ROW_BYTES", 1024)

    with pytest.raises(HTTPException) as caught:
        bootstrap._stream_page_records(
            _Cursor([(1, "x" * 256)]),
            table_name="certification_current",
            bounded_limit=250,
            max_bytes=128,
            row_to_record=_record,
            cursor_for=_cursor_for,
        )

    assert caught.value.status_code == 413
    assert "row_exceeds_transport_bound:certification_current" in str(caught.value.detail)


def test_checkpoint_current_still_has_a_hard_single_row_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bootstrap, "MAX_SINGLE_ROW_BYTES", 192)

    with pytest.raises(HTTPException) as caught:
        bootstrap._stream_page_records(
            _Cursor([(1, "x" * 256)]),
            table_name="checkpoint_current",
            bounded_limit=250,
            max_bytes=128,
            row_to_record=_record,
            cursor_for=_cursor_for,
        )

    assert caught.value.status_code == 413
    assert "row_exceeds_transport_bound:checkpoint_current" in str(caught.value.detail)


def test_normal_page_budget_still_truncates_before_next_row() -> None:
    cursor = _Cursor([(1, "a" * 24), (2, "b" * 24)])
    first_size = len('{"rowid":1,"values":["' + ("a" * 24) + '"]}')

    rows, payload_bytes, done, next_cursor = bootstrap._stream_page_records(
        cursor,
        table_name="certification_current",
        bounded_limit=250,
        max_bytes=first_size + 1,
        row_to_record=_record,
        cursor_for=_cursor_for,
    )

    assert rows == [{"rowid": 1, "values": ["a" * 24]}]
    assert payload_bytes == first_size
    assert done is False
    assert next_cursor == "1"


def test_identity_drift_remains_http_409(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        bootstrap.replication,
        "_meta",
        lambda _reader: {
            "epoch": "current-epoch",
            "schema_fingerprint": "f" * 64,
            "configured_schema_version": "1",
        },
    )
    monkeypatch.setattr(bootstrap.replication, "_schema_fingerprint", lambda _reader: "f" * 64)

    with pytest.raises(HTTPException) as caught:
        bootstrap._validate_identity(_Reader(), "different-epoch", "f" * 64)

    assert caught.value.status_code == 409
    assert "restart_required:identity_changed" in str(caught.value.detail)
