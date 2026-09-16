"""Read-only, bounded-buffer verification of canonical checkpoint section rows.

The writer already seals canonical UTF-8 JSON, optionally in zlib/base64. Verify
that representation, JSON grammar AND canonical spelling before hashing its byte
stream as semantic truth. Retain dictionary shape only (arrays/scalars are opaque
for the reclamation projection). No checkpoint value, compressed output buffer,
or temporary disk spool is materialized on the canonical path.

Unusual older, noncanonical representations retain the existing decoder/hash
semantics via a compatibility fallback; they are never silently byte-hashed as
canonical truth. This is a verifier, not a retention/deletion authority.
"""
from __future__ import annotations

import base64
import codecs
import hashlib
import json
import re
import sqlite3
import zlib
from typing import Any, Iterator

from .active_storage import (
    _CURRENT_PAYLOAD_CODEC, _CURRENT_PAYLOAD_TAG, _MAX_CURRENT_PAYLOAD_BYTES,
    canonical_json, decode_current_payload,
)

_CHUNK = 65_536
_STRING_SPECIAL = re.compile(r'["\\\x00-\x1f]')
_PREFIX = ('{"' + _CURRENT_PAYLOAD_TAG + '":"' + _CURRENT_PAYLOAD_CODEC + '","data":"').encode()
_SUFFIX = re.compile(rb'","utf8_bytes":([1-9][0-9]*)}\Z')


class _NonCanonical(ValueError):
    """Valid legacy input may require the established materializing decoder."""


def dictionary_shape(value: Any) -> Any:
    """Exactly the information used by _project_to_checkpoint_shape, no values."""
    return {key: dictionary_shape(item) for key, item in value.items()} if isinstance(value, dict) else None


class _CanonicalJSON:
    """Validate one canonical JSON value incrementally and return dict-only shape.

    No scalar-string values or array items are retained. Object keys must be
    strictly sorted, and scalar encodings must match the repository JSON encoder.
    Thus byte hashing is equivalent to payload_hash(json.loads(stream)).
    """
    def __init__(self, chunks: Iterator[bytes], *digests: Any) -> None:
        self.chunks = iter(chunks)
        self.decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self.digests = digests
        self.buffer = ""
        self.pos = 0
        self.done = False

    def peek(self) -> str:
        while self.pos == len(self.buffer) and not self.done:
            try:
                raw = next(self.chunks)
            except StopIteration:
                self.buffer = self.decoder.decode(b"", final=True)
                self.done = True
            else:
                for digest in self.digests:
                    digest.update(raw)
                self.buffer = self.decoder.decode(raw)
            self.pos = 0
        return self.buffer[self.pos:self.pos + 1]

    def take(self, expected: str) -> None:
        if self.peek() != expected:
            raise _NonCanonical("noncanonical or invalid JSON token")
        self.pos += 1

    def string(self, *, key: bool = False) -> str | None:
        self.take('"')
        parts: list[str] | None = [] if key else None
        key_size = 0
        while True:
            if not self.peek():
                raise _NonCanonical("unterminated JSON string")
            match = _STRING_SPECIAL.search(self.buffer, self.pos)
            end = match.start() if match else len(self.buffer)
            if parts is not None:
                part = self.buffer[self.pos:end]
                key_size += len(part)
                if key_size > 1_048_576:
                    raise _NonCanonical("oversized dictionary key")
                parts.append(part)
            self.pos = end
            char = self.peek()
            if char == '"':
                self.pos += 1
                return "".join(parts) if parts is not None else None
            if char != "\\":
                if char and ord(char) < 32:
                    raise _NonCanonical("unescaped control character")
                # Chunk ended without a special; scan the next chunk.
                continue
            self.pos += 1
            escape = self.peek()
            if not escape:
                raise _NonCanonical("truncated escape")
            self.pos += 1
            short = {'"': '"', "\\": "\\", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
            if escape in short:
                decoded = short[escape]
            elif escape == "u":
                digits = ""
                for _ in range(4):
                    digit = self.peek()
                    if digit not in "0123456789abcdef" or not digit:
                        raise _NonCanonical("noncanonical Unicode escape")
                    digits += digit
                    self.pos += 1
                decoded = chr(int(digits, 16))
                if ord(decoded) >= 32 or decoded in "\b\f\n\r\t":
                    raise _NonCanonical("noncanonical Unicode escape")
            else:
                raise _NonCanonical("noncanonical string escape")
            if parts is not None:
                parts.append(decoded)
                key_size += 1
                if key_size > 1_048_576:
                    raise _NonCanonical("oversized dictionary key")

    def value(self, capture: bool = True) -> Any:
        char = self.peek()
        if char == "{":
            self.pos += 1
            shape = {} if capture else None
            previous: str | None = None
            if self.peek() != "}":
                while True:
                    key = self.string(key=True)
                    if previous is not None and key <= previous:
                        raise _NonCanonical("unordered or duplicate object keys")
                    previous = key
                    self.take(":")
                    child = self.value(capture)
                    if capture:
                        shape[key] = child
                    if self.peek() != ",":
                        break
                    self.pos += 1
            self.take("}")
            return shape
        if char == "[":
            self.pos += 1
            if self.peek() != "]":
                while True:
                    self.value(False)
                    if self.peek() != ",":
                        break
                    self.pos += 1
            self.take("]")
            return None
        if char == '"':
            self.string()
            return None
        for literal in ("true", "false", "null", "NaN", "Infinity"):
            if char == literal[0]:
                for letter in literal:
                    self.take(letter)
                return None
        token = ""
        while self.peek() and (self.peek() in "0123456789-+.eE" or (token == "-" and self.peek() == "I")):
            if token == "-" and self.peek() == "I":
                for letter in "Infinity":
                    self.take(letter)
                token = "-Infinity"
                break
            token += self.peek()
            self.pos += 1
            if len(token) > 10_000:
                raise _NonCanonical("oversized number")
        if not token or canonical_json(json.loads(token)) != token:
            raise _NonCanonical("noncanonical numeric encoding")
        return None

    def parse(self) -> Any:
        shape = self.value()
        if self.peek():
            raise _NonCanonical("trailing JSON or noncanonical whitespace")
        return shape


def _blob_chunks(blob: sqlite3.Blob, start: int, size: int) -> Iterator[bytes]:
    blob.seek(start)
    while size:
        part = blob.read(min(size, _CHUNK))
        if not part:
            raise RuntimeError("checkpoint payload truncated during read")
        size -= len(part)
        yield part


def _decoded_chunks(blob: sqlite3.Blob) -> Iterator[bytes]:
    """Decode our exact canonical envelope with capped base64/zlib buffers."""
    size = len(blob)
    blob.seek(0)
    if blob.read(len(_PREFIX)) != _PREFIX:
        yield from _blob_chunks(blob, 0, size)
        return
    blob.seek(max(0, size - 80))
    tail = blob.read(80)
    match = _SUFFIX.search(tail)
    if match is None:
        raise _NonCanonical("noncanonical compression envelope")
    expected = int(match.group(1))
    if not 0 < expected <= _MAX_CURRENT_PAYLOAD_BYTES:
        raise RuntimeError("current-state payload encoding is invalid")
    end = size - len(tail) + match.start()
    encoded_size = end - len(_PREFIX)
    if encoded_size <= 0 or encoded_size % 4:
        raise RuntimeError("current-state payload decoding failed: invalid base64 size")
    decoder = zlib.decompressobj()
    total = 0
    consumed = 0
    for encoded in _blob_chunks(blob, len(_PREFIX), encoded_size):
        consumed += len(encoded)
        if b"=" in encoded and consumed != encoded_size:
            raise RuntimeError("current-state payload decoding failed: interior padding")
        pending = base64.b64decode(encoded, validate=True)
        while pending:
            raw = decoder.decompress(pending, _CHUNK)
            pending = decoder.unconsumed_tail
            total += len(raw)
            if total > expected or decoder.unused_data:
                raise RuntimeError("current-state payload decoding failed: oversized or trailing stream")
            if raw:
                yield raw
    # No flush(): its length is only an initial allocation, not an output cap.
    # Feed empty input to drain any capped output still held by the inflater.
    while not decoder.eof:
        raw = decoder.decompress(b"", _CHUNK)
        if not raw:
            break
        total += len(raw)
        if total > expected:
            raise RuntimeError("current-state payload decoding failed: oversized stream")
        yield raw
    if total != expected or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise RuntimeError("current-state payload decoding failed: incomplete or trailing stream")


def verify_section_shape(
    conn: sqlite3.Connection, table: str, key_column: str, key: str,
    section: str, expected_hash: str, combined: Any,
) -> tuple[Any, Any]:
    """Verify physical/decoded hashes under the caller's pinned transaction."""
    row = conn.execute(
        f'SELECT rowid,payload_hash FROM "{table}" WHERE "{key_column}"=?', (key,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"active checkpoint semantic section missing: {section}")
    with conn.blobopen(table, "payload_json", int(row[0]), readonly=True) as blob:
        physical = hashlib.sha256()
        for chunk in _blob_chunks(blob, 0, len(blob)):
            physical.update(chunk)
        if physical.hexdigest() != str(row[1]):
            raise RuntimeError(f"active checkpoint semantic section payload hash mismatch: {section}")
        semantic = hashlib.sha256()
        aggregate = combined.copy()
        try:
            shape = _CanonicalJSON(_decoded_chunks(blob), semantic, aggregate).parse()
            # A raw row containing the reserved codec key must go through the
            # established decoder (it might be a noncanonical codec envelope).
            blob.seek(0)
            if isinstance(shape, dict) and _CURRENT_PAYLOAD_TAG in shape and blob.read(len(_PREFIX)) != _PREFIX:
                raise _NonCanonical("reserved codec key")
        except _NonCanonical:
            blob.seek(0)
            value = decode_current_payload(blob.read().decode("utf-8"))
            shape = dictionary_shape(value)
            semantic = hashlib.sha256()
            aggregate = combined.copy()
            encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
            for fragment in encoder.iterencode(value):
                encoded = fragment.encode("utf-8")
                semantic.update(encoded)
                aggregate.update(encoded)
            del value
        if semantic.hexdigest() != str(expected_hash):
            raise RuntimeError(f"active checkpoint section hash mismatch: {section}")
        return shape, aggregate
