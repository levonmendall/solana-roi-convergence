class _ReclamationJSONStream:
    """Validate canonical JSON without materializing scalar values or arrays.

    This is a verifier for the existing canonical writer's large payloads, not
    a replacement JSON decoder. Noncanonical large encodings fail closed; small
    historical encodings use the original decoder below. Dictionary key order,
    numeric spelling, string escapes, UTF-8 and full input consumption are all
    checked before a canonical-byte digest may stand in for re-encoding values.
    """

    def __init__(self, chunks):
        import re
        self.chunks = iter(chunks)
        self.buffer = b""
        self.offset = 0
        self.string_special = re.compile(rb'["\\\x00-\x1f]')
        self.token_end = re.compile(rb'[\s,\]}]')

    def peek(self):
        while self.offset == len(self.buffer):
            self.buffer = next(self.chunks, b"")
            self.offset = 0
            if not self.buffer:
                return -1
        return self.buffer[self.offset]

    def take(self, expected):
        if self.peek() != expected:
            raise RuntimeError("checkpoint JSON stream is not canonical or valid")
        self.offset += 1

    def string(self, *, key=False):
        self.take(34)
        raw_key = bytearray(b'"') if key else None
        while True:
            if self.peek() < 0:
                raise RuntimeError("checkpoint JSON stream has an unterminated string")
            match = self.string_special.search(self.buffer, self.offset)
            end = len(self.buffer) if match is None else match.start()
            if raw_key is not None:
                raw_key.extend(self.buffer[self.offset:end])
                # Shapes retain keys, not arbitrary unbounded scalar contents.
                if len(raw_key) > 65_536:
                    raise RuntimeError("checkpoint projection key exceeds verification budget")
            self.offset = end
            if match is None:
                continue
            special = self.buffer[self.offset]
            self.offset += 1
            if special == 34:
                if raw_key is None:
                    return None
                raw_key.append(34)
                return json.loads(raw_key)
            if special != 92:
                raise RuntimeError("checkpoint JSON stream has an unescaped control character")
            escape = self.peek()
            if escape < 0:
                raise RuntimeError("checkpoint JSON stream has a truncated escape")
            self.offset += 1
            escaped = bytes((92, escape)) if escape >= 0 else b""
            if escape == ord('u'):
                digits = bytearray()
                for _ in range(4):
                    char = self.peek()
                    if char < 0:
                        raise RuntimeError("checkpoint JSON stream has a truncated escape")
                    self.offset += 1
                    digits.append(char)
                # ensure_ascii=False uses \u only for unnamed ASCII controls.
                if (digits[:2] != b'00' or digits[2] not in b'01'
                        or digits[3] not in b'0123456789abcdef'
                        or bytes(digits) in {b'0008', b'0009', b'000a', b'000c', b'000d'}):
                    raise RuntimeError("checkpoint JSON stream has a noncanonical escape")
                escaped += digits
            elif escape not in b'"\\bfnrt':
                raise RuntimeError("checkpoint JSON stream has an invalid or noncanonical escape")
            if raw_key is not None:
                raw_key.extend(escaped)

    def scalar(self):
        token = bytearray()
        while self.peek() >= 0:
            match = self.token_end.search(self.buffer, self.offset)
            end = len(self.buffer) if match is None else match.start()
            token.extend(self.buffer[self.offset:end])
            self.offset = end
            if len(token) > 8192:
                raise RuntimeError("checkpoint JSON scalar exceeds verification budget")
            if match is not None:
                break
        try:
            value = json.loads(token)
            valid = not isinstance(value, (dict, list, str)) and canonical_json(value).encode('utf-8') == token
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("checkpoint JSON stream has an invalid scalar") from exc
        if not valid:
            raise RuntimeError("checkpoint JSON stream has a noncanonical scalar")

    def value(self, *, capture=True):
        char = self.peek()
        if char == 123:  # object
            self.take(123)
            shape = {} if capture else None
            previous = None
            if self.peek() != 125:
                while True:
                    key = self.string(key=True)
                    if previous is not None and key <= previous:
                        raise RuntimeError("checkpoint JSON keys are duplicate or noncanonical")
                    previous = key
                    self.take(58)
                    child = self.value(capture=capture)
                    if capture:
                        shape[key] = child
                    if self.peek() == 125:
                        break
                    self.take(44)
            self.take(125)
            return shape
        if char == 91:  # array: validate every element, retain no array contents
            self.take(91)
            if self.peek() != 93:
                while True:
                    self.value(capture=False)
                    if self.peek() == 93:
                        break
                    self.take(44)
            self.take(93)
            return None
        if char == 34:
            return self.string()
        self.scalar()
        return None

    def verify(self):
        shape = self.value()
        if self.peek() != -1:
            raise RuntimeError("checkpoint JSON stream has trailing content")
        return shape

def _reclamation_payload_chunks(body: str):
    """Yield bounded decoded bytes; preserve all original envelope/zlib checks."""
    import codecs
    # The encoded row is already a SQLite value. Only parse an envelope when its
    # reserved tag is present. Canonical uncompressed rows stream without parsing.
    if not body.startswith('{"$roi_current_payload":'):
        for start in range(0, len(body), _CURRENT_PAYLOAD_CHUNK_BYTES):
            yield body[start:start + _CURRENT_PAYLOAD_CHUNK_BYTES].encode('utf-8')
        return
    value = json.loads(body)
    if not isinstance(value, dict) or _CURRENT_PAYLOAD_TAG not in value:
        # Reserved text inside a string is not an envelope.
        del value
        for start in range(0, len(body), _CURRENT_PAYLOAD_CHUNK_BYTES):
            yield body[start:start + _CURRENT_PAYLOAD_CHUNK_BYTES].encode('utf-8')
        return
    if (set(value) != {_CURRENT_PAYLOAD_TAG, 'utf8_bytes', 'data'}
            or value[_CURRENT_PAYLOAD_TAG] != _CURRENT_PAYLOAD_CODEC
            or type(value['utf8_bytes']) is not int
            or not 0 < value['utf8_bytes'] <= _MAX_CURRENT_PAYLOAD_BYTES):
        raise RuntimeError("current-state payload encoding is invalid")
    try:
        expected = value['utf8_bytes']
        compressed = base64.b64decode(value['data'], validate=True)
        del value
        decoder = zlib.decompressobj()
        utf8 = codecs.getincrementaldecoder('utf-8')()
        total = 0
        for offset in range(0, len(compressed), _CURRENT_PAYLOAD_CHUNK_BYTES):
            pending = compressed[offset:offset + _CURRENT_PAYLOAD_CHUNK_BYTES]
            while pending:
                raw = decoder.decompress(pending, _CURRENT_PAYLOAD_CHUNK_BYTES)
                pending = decoder.unconsumed_tail
                if decoder.unused_data or (decoder.eof and pending):
                    raise ValueError("trailing compressed data")
                total += len(raw)
                if total > expected:
                    raise ValueError("oversized compressed data")
                utf8.decode(raw)
                if raw:
                    yield raw
        while True:
            raw = decoder.decompress(b'', _CURRENT_PAYLOAD_CHUNK_BYTES)
            if not raw:
                break
            total += len(raw)
            if total > expected:
                raise ValueError("oversized compressed data")
            utf8.decode(raw)
            yield raw
        utf8.decode(b'', final=True)
        if total != expected or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError("incomplete, oversized, or trailing compressed data")
    except (ValueError, TypeError, zlib.error, UnicodeError) as exc:
        raise RuntimeError("current-state payload encoding is invalid") from exc

def verify_current_payload_for_reclamation(body: str, aggregate) -> tuple[Any, str]:
    """Check decoded semantics in bounded chunks and return only projection keys.

    Every chunk feeds the ORIGINAL canonical JSON digest, not a hash-of-hashes.
    A strict canonical validator makes this equivalent to decode + canonical
    re-encode for writer-produced large sections. Small historical noncanonical
    JSON retains the original decoder behavior. Large noncanonical data stays
    protected rather than receiving unverified deletion authority.
    """
    # Handle small historical representations exactly as the original decoder.
    # For envelopes the compressed size is not the decoded size.
    small = len(body) <= 65_536
    if small and '"$roi_current_payload"' in body:
        envelope = json.loads(body)
        if isinstance(envelope, dict) and _CURRENT_PAYLOAD_TAG in envelope:
            size = envelope.get('utf8_bytes')
            small = type(size) is int and 0 < size <= 65_536
        del envelope
    if small:
        value = decode_current_payload(body)
        digest = hashlib.sha256()
        for fragment in json.JSONEncoder(sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str).iterencode(value):
            data = fragment.encode('utf-8')
            digest.update(data)
            aggregate.update(data)
        def shape(item):
            return {key: shape(child) for key, child in item.items()} if isinstance(item, dict) else None
        return shape(value), digest.hexdigest()
    digest = hashlib.sha256()
    def chunks():
        for data in _reclamation_payload_chunks(body):
            digest.update(data)
            aggregate.update(data)
            yield data
    shape = _ReclamationJSONStream(chunks()).verify()
    return shape, digest.hexdigest()
