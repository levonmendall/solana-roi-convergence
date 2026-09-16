from .active_storage import verify_current_payload_for_reclamation

def _validate_compact_checkpoint_metadata(payload: Mapping[str, Any]) -> None:
    missing = sorted(_COMPACT_REQUIRED_CHECKPOINT_FIELDS - set(payload))
    if missing:
        raise RuntimeError("active checkpoint missing required fields: " + ", ".join(missing))
    if str(payload.get("sections_storage") or "") != _COMPACT_SECTIONS_STORAGE:
        raise RuntimeError("active checkpoint compact semantic storage mismatch")
    embedded = sorted(section for section in _SEMANTIC_SECTIONS if section in payload)
    if embedded:
        raise RuntimeError("active checkpoint compact payload embeds semantic sections: " + ", ".join(embedded))
    section_hashes = payload.get("section_hashes")
    if not isinstance(section_hashes, Mapping):
        raise RuntimeError("active checkpoint section hashes missing")

def _validate_compact_checkpoint_shape(
    payload: Mapping[str, Any], *, logical_truth: Mapping[str, Any]
) -> None:
    _validate_compact_checkpoint_metadata(payload)
    section_hashes = payload["section_hashes"]
    for section in _SEMANTIC_SECTIONS:
        if section not in logical_truth:
            raise RuntimeError(f"active checkpoint semantic section missing: {section}")
        if str(section_hashes.get(section) or "") != payload_hash(logical_truth[section]):
            raise RuntimeError(f"active checkpoint section hash mismatch: {section}")
    if str(payload.get("semantic_hash") or "") != semantic_hash(logical_truth):
        raise RuntimeError("active checkpoint semantic hash mismatch")

def _checkpoint_projection_shape(value: Any) -> Any:
    """Retain only the dictionary keys used by historical schema projection.

    Reclamation projects dictionaries recursively, but compares lists/scalars in
    full from the source extraction. No list element or scalar from a checkpoint
    is required after its complete semantic digest has been verified.
    """
    if isinstance(value, dict):
        return {key: _checkpoint_projection_shape(child) for key, child in value.items()}
    return None

def _verify_checkpoint_section_shapes(
    conn: sqlite3.Connection, payload: Mapping[str, Any]
) -> tuple[dict[str, Any], str]:
    """Verify EVERY section in one pinned read transaction, keeping one at a time.

    The aggregate is the original canonical JSON byte stream, not a hash of
    section hashes. Row hashes, logical section hashes and the aggregate seal
    are all independently checked before the caller may use the small shapes.
    """
    aggregate = hashlib.sha256()
    aggregate.update(b"{")
    shapes: dict[str, Any] = {}
    for index, section in enumerate(sorted(_SEMANTIC_SECTIONS)):
        if index:
            aggregate.update(b",")
        aggregate.update(canonical_json(section).encode("utf-8") + b":")
        table, key_column, key = _ACTIVE_SECTION_ROWS[section]
        row = conn.execute(
            f'SELECT payload_json,payload_hash FROM "{table}" WHERE "{key_column}"=?', (key,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"active checkpoint semantic section missing: {section}")
        body = str(row[0])
        stored = hashlib.sha256()
        for start in range(0, len(body), 65_536):
            stored.update(body[start:start + 65_536].encode("utf-8"))
        if stored.hexdigest() != str(row[1]):
            raise RuntimeError(f"active checkpoint semantic section payload hash mismatch: {section}")
        try:
            shape, digest = verify_current_payload_for_reclamation(body, aggregate)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"active checkpoint semantic section is not valid JSON: {section}") from exc
        if digest != str(payload["section_hashes"].get(section) or ""):
            raise RuntimeError(f"active checkpoint section hash mismatch: {section}")
        shapes[section] = shape
        del body, row, shape
    aggregate.update(b"}")
    return shapes, aggregate.hexdigest()

def load_verified_checkpoint(
    path: Path | str, *, expected_release_sha: str | None = None
) -> dict[str, Any]:
    """Return the complete verified checkpoint for callers that need its values."""
    return _load_verified_checkpoint(path, expected_release_sha=expected_release_sha)

def load_verified_checkpoint_for_reclamation(
    path: Path | str, *, expected_release_sha: str | None = None
) -> dict[str, Any]:
    """Return verified metadata and projection SHAPES, never runtime state.

    This reclamation-only view retains all original verification obligations but
    releases each decoded section immediately. It must not hydrate an engine or
    replace load_verified_checkpoint for runtime consumers of semantic values.
    """
    return _load_verified_checkpoint(path, expected_release_sha=expected_release_sha, shape_only=True)

def _load_verified_checkpoint(
    path: Path | str, *, expected_release_sha: str | None = None,
    shape_only: bool = False,
) -> dict[str, Any]:
    active_path = Path(path)
    if not active_path.is_file():
        raise RuntimeError(f"active storage unavailable: {active_path}")
    uri = f"file:{active_path.resolve()}?mode=ro&cache=private"
    try:
        with closing(sqlite3.connect(uri, uri=True, timeout=5.0)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            row = conn.execute(
                "SELECT payload_json,payload_hash,semantic_hash,schema_version,migration_version,release_sha "
                "FROM checkpoint_current WHERE verified=1 ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("active storage has no verified continuation checkpoint")
            body = str(row[0])
            if hashlib.sha256(body.encode("utf-8")).hexdigest() != str(row[1]):
                raise RuntimeError("active checkpoint payload hash mismatch")
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise RuntimeError("active checkpoint is not valid JSON") from exc
            if not isinstance(payload, dict):
                raise RuntimeError("active checkpoint has invalid shape")
            migration_version = int(payload.get("migration_version", -1))
            if migration_version == TRANSITION_MIGRATION_VERSION and shape_only:
                if int(payload.get("schema_version", -1)) != ACTIVE_SCHEMA_VERSION:
                    raise RuntimeError("active checkpoint storage schema version mismatch")
                _validate_compact_checkpoint_metadata(payload)
                shapes, observed_semantic_hash = _verify_checkpoint_section_shapes(conn, payload)
                if str(payload.get("semantic_hash") or "") != observed_semantic_hash:
                    raise RuntimeError("active checkpoint semantic hash mismatch")
                materialized = dict(payload)
                materialized.update(shapes)
            elif migration_version == TRANSITION_MIGRATION_VERSION:
                logical_truth = _read_active_semantic_sections(conn)
                _validate_checkpoint_shape(payload, logical_truth=logical_truth)
                observed_semantic_hash = semantic_hash(logical_truth)
                materialized = dict(payload)
                materialized.update(logical_truth)
            else:
                _validate_checkpoint_shape(payload)
                observed_semantic_hash = semantic_hash(payload)
                materialized = dict(payload)
                if shape_only:
                    for section in _SEMANTIC_SECTIONS:
                        materialized[section] = _checkpoint_projection_shape(payload[section])
    except sqlite3.Error as exc:
        raise RuntimeError("active storage checkpoint unreadable") from exc
    if observed_semantic_hash != str(row[2]):
        raise RuntimeError("active checkpoint stored semantic hash mismatch")
    if int(row[3]) != ACTIVE_SCHEMA_VERSION or int(row[4]) != migration_version:
        raise RuntimeError("active checkpoint persisted version mismatch")
    if str(row[5]) != str(payload["release_sha"]):
        raise RuntimeError("active checkpoint release metadata mismatch")
    if expected_release_sha is not None and str(payload["release_sha"]) != str(expected_release_sha):
        if not _normal_active_release_rollforward(expected_release_sha):
            raise RuntimeError("active checkpoint release SHA does not match running release")
    return materialized
