from __future__ import annotations

from .storage_retention import RetentionClass, RetentionContract


def _f(
    dataset: str,
    owner: str,
    retention_class: RetentionClass,
    purpose: str,
    consumer: str,
    *,
    hot_or_cold: str,
    age: str | None = None,
    bytes_: int | None = None,
    archive: str = "none",
    prune: str,
    startup: bool = False,
    certification: bool = False,
) -> RetentionContract:
    return RetentionContract(
        dataset=dataset,
        owner=owner,
        retention_class=retention_class,
        purpose=purpose,
        consumer=consumer,
        hot_or_cold=hot_or_cold,
        max_hot_age=age,
        max_hot_rows=None,
        max_hot_bytes=bytes_,
        archive_policy=archive,
        prune_condition=prune,
        startup_access=startup,
        certification_access=certification,
    )


# Durable filesystem artifacts are intentionally separate from the SQLite table
# allowlist so a cold file can never become a startup table merely by being
# registered. Together with RETENTION_REGISTRY this is the positive-retention
# registry for every artifact introduced by the active-storage architecture.
_FILE_CONTRACTS = (
    _f(
        "active_rollover_request",
        "storage",
        RetentionClass.TEMPORARY,
        "Restart-safe request to perform a quiescent verified active-epoch rollover",
        "active runtime startup",
        hot_or_cold="hot",
        age="until next successful rollover or explicit operator remediation",
        bytes_=1_048_576,
        archive="none",
        prune="delete only after canonical successor is atomically installed and reverified",
        startup=True,
        certification=False,
    ),
    _f(
        "sealed_active_epoch",
        "storage-maintenance",
        RetentionClass.LEGACY_UNCLASSIFIED,
        "Preserve the exact prior active epoch while independent retention classification/purge remains incomplete",
        "storage maintenance only",
        hot_or_cold="cold",
        archive="not a permanent archive; classify retained value independently before purge",
        prune="explicit classification plus operator-approved purge; never automatic runtime recovery",
        startup=False,
        certification=False,
    ),
    _f(
        "sealed_epoch_reclamation_receipt",
        "storage-maintenance",
        RetentionClass.CURRENT_STATE,
        "Crash-safe intent/final receipt for the one exact operator-approved sealed epoch reclamation",
        "production cleanup bootstrap/storage operations",
        hot_or_cold="hot",
        age="current checkpoint only",
        bytes_=1_048_576,
        archive="none",
        prune="atomically replace when a later verified checkpoint is explicitly approved for reclamation",
        startup=True,
        certification=False,
    ),
)

PERSISTENT_FILE_RETENTION_REGISTRY: dict[str, RetentionContract] = {
    contract.dataset: contract for contract in _FILE_CONTRACTS
}


def file_contract_for(dataset: str) -> RetentionContract:
    try:
        return PERSISTENT_FILE_RETENTION_REGISTRY[dataset]
    except KeyError as exc:
        raise ValueError(f"unregistered persistent file dataset: {dataset}") from exc


def assert_persistent_file_registered(dataset: str) -> None:
    file_contract_for(dataset)


def validate_file_registry() -> None:
    if len(PERSISTENT_FILE_RETENTION_REGISTRY) != len(_FILE_CONTRACTS):
        raise ValueError("duplicate persistent file retention dataset")
    for contract in _FILE_CONTRACTS:
        if not contract.purpose.strip() or not contract.consumer.strip():
            raise ValueError(f"file dataset {contract.dataset} lacks concrete purpose/consumer")
        if contract.hot_or_cold not in {"hot", "cold"}:
            raise ValueError(f"file dataset {contract.dataset} has invalid hot_or_cold")
        if contract.dataset == "sealed_active_epoch":
            if contract.retention_class is not RetentionClass.LEGACY_UNCLASSIFIED:
                raise ValueError("sealed active epoch must remain legacy-unclassified until independently classified")
            if contract.startup_access or contract.certification_access:
                raise ValueError("sealed active epoch cannot be a runtime/certification dependency")
        if contract.dataset == "sealed_epoch_reclamation_receipt":
            if contract.retention_class is not RetentionClass.CURRENT_STATE:
                raise ValueError("sealed epoch reclamation receipt must be bounded current state")
            if not contract.startup_access or contract.certification_access:
                raise ValueError("sealed epoch reclamation receipt is startup-only operational state")


validate_file_registry()


__all__ = [
    "PERSISTENT_FILE_RETENTION_REGISTRY",
    "assert_persistent_file_registered",
    "file_contract_for",
    "validate_file_registry",
]
