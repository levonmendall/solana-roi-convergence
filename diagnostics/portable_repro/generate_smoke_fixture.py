#!/usr/bin/env python3
"""Generate a deterministic *smoke-only* canonical SQLite fixture.

This deliberately uses the canonical ObservationEventStore constructor so schema
and indexes come from production code rather than a duplicated hand-written schema.
The output is only for harness validation. It is NOT production-scale evidence and
must never satisfy a production-target fidelity manifest by itself.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from solana_roi.observation_store import ObservationEventStore

SMOKE_FIXTURE_VERSION = "portable-repro-smoke-v1"
SEED = 920016


def _wallet(i: int) -> str:
    return f"wallet-{i:04d}-{hashlib.sha256(f'w:{SEED}:{i}'.encode()).hexdigest()[:12]}"


def _mint(i: int) -> str:
    return f"mint-{i:04d}-{hashlib.sha256(f'm:{SEED}:{i}'.encode()).hexdigest()[:12]}"


def build_fixture(path: Path, *, wallets: int = 12, tokens: int = 24, swaps_per_wallet: int = 8) -> dict[str, object]:
    if wallets <= 0 or tokens <= 0 or swaps_per_wallet <= 0:
        raise ValueError("wallets, tokens and swaps_per_wallet must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()

    store = ObservationEventStore(path)
    try:
        for i in range(wallets):
            wallet = _wallet(i)
            store.upsert_wallet_profile(
                wallet=wallet,
                entity_id=f"entity-{i // 2:04d}",
                tier="S" if i % 3 == 0 else "A",
                first_touch_sample_size=20 + i,
                historically_eligible=True,
                updated_at=f"2026-09-13T00:{i:02d}:00+00:00",
            )

        touch_seen: set[str] = set()
        swap_index = 0
        for i in range(wallets):
            wallet = _wallet(i)
            for j in range(swaps_per_wallet):
                token_index = (i * swaps_per_wallet + j) % tokens
                mint = _mint(token_index)
                minute = swap_index % 60
                second = (swap_index * 7) % 60
                observed = f"2026-09-13T01:{minute:02d}:{second:02d}+00:00"
                received = f"2026-09-13T01:{minute:02d}:{min(second + 1, 59):02d}+00:00"
                signature = hashlib.sha256(f"sig:{SEED}:{swap_index}".encode()).hexdigest()
                store.record_swap(
                    signature=signature,
                    slot=350_000_000 + swap_index,
                    observed_at=observed,
                    received_at=received,
                    wallet=wallet,
                    token_mint=mint,
                    side="buy" if j % 2 == 0 else "sell",
                    token_amount=1000.0 + swap_index,
                    native_amount_sol=0.25 + (swap_index % 10) * 0.01,
                    reference_price_sol=0.00025 + (swap_index % 17) * 0.000001,
                    ingestion_latency_ms=float(15 + (swap_index % 40)),
                    source="portable-smoke",
                )
                if mint not in touch_seen:
                    store.claim_first_touch(
                        token_mint=mint,
                        signature=signature,
                        wallet=wallet,
                        entity_id=f"entity-{i // 2:04d}",
                        tier="S" if i % 3 == 0 else "A",
                        observed_at=observed,
                        reference_price_sol=0.00025,
                    )
                    touch_seen.add(mint)
                swap_index += 1

        for i in range(tokens):
            mint = _mint(i)
            observed = f"2026-09-13T02:{i % 60:02d}:00+00:00"
            received = f"2026-09-13T02:{i % 60:02d}:01+00:00"
            store.record_risk_evidence(
                token_mint=mint,
                dimension="liquidity",
                observed_at=observed,
                received_at=received,
                source="portable-smoke",
                payload={"depth_sol": 25.0 + i, "seed": SEED},
            )
            store.record_price_mark(
                token_mint=mint,
                observed_at=observed,
                received_at=received,
                price_sol=0.00025 + i * 0.000001,
                source="portable-smoke",
                source_ref=f"smoke-{i}",
            )
            store.record_program_coverage(
                token_mint=mint,
                pair_created_at="2026-09-13T00:00:00+00:00",
                assessed_at=received,
                launch_lag_ms=float(100 + i),
                launch_near_creation=True,
                early_buy_count=3 + (i % 4),
                early_buyer_count=2 + (i % 3),
                early_buyers_complete=True,
            )

        counts = store.evidence_counts()
    finally:
        store.close()

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "fixture_version": SMOKE_FIXTURE_VERSION,
        "purpose": "HARNESS_SMOKE_ONLY_NOT_CAUSAL_REPRODUCTION",
        "seed": SEED,
        "path": str(path),
        "sha256": digest,
        "wallets_requested": wallets,
        "tokens_requested": tokens,
        "swaps_per_wallet": swaps_per_wallet,
        "counts": counts,
        "production_scale_claimed": False,
        "fidelity_classification": "SYNTHETIC STRUCTURAL SMOKE FIXTURE",
        "canonical_schema_sources": [
            "solana_roi.storage.AppendOnlyEventStore",
            "solana_roi.observation_store.ObservationEventStore",
        ],
    }
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--wallets", type=int, default=12)
    parser.add_argument("--tokens", type=int, default=24)
    parser.add_argument("--swaps-per-wallet", type=int, default=8)
    args = parser.parse_args()
    manifest = build_fixture(
        Path(args.output),
        wallets=args.wallets,
        tokens=args.tokens,
        swaps_per_wallet=args.swaps_per_wallet,
    )
    out = Path(args.manifest_output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
