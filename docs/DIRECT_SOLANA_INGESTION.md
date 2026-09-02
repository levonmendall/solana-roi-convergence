# Direct Solana ingestion

This production data plane preserves the ROI Convergence v3.1 strategy and its frozen market universe while removing paid enhanced-webhook interpretation from the critical path.

## Scope invariant

The runtime permanently observes all seven frozen program IDs at `processed` commitment:

- Pump.fun bonding curve
- Pump AMM / PumpSwap
- Raydium AMM v4
- Raydium CPMM
- Raydium CLMM
- Raydium Stable AMM
- Raydium LaunchLab

The three frozen S-tier scout wallets are subscribed independently on the same two providers. Scout subscriptions change prioritization only; they do not define or narrow the information universe.

## Fast path

1. Two independent standard Solana WebSocket connections run concurrently.
2. Every frozen program and scout uses `logsSubscribe` at `processed` commitment.
3. A compact receipt `(signature, slot, source, received_at)` is durably journaled immediately.
4. Scout signatures receive hydration priority 0.
5. If the fastest RPC has not completed by the configured hedge delay, the same read is issued to the second RPC and the first valid result wins.
6. `getTransaction(jsonParsed, confirmed)` supplies authoritative balances/accounts.
7. Local normalization derives wallet, mint, side, token amount, SOL/WSOL amount and execution reference price. Ambiguous transactions fail closed.
8. Candidate launch-window evidence is reconstructed from signatures that were already observed prospectively in the compact receipt journal.
9. The unchanged six-dimensional risk path runs.
10. The unchanged Jupiter amount-specific quote and unsigned mainnet simulation run.
11. Only the unchanged final activation gate can authorize a paper entry.

No signer, private key, transaction-send method or live-money authority exists in this path.

## Depth without a paid firehose

Every full-market notification is observed and durably accounted for. Expensive confirmed-transaction hydration is selective:

- every frozen scout transaction is hydrated;
- launch-like program events are hydrated;
- a deterministic program sample is hydrated while prospective coverage is incomplete;
- a lower-rate deterministic audit sample remains after coverage is certified;
- when a relevant token needs launch/early-buyer context, all already-observed signatures in its exact launch window are hydrated concurrently before the risk decision, subject to the existing latency/freshness gate.

Sampling therefore controls expensive RPC work, not market observation. It does not change which programs or opportunities the strategy is allowed to see.

## Durability and continuity

The SQLite receipt journal stores recent per-signature observations for the candidate reconstruction horizon and permanent per-minute source counts with rolling SHA-256 digests. This prevents a high-volume market stream from expanding the database without bound while retaining evidence that the complete source stream was observed.

If both WebSocket providers are simultaneously unavailable, the runtime opens an outage interval and the forward-cohort continuity gate fails closed. On reconnection, every watched program and scout is backfilled with bounded `getSignaturesForAddress` pagination. If the outage boundary cannot be reached, `unresolved_gap=true` remains and the cohort cannot freeze or arm.

Recovered historical swaps repair chronology only. They are never processed as fresh candidates and are excluded from direct hydration latency telemetry, preventing a reconnect from creating a late paper trade or artificially improving prospective latency statistics.

Provider `connected` state is reset on every process start; liveness cannot survive a crashed process as stale database state.

## Cost and provider independence

The default blueprint uses two independent public standard RPC/WebSocket endpoints and requires no Helius credential. `SOLANA_ROI_RPC_ENDPOINTS_JSON` can replace them with other standard Solana endpoints without changing strategy code.

Jupiter remains a separate amount-specific quote/unsigned-transaction provider. Changing the Solana transport does not change the strategy, risk rules, sizing, exits or certification thresholds.

## Certification remains unchanged

The direct transport receives no special exemption. Before the first paper trade it must prospectively satisfy the same gates already frozen for v3.1, including:

- at least 100 program coverage observations;
- at least 10 normalized swaps each for PUMP_FUN, PUMP_AMM and RAYDIUM;
- at least 95% launch-near-creation, early-buyer-complete and funding-complete fractions;
- zero first-touch chronology conflicts;
- at least 100 candidate latency observations with the existing p95/p99 ceilings;
- at least 100 usable-quote observations with the existing Jupiter latency ceilings;
- at least 100 unsigned shadow simulations with the existing success/latency/size limits;
- complete fresh six-dimensional risk evidence;
- exact-release binding, valid event chain, untouched $500 genesis and continuous paper clock.

`direct_full_scope_stream_continuity` is an additional operational readiness requirement. It strengthens fail-closed behavior; it does not relax any previous gate.
