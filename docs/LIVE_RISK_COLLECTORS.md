# Live Risk Collectors

This stage connects concrete public/live data collectors to the fail-closed risk plane while keeping the $500 forward paper cohort disabled.

## Automated dimensions

- `authority`: Helius Solana RPC `getAccountInfo` with `jsonParsed`; mint or freeze authority remaining active is a veto.
- `liquidity`: DEX Screener token-pairs endpoint; the deepest single Solana pool is used rather than summing unrelated pools.
- `flow`: only normalized swaps received before the decision timestamp are used. The first heuristic flags an early buyer exiting and high sell-share pressure; insufficient observations leave the dimension missing.
- `deployer`: bounded Helius mint-address history. The creator is recorded only when pagination reaches the earliest transaction; otherwise deployer evidence remains missing.

## Still fail closed

`launch` and `funding` are intentionally not synthesized. Until authoritative point-in-time collectors exist for bundled/sniper launch structure and early-wallet funding provenance, a complete `RiskSnapshot` cannot be produced from live collectors alone.

## No-lookahead repair

The original eligible tracked first touch is now persisted immediately, before risk completeness is evaluated. If risk is incomplete at that moment, the trade remains blocked, but a later wallet cannot be relabeled as the first touch. This preserves the forward-test chronology.

## Runtime boundary

Live risk collection is shadow-only. `paper_signal_promotion_enabled` remains false. No private keys, transaction construction, signing, order submission, custody, deposits, withdrawals, or live-money authority exist.
