# Risk-Conditioned Alpha v5

## Objective

Maximize forward, executable percentage ROI and compounded paper growth without treating probabilistic danger labels as automatic exclusions.

The system remains paper-only. It has no private-key handling, signing, transaction submission, or live-money authority.

## Decision taxonomy

1. **Mechanical hard stops** remain categorical rejects: unavailable sell route, transfer restriction, unexitable liquidity, unavailable exact entry/exit quote, authority capable of blocking transfer/exit, or a linked entity able to remove required exit liquidity.
2. **Hazards** are modeled rather than vetoed: bundled launch, sniper concentration, common funding, creator linkage/distribution, mint authority, early-holder distribution, quote/slippage deterioration, late lifecycle, and high snipe tax.
3. **Alpha signals** are evaluated separately: elite wallet/entity continuation, creator/deployer continuation, independent entity flow, FOMO acceleration, graduation/lifecycle transition, and cross-venue persistence.
4. **Execution drag** is observed directly through exact amount-specific entry/exit quotes, chase, latency, fees/tax, gas and round-trip cost.

Forward promotion uses robust expected log growth, leave-best-trade-out mean, expected shortfall, winner concentration and fraction-scaled drawdown. Hit rate and median return remain diagnostics and are not promotion vetoes.

## Solana venue/lifecycle policy

### Pump.fun bonding curve

Pump.fun remains a residual-continuation source, not a millisecond/first-slot sniping strategy. V5 separates creator, elite-wallet, entity-flow and hazard continuation evidence. The 15% chase boundary remains the baseline while 15–25% and 25–40% are explicit paper challenger bands; >40% is observe-only until forward evidence justifies changing that ceiling.

Bonding-curve progress is not fabricated when the current observation source cannot prove it. The v5 context records the proven Pump.fun bonding-curve lifecycle and leaves unavailable progress dimensions unknown.

### PumpSwap / Pump AMM

PumpSwap is first-class in active v5 evidence with distinct lifecycle states:

- immediate graduation, 0–30 seconds;
- early post-graduation, 30–120 seconds;
- established continuation, 2–5 minutes;
- mature intraday momentum after 5 minutes.

A dedicated graduation-continuation lane competes with elite-wallet, creator, entity-flow and hazard continuation. Historical launch danger is retained in the risk signature while current forward outcomes determine whether that hazard remains profitable.

### Raydium

Raydium evidence stays isolated from Pump.fun/PumpSwap. V5 separates post-Pump migration evidence from native/unproven Raydium behavior and adds a cross-venue persistence lane when the same observed wallet continues buying after earlier Pump evidence. No Raydium success is transferred into Pump authority or vice versa.

## FOMO policy

FOMO is split into **clean FOMO** and **hazard FOMO** forward cohorts.

Creator distribution, early-holder distribution, moderate quote deterioration and moderate exit-slippage deterioration are hazards, not automatic entry blockers. Actual flow exhaustion still exits when transaction frequency and net-buy flow decelerate while buy/sell imbalance turns negative.

Incomplete risk evidence, unknown critical timing, latency beyond the certified 20-second architecture, and extreme chase beyond the current >40% research ceiling remain fail-closed.

The active FOMO position cap remains 5%. Forward expected-log-growth challengers at 7.5% and 10% are recorded for research but do not receive active paper sizing authority yet. Hazard FOMO bootstrap sizing is smaller than clean FOMO sizing.

## Robinhood Chain policy

Robinhood Chain v5 has separate active-paper lanes:

- elite entity continuation;
- creator/deployer continuation;
- independent entity-flow accumulation;
- FOMO continuation;
- lifecycle-transition continuation;
- hazard continuation.

The deployer is no longer an automatic veto and never counts as independent confirmation. Economic-entity resolution remains fail-closed when required.

Pons V2 progress at or above 85% is a lifecycle challenger instead of an automatic rejection while the curve remains in a tradeable phase. Snipe tax above 500 bps is a hazard rather than a standalone veto; exact all-in round-trip economics still govern accessibility. Mature positive contexts can support a larger round-trip-cost ceiling only when their forward residual edge empirically justifies it.

Robinhood records forward mark-to-market observations on every settlement evaluation. After at least 30 closed outcomes in the same lane × venue × lifecycle context, stop, harvest and maximum-hold policy can be learned from forward MFE/MAE and time-to-MFE evidence. Until then, the existing -12% / +30% / 20-minute policy remains the bootstrap fallback.

## Cross-regime paper allocation

`cross_regime_paper_allocator.py` ranks mature Pump.fun, PumpSwap, Raydium, FOMO and Robinhood segments using robust forward expected log growth adjusted for drawdown and expected shortfall.

Unknown correlation is not treated as zero. Until sufficiently aligned forward evidence exists, any single regime is capped at 25% allocator weight and the remainder stays in paper cash. The permanent hard per-regime ceiling is 50%.

## Evidence and compatibility

V5 writes its own release-bound forward trial/outcome evidence while preserving the existing v4 final-trial tables required by FOMO and audit tooling. The Solana v5 buy wrapper produces one exact execution snapshot and mirrors it into compatibility evidence, avoiding duplicate quote/RPC fanout for the same observation.

Historical evidence remains audit-only and cannot promote a v5 context.

## Unchanged safety and production boundaries

- paper-only authority;
- no signing;
- no transaction submission;
- no private-key or seed-phrase surface;
- no live-money authority;
- existing Solana continuity and recovery architecture remains authoritative;
- no first-slot/millisecond Pump.fun execution assumption;
- no cross-venue or cross-chain success transfer without exact-context forward evidence.
