# FOMO Continuation Shadow

`fomo_continuation_shadow` is a production-shadow, paper-only research lane. It does not mutate the active wallet cohort, strategy authority, tracking allocation, signing, submission, or live-money boundaries.

The lane models FOMO as an on-chain market state rather than a venue. It can therefore be evaluated on Pump.fun, Pump AMM/PumpSwap, Raydium, or any other venue already identified by the wallet-context router.

## Feature family

The first production version measures:

- independent buyer acceleration over short and longer windows;
- transaction-frequency acceleration;
- net buy-flow acceleration;
- buy/sell imbalance;
- persistence of independent demand;
- participation by wallets/entities already classified for momentum or confirmation roles when that metadata is available;
- creator accumulation versus distribution;
- early-holder distribution;
- chase distance;
- signal-to-entry latency;
- quote deterioration when supplied by point-in-time opportunity evidence;
- executable-depth growth when supplied;
- exit-slippage deterioration when supplied.

The classifier emits `no_fomo`, `pre_fomo`, `active_fomo`, or `late_or_inaccessible_fomo`. Missing timing, chase, or risk evidence fails closed. The existing 15% chase ceiling and 20-second strategy ceiling are unchanged.

## Forward evidence

The lane persists observations and outcomes separately from active strategy lanes and reports residual ROI in percentage units, median ROI, trimmed ROI excluding the best 1/3/5 outcomes, positive-rate percentage, and signal-decay buckets at 1/2/5/10/20/30/60 seconds.

Historical evidence has no promotion authority. The lane has zero automatic strategy authority; any future promotion requires a separate prospective governance decision after adequate forward evidence.

## Separation from scout semantics

FOMO/momentum observations never become additional scout confirmations. Wallet context routing remains `entity × venue × lifecycle × role × regime`, and the FOMO state is an overlay on that context rather than a replacement for it.
