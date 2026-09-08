# v5.2 Batch 2 — Lossless Candidate Accounting

## Purpose

Batch 2 adds a research/test-only accounting boundary across the existing five-lane
capability seam:

- Pump.fun (`pump_fun`)
- Pump AMM / PumpSwap (`pump_amm`)
- Raydium (`raydium`)
- FOMO (`fomo`)
- Robinhood (`robinhood`)

The objective is narrow: once a candidate enters the Batch 2 accounting boundary,
it may not silently disappear. Every observed candidate must remain explicitly
active or carry one valid terminal disposition.

## Hard readiness invariant

The status surface exposes:

`unexplained_disappearance_count`

Certification readiness is **false** whenever that count is greater than zero.

This is a fail-closed accounting gate. It does not make a candidate attractive,
change a trading threshold, authorize entry, or promote the v5.2 challenger.

## Valid accounting outcomes

An observed candidate is accounted when exactly one current record proves one of:

1. the candidate remains active; or
2. the candidate is terminal with an explicit disposition:
   - `exited`
   - `permanently_rejected`
   - `expired`
   - `invalidated`

Temporary blockers are not terminal; the candidate must remain actively tracked.

## Fail-closed cases

Readiness also fails for accounting ambiguity or corruption, including:

- missing/blank candidate identity;
- unknown canonical lane;
- duplicate observed identity;
- duplicate or conflicting accounting records;
- candidate lane conflict;
- active record carrying a terminal disposition;
- missing or unknown terminal disposition;
- accounting records for candidates absent from the observed population;
- incomplete five-lane capability matrix.

These errors are not folded into the disappearance count. The disappearance metric
remains an exact measure of observed candidates that lack a valid accounting record,
while malformed/unexpected ledger state separately blocks readiness.

## Conservation and lane visibility

The reconciler publishes both aggregate and per-lane counts for:

- observed candidates;
- active candidates;
- terminal candidates;
- accounted candidates;
- unexplained disappearances;
- unexpected candidate records.

Internal conservation checks require the per-lane and aggregate totals to agree.

## Integration boundary

`run_five_lane_accounting_matrix(...)` calls the existing synthetic
`run_five_lane_capability_matrix(...)` unchanged and attaches the Batch 2 accounting
gate. The v5.1 capability semantics remain intact.

`solana_roi.v52_lossless_candidate_accounting` is explicitly classified `test_only`
in `module_reachability_policy.json`. There is no production composition hook.

## Authority lock

Batch 2 preserves all existing safety constraints:

- v5.1 remains the incumbent strategy authority;
- v5.2 challenger entry authority is false;
- research-only;
- paper-only;
- live-money authority is false;
- signing is unavailable;
- transaction submission is unavailable;
- economic thresholds are unchanged;
- strategy authority is unchanged.

This completes the Batch 2 accounting primitive only. It does not by itself certify
the five lanes economically or promote v5.2 into production.
