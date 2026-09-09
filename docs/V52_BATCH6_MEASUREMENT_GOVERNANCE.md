# v5.2 Batch 6 — Measurement and Governance

## Scope

Batch 6 completes the six-batch v5.2 challenger design with a research/test-only measurement and governance layer. It does not change v5.1 production authority, economic thresholds, production composition, signing, submission, or live-money capability.

The batch implements:

1. an executable missed-opportunity measurement boundary;
2. executable MFE/MAE after costs from the first legitimate executable opportunity;
3. realized-to-MFE capture ratio;
4. detection capture ratio;
5. actionability-conversion accounting;
6. explicit opportunity/regret decomposition;
7. deterministic challenger hypothesis freezing;
8. identical-stream v5.1-vs-v5.2 forward comparison;
9. incremental-forward-alpha measurement for newly introduced signals.

## Executable measurement boundary

Headline market moves are not accepted as opportunity evidence. A measurement path begins at the first observation that satisfies the existing execution safety envelope:

- exact amount-specific buy quote available;
- exact amount-specific sell quote available;
- structurally exitable;
- latency at or below the existing 20-second hard maximum;
- candidate-relative chase at or below 40% for a legitimate entry opportunity.

MFE/MAE is then calculated only from executable after-cost snapshots. A theoretical, stale, unquotable, or structurally unexitable peak cannot inflate executable MFE.

## Executable MFE / MAE

For each candidate or position, Batch 6 records:

- first legitimate executable timestamp/index;
- number of executable observations;
- number of non-executable observations after the first legitimate opportunity;
- executable maximum favorable excursion (MFE);
- executable maximum adverse excursion (MAE);
- realized executable net return when a settlement exists;
- capture ratio when MFE is positive.

The per-position capture ratio is:

`realized executable net return / executable MFE`

This is a measurement KPI only; it does not authorize parameter tuning.

## Detection capture ratio

Detection timing is measured as:

`remaining executable upside at detection / total executable forward upside`

This separates “the system found the winner” from “the system found the winner while meaningful executable upside still remained.”

## Actionability conversion

Batch 6 counts the complete forward lifecycle population:

- discovered;
- developing;
- pre-actionable;
- temporary reject;
- reactivated;
- actionable;
- entered;
- successful position.

It also measures how many temporary rejects later became profitable actionable opportunities. This is intended to show whether candidate continuity and automatic reactivation are working.

## Opportunity-capture / regret decomposition

Every material missed executable opportunity can be attributed to one or more explicit causes:

- not discovered;
- discovered too late;
- candidate continuity failure;
- graduation handoff failure;
- insufficient monitoring priority;
- quote failure;
- chase restriction;
- evidence maturity;
- structural hard stop;
- hazard sizing;
- undersizing;
- failed pyramid;
- premature exit;
- runner too small;
- failed re-entry;
- correct avoidance.

`correct_avoidance` cannot carry positive missed-alpha attribution. This prevents legitimate safety rejections from being silently counted as strategy regret.

Because a missed opportunity may have more than one contributing cause, per-cause alpha attribution is intentionally non-additive. The canonical total missed executable alpha is the candidate-level total, not the sum of overlapping cause buckets.

## Frozen challenger governance

The challenger remains:

- `roi-convergence-v5.2-continuation-capture-1`
- epoch `v52-weekend-review-20260908`
- freeze id `v52-weekend-review-20260908-frozen-challenger-1`

The frozen feature set covers the six v5.2 batches:

1. candidate continuity;
2. early detection;
3. graduation execution;
4. upside capture;
5. detection intelligence;
6. measurement/governance.

The freeze manifest receives a deterministic SHA-256 fingerprint. Batch 6 has no authority to mutate strategy parameters from measured results and no authority to promote historical/weekend evidence.

## Identical-stream v5.1-vs-v5.2 comparison

v5.1 stays the incumbent control. v5.2 is compared only on the exact same forward candidate identities, lanes, and stream order. A mismatch fails closed.

The comparison reports:

- compounded return after costs;
- geometric growth;
- mean detection latency;
- actionable-opportunity recall;
- aggregate capture ratio;
- executable MFE captured;
- maximum drawdown;
- expected shortfall;
- mean slippage;
- losing-trade frequency;
- reject regret;
- exit regret;
- capital utilization.

The comparison result has no promotion authority. Economic promotion requires a separate explicit governance decision after sufficient stationary forward evidence.

## Incremental alpha requirement for new signals

Any added feature such as wallet cascades, graduation probability, concentration direction, or liquidity acceleration must be evaluated on paired forward candidates against a baseline without that feature. Insufficient forward sample fails closed. A positive measured delta remains research evidence only and cannot auto-promote the signal or mutate production parameters.

## Preserved safety boundary

Batch 6 preserves:

- v5.1 authoritative production control;
- paper-only operation;
- no signing;
- no transaction submission;
- no live-money authority;
- 20-second hard operational maximum;
- >40% chase observe-only for the current impulse;
- exact amount-specific entry and exit quotes;
- structural exit hard stops;
- hazards as evidence/sizing modifiers unless a structural hard stop is crossed;
- no averaging down;
- no production composition hook.

## Acceptance gate

Batch 6 is technically complete only when:

1. focused executable-MFE/MAE and detection-capture tests pass;
2. headline/non-executable price moves are proven unable to inflate measurement;
3. actionability conversion and regret attribution tests pass;
4. frozen comparison rejects different candidate populations or stream order;
5. incremental-signal measurement fails closed on insufficient forward sample;
6. all five canonical lanes are supported without granting authority;
7. the module is classified test-only by repository reachability policy;
8. full required CI passes on the PR head;
9. the PR is merged;
10. full required CI passes again on the exact post-merge `main` SHA.

Technical completion of Batch 6 completes the v5.2 design implementation, but it does **not** establish that v5.2 is economically superior to v5.1. The frozen challenger must now accumulate stationary forward evidence on the same candidate stream before any promotion decision.
