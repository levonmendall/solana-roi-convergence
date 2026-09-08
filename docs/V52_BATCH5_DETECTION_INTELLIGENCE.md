# v5.2 Batch 5 — Detection Intelligence

## Scope

Batch 5 implements the detection-intelligence layer from the frozen v5.2 challenger design. It is research/test-only and covers the canonical five-lane seam:

1. Pump.fun
2. Pump AMM / PumpSwap
3. Raydium
4. FOMO
5. Robinhood Chain

It does not create a production decision path, alter v5.1 authority, or grant any wallet, creator, funder, trajectory, seller-pressure, or cohort signal trading authority.

## Independent flow and participation quality

The Batch 5 flow model distinguishes raw activity from economically independent participation.

It measures:

- unique buyers;
- unique funding clusters;
- buyer breadth after common-funding collapse;
- repeat buyers;
- buy and sell notionals;
- dollar-weighted buy/sell imbalance;
- buy-notional-weighted historical wallet quality;
- an independent-buy-notional fraction that prevents one funding cluster from multiplying its apparent importance by splitting across wallets.

Participation quality combines independent breadth, independent notional, funding independence, repeat buying, historical wallet quality, holder growth, concentration dispersion, and concentration direction. The aggregate score is research-only.

## Wallet cascades

A wallet cascade requires both:

- an ordered sequence of distinct high-quality funding clusters; and
- broader independent-cluster follow-through.

Many wallets sharing one or two funding clusters cannot satisfy the cascade condition merely by producing many transactions.

All cascade parameters remain prospective experiment inputs. A cascade is observation evidence only and has no direct trade authority.

## Bidirectional wallet discovery

Successful candidates may surface previously unknown wallets, but newly discovered wallets start with:

- `initial_signal_weight = 0.0`;
- `prospective_validation_required = true`;
- observation authority only;
- no trading authority.

This prevents a weekend winner or one successful token from retrospectively promoting a wallet into an authoritative signal.

## Creator/funder propagation

Creator/funder relationships can raise observation priority only after incremental alpha has been prospectively validated.

Any validated priority increment decays by an explicit half-life. If incremental alpha is not validated, the propagated increment is zero.

Creator/funder propagation never has trading authority.

## Liquidity trajectory

Liquidity is treated as both risk evidence and directional information. Batch 5 measures:

- liquidity growth;
- executable sell-depth growth;
- liquidity / market-cap ratio trajectory;
- sell-depth / position-size ratio;
- change in slippage at constant notional.

The model distinguishes supportive, deteriorating, stable, and mixed trajectories.

## Concentration trajectory

Batch 5 measures direction rather than judging only a point estimate.

A path such as:

`60% -> 48% -> 37% -> 27%`

is classified as healthy distribution, while:

`18% -> 27% -> 41% -> 58%`

is classified as increasing concentration.

The result is evidence only; it cannot bypass structural hard stops.

## Seller pressure

The seller-pressure model measures:

- dollar-weighted buy vs sell flow;
- median buy and sell size;
- upper-percentile sell size;
- largest seller activity and share;
- repeated seller behavior;
- early-wallet distribution;
- LP withdrawal;
- negative price response associated with selling.

The resulting pressure score is research evidence for sizing/exit analysis and cannot independently authorize a trade.

## Dynamic hazard direction

Hazards are evaluated using:

- current severity;
- severity slope;
- worsening persistence;
- liquidity interaction;
- participation-quality interaction.

Improving hazard direction may strengthen research confidence. Deteriorating hazard direction may weaken it. Hazard direction can never override an existing structural hard stop.

## Cohort-relative anomalies

Candidates are compared only against exact comparable cohorts across:

- token-age bucket;
- venue;
- lifecycle stage;
- liquidity bucket;
- market-cap bucket;
- launch mechanism;
- hazard class;
- market regime.

If the exact comparable sample is below the prospective minimum, cohort scoring fails closed with `insufficient_comparable_cohort`.

The anomaly percentile threshold is an explicit experiment input rather than a silently promoted production constant.

## Authority boundary

Batch 5 preserves:

- v5.1 as authoritative;
- v5.2 as research/test-only;
- no production composition hook;
- paper-only operation;
- no signing;
- no transaction submission;
- no live-money authority;
- the 20-second operational ceiling;
- >40% chase as observe-only for the current impulse;
- exact amount-specific two-sided quotes;
- structural exit hard stops;
- no averaging down.

No Batch 5 feature changes an economic threshold or receives direct trade authority.

## Completion criteria

Batch 5 is technically complete only after:

1. focused Batch 5 regressions pass;
2. the full required CI gate passes on the exact PR head;
3. the PR is merged to `main`;
4. the full required CI gate passes again on the exact merge SHA.

Technical completion is not economic promotion of v5.2. Prospective forward evidence is still required before any future challenger promotion decision.
