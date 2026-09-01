# Frozen Forward Baseline — ROI Convergence v3.1

This file defines the first prospective strategy cohort. Parameters must not be changed retroactively after forward evidence begins.

## Eligibility

- Scout must be historically eligible and S-tier or top A-tier using only information available before the signal.
- Required risk evidence must be fresh and clean.
- Any hard risk flag fails closed.
- Confirmation must come from a different economic entity, not merely a different address.

## S-tier entry

1. Clean first touch -> paper starter equal to 30% of the full position.
2. Independent eligible confirmation must arrive within 20 seconds.
3. Price may not be more than 15% above the scout reference price.
4. On confirmation, add the remaining 70%.
5. Without confirmation, exit the starter after the confirmation window.

## A-tier entry

1. No starter.
2. Wait for independent eligible confirmation within 20 seconds.
3. Price may not be more than 15% above scout reference.
4. Enter the full position only after confirmation.

## Sizing

- Genesis paper NAV: $500.
- Risk budget: 0.75% of current NAV.
- Catastrophic stop: -30%.
- Full spot notional: risk / stop = 2.5% of current NAV.
- S-tier starter: 30% of that full notional.

## Exit rules

- Catastrophic stop: -30% from confirmation reference.
- Stagnation: exit if confirmed trade is non-positive after 90 seconds.
- Thesis timeout: exit if +50% has not been reached by 180 seconds.
- At +50%: sell 70% of current units.
- Keep 30% as the runner.
- Runner high-water trails by 40%; close the runner on a 40% drawdown from its post-harvest high.

## Execution stress

The baseline simulator applies 2.5% adverse execution drag to each side of traded capital, approximating about 5% round-trip friction before any future provider-specific calibration.

## Certification

No profitability claim until at least 300 independent closed token episodes satisfy all configured gates, including positive aggregate P&L, positive geometric growth, profit factor > 1, 95% Wilson lower hit-rate bound above 57.49%, positive P&L excluding the best trade, and positive P&L excluding the best scout.
