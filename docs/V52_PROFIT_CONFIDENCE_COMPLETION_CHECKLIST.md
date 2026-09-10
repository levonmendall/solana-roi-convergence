# v5.2 Completion Verification Checklist

The merge gate for the profit/confidence completion requires all of the following on the exact branch SHA:

- Full repository CI passes.
- New v5.2 profit/confidence regression tests pass.
- Final Solana/FOMO/Robinhood decision wrappers retain v5.2 authority markers.
- Numeric lane caps are enforced after every adaptive sizing layer.
- Signal age is bounded by the configured 120-second absolute ceiling.
- Hard observation/execution latency remains at or below 20 seconds.
- Absolute chase remains at or below 80%; exceptional continuation cannot bypass it.
- Exact amount-specific two-sided quotes remain required.
- Exact exit-depth coverage remains at least 2x.
- Maximum sizing quote rounds remain no greater than two.
- No averaging down and no first-slot Pump.fun sniping.
- Counterfactual learning is analytical only and cannot grant entry authority.
- 24h and 7d performance endpoints are GET/read-only.
- Paper-only=true; live-money authority=false; signing=false; transaction submission=false.
- Post-merge GitHub `main` SHA equals both Render release SHAs before the release is called verified.
