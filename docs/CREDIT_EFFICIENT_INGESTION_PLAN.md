# Credit-efficient live ingestion

The ROI Convergence v3.1 forward cohort must not subscribe to `ANY` traffic across all frozen program IDs continuously. That shape can consume the Helius free-plan allowance before prospective certification begins.

Production collection is split into two lanes:

1. **Permanent scout-trigger lane** — one Enhanced webhook targets only the frozen S/A scout wallet public addresses. This is the low-volume path that can create candidate first touches, run fresh 6D risk, request the amount-specific Jupiter quote, and perform unsigned shadow simulation.
2. **Bounded coverage-certification lane** — program-level evidence is sampled only until the unchanged prospective certification requirements are satisfied. It is not permitted to authorize paper trades and must stop automatically once the coverage gate is certified.

The immutable strategy, scout cohort, risk thresholds, latency thresholds, quote thresholds, $500 genesis, paper-only boundary, and no-signing/no-submission rules are unchanged.
