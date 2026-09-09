# v5.2 Final Refinement Freeze

Status: **final research/test-only refinement for prospective testing**  
Batch: `v52-batch7-final-refinement-1`  
Freeze: `v52-weekend-review-20260908-final-refinement-freeze-2`

## Boundary

This batch completes the final refinement pass requested after the September 5–7 weekend review. It is intentionally a refinement rather than a redesign. The existing v5.2 candidate-continuity, early-detection, graduation/execution, upside-capture, detection-intelligence, and measurement-governance layers remain intact.

The v5.1 incumbent remains authoritative. Batch 7 is explicitly classified `test_only` in `module_reachability_policy.json`, has no production composition hook, and cannot authorize entries, signing, submission, or live-money execution.

## Final additions

1. **State hysteresis / anti-flapping**
   - Improving transitions require corroboration.
   - Hard structural deterioration can demote immediately.
   - The same marginal signal fingerprint cannot repeatedly reopen entry consideration.

2. **Signal expiration and continuation epochs**
   - Candidate lifecycle continuity remains durable.
   - Economic evidence is epoch-scoped and decays with age/freshness.
   - Expired epochs require a new evidence epoch rather than reusing stale evidence.

3. **Velocity-sensitive quote freshness**
   - The absolute latency ceiling remains 20 seconds.
   - The dynamic execution-evidence limit is always tighter than that ceiling and contracts as price velocity rises.
   - `signal -> quote`, `quote -> decision`, and `decision -> simulated fill` are measured explicitly.

4. **Exit-capacity stress testing**
   - Exact sell depth is stressed under degraded-liquidity scenarios before entry or any pyramid.
   - Requested notional must fit the stressed exit-capacity limit.

5. **Flow-to-price response**
   - Measures price response per independent net-buy dollar and per new independent buyer.
   - Compares current marginal response with prior response to identify seller absorption/exhaustion.

6. **Portfolio-level opportunity competition**
   - Candidates compete on expected residual return, confidence, execution quality, and tail-risk/cost.
   - Correlated creator/funder/wallet/cohort/venue exposure is penalized during ranking.
   - The existing 25% immature-family portfolio cap remains enforced.

7. **Confidence, freshness, and provenance**
   - Derived signals carry `value + confidence + freshness + provenance`.
   - Missing provenance fails closed instead of being treated as high-confidence evidence.

8. **Time-to-alpha-loss curve**
   - Measures executable residual upside at 1, 2, 5, 10, 20, 30, and 60 seconds.
   - Reports alpha lost versus the 1-second observation so engineering work can target economic latency rather than technical latency alone.

## Locked constraints preserved

This refinement does **not**:

- raise the `>40%` chase observe-only boundary;
- loosen exact two-sided quote or exact sell-route requirements;
- permit first-slot Pump.fun sniping;
- increase allocation merely because a token is accelerating;
- lower evidence standards to force trades;
- give the opportunity-emergence model trading authority;
- permit averaging down;
- weaken structural exit hard stops;
- enable signing, transaction submission, or live-money authority.

The absolute immediate-copy hard ceiling remains **20 seconds**. Dynamic quote freshness is an additional tighter requirement, not a relaxation of that ceiling.

## Prospective-testing rule

With Batch 7 merged, the v5.2 challenger is frozen for prospective testing. September 5–7 evidence may remain part of the documented design rationale, but it cannot directly retune the frozen challenger. Any subsequent strategy modification must be justified by new forward evidence and pass the existing governance and safety gates.

## Certification expectations

The final freeze must continue to prove:

- deterministic freeze fingerprint;
- v5.1 authority unchanged;
- challenger entry authority false;
- research/test-only reachability;
- paper-only operation;
- signing false;
- transaction submission false;
- live-money authority false;
- exact two-sided execution evidence preserved;
- structural exit hard stops preserved;
- 20-second hard latency ceiling preserved;
- >40% chase observe-only boundary preserved;
- no averaging down;
- no historical/weekend direct promotion.
