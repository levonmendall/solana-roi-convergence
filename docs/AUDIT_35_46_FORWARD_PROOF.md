# Audit 35–46 — Live Forward Proof

This closure adds one fail-closed, read-only certification surface for the production evidence phase after the 23–34 evidence-validity work. It does **not** create a second strategy authority and does not change any frozen v5.1 economic rule.

The active authority remains `roi-convergence-v5.1-consolidated-proof-1`, strategy `roi-convergence-v5.1-context-exactness-1`, economic epoch `v51-consolidated-proof-20260905`. Paper-only authority, no signing, no transaction submission, the 20-second operational ceiling, chase rules, family caps, promotion thresholds, hazard burdens, kill rules and exits are unchanged.

## 35–46 machine gates

35. **Exact live release** — binds the certificate to the production release SHA and fails closed on mismatch.
36. **Paper-only safety boundary** — requires paper-only=true, live-money=false, signing=false and transaction-submission=false.
37. **Solana transport** — consumes the existing unified Solana readiness/blocker model rather than inventing a second liveness definition.
38. **FOMO transport** — consumes the existing FOMO readiness/blocker model; collector errors remain visible and isolated from economic authority.
39. **Robinhood transport** — consumes the existing isolated-worker readiness/fail-closed model.
40. **Real forward candidate flow** — requires the existing forward proof SLO to be confirmed and reports recent stage-event flow. No recent candidate is evidence debt, not a fabricated transport failure.
41. **Current-release attestation** — the current release must earn its live surface attestation; an API read cannot manufacture it.
42. **Validation/holdout family economics** — consumes only existing frozen-v5.1 promotion claims. Discovery-only evidence remains non-authoritative.
43. **Rejected-opportunity counterfactuals** — exposes resolved, pending and positive rejected outcomes; retrospective entry authority remains false.
44. **Hazard calibration** — reports clean-to-extreme observations for future-epoch diagnosis only. Current hazard multipliers/evidence burdens are unchanged.
45. **Cross-family correlation and one-capital-base NAV** — correlation maturity is visible for future mature scaling, while all overlapping positions must reconcile against one shared paper capital base. Correlation maturity does not lift the active frozen family cap.
46. **Final forward certificate** — combines the hard operational gates with actual forward evidence maturity and the existing per-family promotion claims. It has no allocation, strategy-mutation or live-money authority.

## State semantics

`certified` means the exact deployed release, safety boundary, all three transports, measurement SLO, one-capital-base reconciliation, recent forward flow, live release attestation, at least one existing valid family promotion claim and rejected-counterfactual coverage are all simultaneously proven.

`collecting_forward_evidence` is a normal fail-closed state when the system is operational but real forward evidence is not yet mature. It is not equivalent to a transport failure and it is not evidence of profitability.

`transport_degraded`, `measurement_degraded`, `release_mismatch` and `safety_boundary_failed` identify operational/proof failures rather than insufficient sample size.

## Current-cap versus future mature scaling

The active immature-family cap remains the frozen v5.1 cap. The existing maturity-allocation diagnostic may report a family as eligible for the permanent ceiling only when its current promotion claim, mature correlation evidence and material execution stress all support that future change. This audit does not activate the higher ceiling.

## Production verification

`Production Forward Proof` is an explicit **post-deploy** verifier. It is intentionally not triggered by a push to `main`, because Render uses `autoDeployTrigger=checksPass`; making a workflow wait for Render while Render waits for that same workflow would create a deployment deadlock.

After the exact release is live, the verifier checks:

- `/health` remains paper-only;
- `/v1/strategy/e2e-status` is bound to the exact release and exposes no signing/submission authority;
- `/v1/strategy/forward-certification` is bound to the same release;
- Solana, FOMO and Robinhood transports are ready;
- the one-capital-base reconciliation invariant is intact;
- the certificate explicitly reports no strategy-authority or economic-threshold mutation.

The verifier intentionally does **not** require `system_forward_certified=true`, because a freshly deployed, healthy system can legitimately still be collecting real validation/holdout outcomes. That state must remain observable rather than being converted into synthetic evidence.
