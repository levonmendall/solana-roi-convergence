# Cross-release learning compatibility

Deployment SHA remains permanent audit lineage, but a deployment is not automatically a new statistical strategy. Releases may pool only when they are explicitly registered to the same **economic freeze epoch** and therefore implement the same selection, sizing, promotion, kill and exit-learning economics.

## Canonical epoch

The current production economic epoch is:

`v51-consolidated-proof-20260905`

Its authority id is:

`roi-convergence-v5.1-consolidated-proof-1`

The predecessor compatibility epoch `continuation-v1-cross-release-20260905` remains durable audit history. It does **not** have promotion authority for the consolidated epoch because the consolidation changes latency-decay, hazard evidence burden, hierarchical pooling and kill economics.

Each compatible release is registered in `v51_economic_freeze_releases` with its authority fingerprint. Solana, FOMO and Robinhood promotion/exit-learning queries join through that registration. A normal reliability deployment can therefore keep learning without a statistical reset, while an economic change must start a new epoch rather than contaminating forward evidence.

Repeated source signatures/trial ids are deduplicated where evidence is pooled. Cross-entity evidence cannot grant a specific entity promotion authority. Tracked-wallet FOMO and independent-market-flow FOMO remain separate economic contexts.

Authority remains paper-only. No compatibility mechanism may add signing, transaction submission, private-key or live-money capability.
