# Cross-release learning compatibility

Production strategy evidence uses an explicit economic compatibility epoch rather than treating a deployment SHA as a statistical reset.

`release_commit` remains permanent audit lineage. Solana/Pump/Raydium and FOMO evidence can pool across releases only after each release is registered into the same compatibility epoch. Releases predating the epoch are preserved but have no promotion authority in the new pooled statistics.

The current epoch is `continuation-v1-cross-release-20260905`. It begins after the continuation-first policy is the final production authority, preventing older v5.1 releases with different timing/chase economics from being mixed solely because they share the same v5.1 version string.

Repeated economic signals are deduplicated across compatible releases. Tracked-wallet FOMO and independent-market-flow FOMO remain separate evidence lanes even though they share the same underlying venue correlation family.

Authority remains paper-only. This repair adds no signer, private key surface, transaction submission, or live-money capability.
