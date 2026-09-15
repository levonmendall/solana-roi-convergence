# PR #260 carry-forward audit

This branch carries forward only the two still-missing SQLite write-amplification protections from PR #260.

Superseded portions are deliberately excluded:

- RPC task-root ownership is already provided by the stronger merged `rpc-endpoint-task-terminal-ownership-v3-captured-capacity-reference` repair.
- Forward-certification HTTP reads are already served from the dedicated certification-generation snapshot/cache boundary with single-flight coordination and post-build resource guarding.

The retained change prevents conflict UPSERTs in the execution-cost ledger and rejected-counterfactual ledger from issuing SQLite UPDATEs when all canonical facts are unchanged. Changed facts continue to persist normally.

No strategy, certification, continuity, evidence-retention, signing, submission, custody, or live-money authority behavior is changed. Paper-only operation is preserved.
