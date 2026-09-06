# Main merge topology

The required CI workflow enforces a non-forced, two-parent merge topology on pushes to `main`.

Operational rule: pull requests that are intended to advance canonical `main` must be merged with a true merge commit. Do not squash-merge or rebase-merge them into `main`, because those forms create a one-parent head commit and intentionally fail the `Main history policy backstop` check.

This policy is repository-governance only. It does not alter strategy authority, economic thresholds, execution behavior, paper-only status, signing capability, transaction submission capability, or live-money authority.
