# Dependency update policy

This repository uses `requirements.lock` as the deterministic Python 3.11 production dependency set. Dependency updates are architecture/measurement maintenance, not strategy changes.

## Required process

Every dependency update must be proposed by pull request. Automated updates may open pull requests, but they never merge or deploy themselves.

A dependency PR must:

1. update the direct pin in `pyproject.toml` when the direct dependency changes;
2. regenerate `requirements.lock` deliberately from the intended Python 3.11 environment;
3. update `dependency_compatibility.json` so its `requirements_lock_sha256` exactly matches the new lock file;
4. record whether execution compatibility and measurement compatibility are unchanged, require a new epoch, or are not applicable;
5. keep `strategy_economic_authority_changed=false` unless the change is intentionally governed as a new economic strategy epoch;
6. pass the complete `required-ci` gate, including final-production composition, black-box E2E, migration, paper-only/no-live-money, and forward-certification regressions;
7. merge only through a PR using the exact tested head SHA;
8. verify post-merge `main` CI and the exact Render deployment.

## Compatibility classification

A dependency change requires an **execution compatibility review** when it can affect HTTP/WebSocket behavior, quote/simulation semantics, timing, serialization, numerical behavior, transaction construction, or provider interaction.

A dependency change requires a **measurement compatibility review** when it can affect candidate identity, event ordering, timestamps, persistence/serialization, evidence attribution, statistics, or proof generation.

If either review concludes that evidence is not statistically or semantically exchangeable with the current measurement/execution epoch, the dependency update must create the appropriate new compatibility epoch. Existing historical evidence remains auditable but must not silently gain current promotion authority.

## Automation

GitHub Dependabot checks the Python dependency set weekly and opens pull requests only. The repository CI enforces that the lock hash and compatibility manifest remain synchronized. No automated dependency update has authority to bypass tests, change v5.1 economics, sign, submit transactions, or create live-money authority.
