# Isolated v5.2 extraction — first restart-safe paper lifecycle

**Scope: offline synthetic acceptance only. No deployment, migration, live provider,
live/evolved policy import, production certification, or profitability claim.**

Base: `b8d7ee2e672b52ffe314f8b285d00892cf171387`.
Base source tree: `424956b0f420f015cb685c4085cee4a4f19e4a10`.
Branch: `extraction/v52-restart-safe-lifecycle`.

## Run the complete milestone

Use Python 3.11.16 (repository CI pin). Choose fresh output/data directories:

```sh
python extraction/v52/build.py --output /tmp/v52-detached
PYTHONPATH=/tmp/v52-detached python -B -S -m roi_extracted.demo --directory /tmp/v52-acceptance
python -m pytest extraction/v52/test_extraction.py -v
```

The built package needs only the Python standard library. `-S` excludes unrelated
site initialization; every demo subprocess uses that same clean boundary. The test-only differential
oracle imports the original repository functions in a separate process; install
`requirements.lock` for that comparison. The normal detached runtime never imports
`solana_roi`, FastAPI, provider clients, registry/bootstrap/repair installers,
legacy storage, checkpoints or predecessor verification. The output can be copied
away from this repository. A manifest pins source and generated file hashes;
changed policy, source or artifact bytes fail closed. No existing file is overwritten.

## Preserved behavior and exact extraction boundary

`build.py` parses pinned source, follows definition/constant dependencies and copies
verbatim economic definitions; it does **not** retype their coefficients or simplify
their decisions. Whole policy modules and both JSON policy files are byte-identical.

The explicit selection graph is:
`_v52_solana_choose` -> adaptive `_solana_choose` -> completion
`_completed_solana_choose` -> finalization `_final_solana_choose`.
It retains mechanical risk, context/entity confirmations, wallet validation and
confidence, minimum-economic-position/portfolio competition, chase, latency,
signal-age and exit-depth constraints. The completion layer's cold-start $2.50
minimum means the fixture's actual starter is **0.5%**, not a substituted 0.25%.

Original `WalletAlphaRefinementLedger`, EntityGraph, market-regime/confirmation
methods, `_exit_features` and ExitAlphaModel run against an explicit in-memory
point-in-time evidence port. No economic result is mocked in the acceptance runtime.
Facts unavailable at the decision timestamp are excluded, not learned retroactively.
Clock ports use the supplied decision time. Real wallet-scoring tests prove both
eligible paired-evidence influence and future-evidence exclusion; default lifecycle
fixtures do not claim that unvalidated wallet alpha is already predictive.

The single-position exit plan is extracted by a documented AST transformation of
`_completed_solana_sell`: retain statements from feature evaluation through amount
and stage selection, omit only the old exit-signal logging call, convert loop
`continue` to `return None`. Quote acquisition, persistence and accounting belong to
the new owner. Differential tests intercept the original method only at its I/O
ports and compare selected exit quantity/stage/reason. Hash/AST tests verify source
identity for the verbatim definitions and methods. The retained policy can emit
staged exits, but the first lifecycle acceptance proves hold followed by full
leader-triggered liquidation; broad runner/staged-exit coverage is not claimed.

## New runtime, not a disguised import of the old application

`kernel.py`: explicit fixed economic delegate graph and ephemeral evidence view.
`runtime.py`: the only durable writer; single SQLite transaction owns decision,
reservation, position, cash, journal and settlement. Fixed-point USD nanodollars
and integer native/token amounts avoid monetary drift. Net quote proceeds include
fees and immediate bid marks. Cash + remaining cost basis = $500 + realized P&L;
NAV additionally reflects executable marks, or is unknown when a mark is missing.
`demo.py`: clearly labelled synthetic observation/quote input and separate-process
lifecycle driver. No credentials, sockets, signing or transaction submission exist.

A named new experiment must be initialized explicitly outside `/var/data`. An
existing production/unknown database is rejected read-only before any writer opens.
The experiment is not a continuation/replacement of the old paper history. Runtime
metadata binds it to the exact artifact; archive/hash changes require a new explicit
experiment or future reviewed migration, not silent adoption.

Journal insert, state mutation and settlement commit atomically using FULL
synchronous WAL. Duplicate events return the exact original serialized receipt;
conflicting same-ID evidence fails. Replay never spends or settles twice. Startup
streams and verifies this small journal and reconciles its head with current state;
there is no old full-ledger/checkpoint/bootstrap scan. Before/after-COMMIT SIGKILL
cases exercise rollback and committed-but-unacknowledged recovery for entry AND exit.
Two competing processes cannot reserve capital twice. Tampering, out-of-order
inputs, artifact drift and evidence admission overflows fail explicitly.

This first acceptance harness has explicit bounds: 128 KiB per event, 256 supplied
rows per evidence category, 512 decisions and a 32 MiB logical ledger. It stops
rather than silently pruning required evidence. This is NOT a proposed indefinite
production retention system or proof that the original memory issue is repaired.
A separate-process full lifecycle must fit a 256 MiB address-space envelope.

## First acceptance result and limitations

The fixture has one Pump.fun buy, an unrelated-wallet sell that causes a HOLD,
a leader sell that selects a full exit, and a structurally invalid candidate that
is rejected. Eleven fresh interpreter boundaries restore and check durable state.
The order is $2.50 plus $0.0005 fee; final net proceeds are $2.625; final cash is
$500.1245, zero open position/basis, one settlement. **Those numbers are constructed
fixture accounting, not measured market returns.**

This milestone supports cold-start Pump.fun starter decisions and exits using the
checked-in policy plus the effective refinement chain. Other venues are explicit
scope rejections, not claimed original-strategy rejections. Scale-ins, re-entry,
post-settlement adaptive learning, live feeds, production-state import and policy
promotion are also not implemented. These are disclosed operational scope limits;
no strategy threshold is lowered and no five-lane/full-market certification is
claimed. Original five-lane configuration remains preserved in the artifact.

The first milestone does not assume a clean database proves all production state
was disposable. Old data, Render services, maintenance commands and all recovery,
reclamation and Robinhood V2/V4 controls remain outside this branch's authority.

## Next acceptance boundary

Before prospective testing: extend supported cases one at a time with source parity,
then add the live provider observation/quote adapter, production-effective policy
and trustworthy state import (or a separately approved new epoch), all intended
surface/lifecycle coverage, sustained workload/resource evidence and a governed
certification/read-model path. Do not re-import the old production entrypoint to
make missing behavior appear available. Do not merge/deploy this branch as a
replacement runtime merely because the synthetic milestone passes.
