# Validation Cloud Robinhood production proof

Validation Cloud remains a dedicated Robinhood Chain `eth_getLogs` provider. It does not replace the primary current-read/WebSocket provider and has no strategy, sizing, wallet, signing, submission, custody, or live-money authority.

## Runtime behavior

The production proof repair adds:

- a dedicated 12-second Validation Cloud request timeout by default, independent of the base Robinhood RPC client's 4-second timeout;
- one bounded same-RPC retry by default for timeouts, transport failures, HTTP 408/425/429/5xx, and selected provider-capacity JSON-RPC errors;
- bounded exact-range subdivision for timeout/5xx/provider-capacity failures before falling back;
- explicit prevention of Validation Cloud re-entry after a range has already failed;
- reapplication of the active fallback provider's own range limit, including Alchemy's 10-inclusive-block constraint;
- sanitized production telemetry that never logs the Validation Cloud URL or raw exception text;
- cumulative proof counters for attempted/successful/failed Validation Cloud ranges, retries, subdivisions, fallbacks, and the last sanitized failure classification.

Optional environment overrides:

- `ROBINHOOD_VALIDATION_CLOUD_TIMEOUT_SECONDS`
- `ROBINHOOD_VALIDATION_CLOUD_RETRIES`
- `ROBINHOOD_VALIDATION_CLOUD_MAX_BLOCKS`

No override is required for the initial production proof.

## Pass criteria

A Validation Cloud provider proof requires sustained broad-research passes while the known Chainstack archive 403 and exhausted Alchemy fallback cannot independently satisfy the workload. During the observation window:

1. Validation Cloud must verify Robinhood Chain ID `4663` and a readable current block head.
2. `ROBINHOOD_VALIDATION_CLOUD_GETLOGS_SUCCESS` must continue advancing over real production ranges.
3. The broad-research successful-pass counter must advance repeatedly and freshness must remain valid.
4. There must be no unexplained block-range gaps or cursor advancement after a failed middle range.
5. `RobinhoodProviderPoolUnavailable` must not recur as the terminal result of a Validation Cloud-eligible log request.
6. Any Validation Cloud failure must be attributable through sanitized error type, HTTP status or JSON-RPC code, exact requested range/span, retry/split behavior, and fallback count.
7. Primary current RPC/WebSocket health must remain intact.
8. Paper-only and strategy authority boundaries must remain unchanged.

Whole-service certification additionally requires the independent storage/memory stability gate; provider success alone does not waive that gate.
