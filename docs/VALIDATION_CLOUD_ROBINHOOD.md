# Validation Cloud Robinhood `eth_getLogs` integration

The Robinhood lane can use Validation Cloud as a dedicated HTTP provider for `eth_getLogs` without changing the primary Robinhood RPC/WebSocket provider pair.

## Render configuration

Required secret:

- `ROBINHOOD_VALIDATION_CLOUD_RPC_URL` — the full HTTPS Robinhood Chain mainnet Node API endpoint copied from Validation Cloud. Keep the credential only in Render; do not commit it.

Optional:

- `ROBINHOOD_VALIDATION_CLOUD_MAX_BLOCKS` — positive inclusive block-count cap for each Validation Cloud `eth_getLogs` request. Leave unset initially so production evidence can determine the provider's practical limit.
- `ROBINHOOD_VALIDATION_CLOUD_TIMEOUT_SECONDS` — per-request timeout for Validation Cloud. Defaults to 12 seconds so archive/log requests do not inherit the base Robinhood RPC client's 4-second timeout.
- `ROBINHOOD_VALIDATION_CLOUD_RETRIES` — bounded same-RPC retry count for retryable transport/provider failures. Defaults to 1 and is capped at 3.

The existing `ROBINHOOD_ETH_GET_LOGS_MAX_BLOCKS` remains a global override and takes precedence if it is set.

## Safety and authority

When the Validation Cloud endpoint is configured, only `eth_getLogs` is routed to it. Before accepting logs, the runtime verifies Robinhood Chain ID `4663` and a readable current block head. The primary Chainstack/current provider URL and WebSocket generation are not moved by a successful Validation Cloud log request.

If Validation Cloud fails, the exact same unadvanced block range enters the existing governed fallback path exactly once. The fallback provider's own range constraint is reapplied after provider switching, including Alchemy's 10-inclusive-block limit. Existing contiguous-frontier and fail-closed behavior remains intact.

Validation Cloud failures emit sanitized telemetry with the exact block range/span, error class, HTTP status or JSON-RPC code, retry/split counters, and fallback count. The endpoint URL, credential-bearing path, and raw exception text are never logged.

No strategy thresholds, sizing, wallet roles, signing, submission, custody, or live-money authority are changed.
