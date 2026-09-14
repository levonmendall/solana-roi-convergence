# Validation Cloud Robinhood `eth_getLogs` integration

The Robinhood lane can use Validation Cloud as a dedicated HTTP provider for `eth_getLogs` without changing the primary Robinhood RPC/WebSocket provider pair.

## Render configuration

Required secret:

- `ROBINHOOD_VALIDATION_CLOUD_RPC_URL` — the full HTTPS Robinhood Chain mainnet Node API endpoint copied from Validation Cloud. Keep the credential only in Render; do not commit it.

Optional:

- `ROBINHOOD_VALIDATION_CLOUD_MAX_BLOCKS` — positive inclusive block-count cap for each Validation Cloud `eth_getLogs` request. Leave unset initially so the capability test can determine the provider's practical limit.

The existing `ROBINHOOD_ETH_GET_LOGS_MAX_BLOCKS` remains a global override and takes precedence if it is set.

## Safety and authority

When the Validation Cloud endpoint is configured, only `eth_getLogs` is routed to it. Before accepting logs, the runtime verifies Robinhood Chain ID `4663` and a readable current block head. The primary Chainstack/current provider URL and WebSocket generation are not moved by a successful Validation Cloud log request.

If Validation Cloud fails, the exact same unadvanced block range falls back to the existing governed provider path. Existing contiguous-frontier and fail-closed behavior remains intact. No strategy thresholds, sizing, wallet roles, signing, submission, custody, or live-money authority are changed.
