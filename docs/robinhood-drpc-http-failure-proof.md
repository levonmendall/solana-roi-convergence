# Robinhood dRPC HTTP failure proof

This diagnostic exists only to identify the production cause of a dRPC capability-probe HTTP failure.

It emits only:

- the allow-listed probe method (`eth_chainId` or `eth_blockNumber`, otherwise `other`);
- the exception class;
- a numeric HTTP status code when one is safely available.

It never emits the provider URL, API key, request headers, response headers, response body, exception message, wallet data, or credentials.

The diagnostic does not change strategy economics, paper-entry authority, failover thresholds, provider cooldowns, signing, submission, custody, or live-money authority. The existing `eth_chainId` + `eth_blockNumber` dRPC health gate remains unchanged and fail-closed.
