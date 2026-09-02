# Live-shadow deployment handoff

The repository is designed to begin **paper-only mainnet observation** as soon as the Render service is provisioned and the required external values are supplied.

## What is automated

- `render.yaml` creates one Python web service with a persistent `/var/data` disk, one instance, the continuous price clock enabled, and auto-deploy disabled.
- The v3.1 scout cohort is seeded with the exact public addresses for Jijo, Wugi, and The Doc. All three are frozen S-tier profiles with at least 30 historical first touches.
- On startup, the service reads Render's `RENDER_EXTERNAL_URL` and idempotently creates or updates the Helius enhanced webhook to `/v1/ingestion/helius`.
- The Helius subscription watches the frozen Pump bonding-curve, Pump AMM, and Raydium program IDs with `transactionTypes=["ANY"]`; the application parser still fails closed and admits only supported normalized trades.
- Authenticated webhook deliveries are durably queued in SQLite before HTTP acknowledgement.
- The exact deployed commit is supplied by Render through `RENDER_GIT_COMMIT` and is used by the eventual immutable experiment manifest.
- `/v1/deployment/preflight` reports only booleans/public configuration. It never returns API keys, webhook auth secrets, cohort admin secrets, private keys, seed phrases, or mnemonics.

## Values that must come from outside the repository

The Blueprint intentionally declares these as `sync: false` so their values are never committed:

- `HELIUS_API_KEY`
- `HELIUS_WEBHOOK_AUTH`
- `JUPITER_API_KEY`
- `SOLANA_ROI_COHORT_ARM_AUTH`
- `SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY`

`SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY` must be only a normal Solana public address. Do not provide a private key, seed phrase, mnemonic, keystore, or signing service to this application.

## Verification after deployment

1. Open `/v1/deployment/preflight`. `ready_for_live_shadow_collection` must be `true` before treating observations as deployment-certified.
2. Open `/v1/ingestion/status`. Confirm paper NAV/cash are still `$500`, `event_chain_valid=true`, the webhook queue is healthy, and `paper_signal_promotion_enabled=false`.
3. Confirm a `helius_webhook_bootstrap` event appears after startup or run `solana-roi sync-helius-webhook --service-url <https-url>` from the configured service shell. The command is idempotent and does not print credentials.
4. Leave the forward cohort **unarmed** while the system accumulates prospective coverage, latency, quote, and unsigned transaction-simulation observations.
5. Freeze/arm only after the existing forward-cohort gate reports every required certification as passed.

## Local/remote preflight command

```bash
solana-roi preflight
```

Exit code `0` means the environment has the required paper-only deployment inputs. Exit code `1` means at least one launch requirement is missing or unsafe.

## Safety boundary

The application has no accepted private-key environment variable, no signer, and no transaction-submission path. Deployment preflight explicitly fails if ROI-specific private-key/seed environment names are populated. The canonical bankroll remains a virtual `$500.00` paper ledger.
