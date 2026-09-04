# Robinhood Chain Paper Strategy

Robinhood Chain is a first-class **paper-trading** chain in ROI Convergence. It is not gated behind a separate shadow-only promotion stage. Real forward Robinhood Chain observations can create bounded paper positions immediately; those paper outcomes are the prospective evidence used to promote, demote, and size exact contexts.

## Authority boundary

- `paper_trading_authority=true`
- `shadow_only=false`
- `live_money_authority=false`
- signing is unavailable
- transaction submission is unavailable
- no private key, seed phrase, mnemonic, or signer is accepted

The Robinhood paper sleeve starts at $500 for an apples-to-apples chain benchmark. This is simulated capital, not additional live capital.

## Initial venue/lifecycle scope

The first active contexts are deliberately crypto-only:

1. **Direct Uniswap v3 WETH pools** — chain-wide WETH pool discovery, exact QuoterV2 buy and immediate-sell quotes, `new_weth_pool` lifecycle.
2. **Pons v1 -> Uniswap v3** — if the factory emits again, its launch-protection block boundary is respected before paper entry; lifecycle changes to `post_protection_v3` only after restrictions expire.
3. **Pons v2 bonding curve** — whitelisted launches are observed; paper entry uses the curve's onchain reserves, fee, creator tax, current snipe tax and sellable balance. Entry is rejected above 500 bps snipe tax or after 85% graduation progress.
4. **Pons v2 -> Uniswap v4** — v4 is initially an exit route for a curve position after graduation, not a new-entry source.

Robinhood Stock Tokens and tokenized ETFs are excluded from this strategy using Robinhood's official read-only Stock Token deployment registry. If that registry is unavailable and the RWA filter is required, direct-v3 paper entry fails closed.

## Economic-entity independence

Robinhood supports ERC-4337, so `tx.from` can be a bundler and is never used as the trading identity. Uniswap v3 uses the Swap event recipient; Pons v2 uses the indexed curve buyer/seller.

Before a demand state can authorize a paper entry, recent buyer addresses are collapsed using cached Blockscout native-funding anchors. If entity resolution is required and unavailable, raw addresses are **not** counted as independent confirmations and the paper entry fails closed.

## Paper entry and sizing

The paper strategy identifies `pre_fomo` and `active_fomo` from real forward flow using buyer breadth, buy/sell flow, acceleration, and price chase. It then requires an amount-specific executable buy quote and an immediate executable sell quote.

Hard initial bounds:

- bootstrap position: 1% of Robinhood paper NAV
- maximum promoted position: 5%
- maximum total open Robinhood exposure: 20%
- maximum chase: 15%
- maximum immediate round-trip cost: 15%
- stop loss: -12%
- harvest: +30%
- maximum hold: 20 minutes
- one open paper position per token

The first 30 settled outcomes in an exact economic-entity × venue × lifecycle context are still **paper trades**. They are not shadow observations. Before maturity the context receives bounded bootstrap positions. After 30 outcomes, positive median ROI, positive mean ROI after removing the single best trade, and at least a 50% positive rate are required for promotion. A failing mature context is demoted from new paper entries.

Promoted contexts choose among 0.5%, 1%, 2%, and 5% sizing by expected log growth. Historical data can prioritize observation but cannot promote a context.

## Exit realism

A position is not allowed to disappear from evidence merely because an exit quote becomes unavailable. Before the maximum hold the strategy can wait for an executable route. At the forced deadline, an unexitable position is settled as a full paper loss. This makes thin-liquidity and graduation-transition failures visible in ROI rather than creating survivorship bias.

## Isolation

Robinhood authority is keyed by:

`chain × economic entity × venue × lifecycle × role`

Success cannot transfer from Solana to Robinhood, between Robinhood venues, or between Robinhood lifecycles. Robinhood uses its own tracking, profitability, paper NAV, and paper outcomes, while sharing the existing durable SQLite evidence store and production process.

## Data plane

- chain ID: 4663
- ETH gas
- default RPC fallback: Robinhood public mainnet RPC (rate-limited)
- production override: `ROBINHOOD_RPC_URL` (Alchemy is the preferred provider)
- Uniswap v3 factory and QuoterV2: chain-specific deployed contracts
- Blockscout: read-only entity/funding-anchor enrichment
- Robinhood Stock Token `/rhj/assets`: read-only RWA exclusion registry

A Robinhood task failure is isolated from the existing Solana runtime. The public endpoint `/v1/robinhood-chain/status` exposes chain continuity, paper NAV, open exposure, profitability by venue/lifecycle, entity-context performance, RWA-filter state, and errors.
