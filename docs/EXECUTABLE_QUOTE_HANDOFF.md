# Amount-Specific Executable Quote Handoff

This stage replaces optimistic generic entry marks with shadow, amount-specific route quotes sized from the actual $500 paper bankroll.

## Jupiter quote-only transport

The system uses Jupiter Swap V2 `GET /order` without a `taker`. Jupiter documents that omitting the taker returns a quote without a transaction, which is exactly the paper-only boundary required here. The service never calls `/execute`, never constructs or signs a transaction, and never loads a private key.

Each quote records the winning router, Jupiter fee basis points, quote latency, chain-to-quote latency, effective SOL/token output price, and the drift from the original scout transaction price.

## Position-aware sizing

Quotes use the frozen strategy's current compounding position size:

- S-tier first touch: 30% starter quote;
- S-tier independent confirmation: remaining 70% quote;
- A-tier independent confirmation: 100% full-position quote.

The original scout price is retained only as the information/reference price for the 15% chase ceiling. It is never treated as the post-risk fill price.

SOL/USD is derived from a short-lived Jupiter SOL→USDC quote, and token decimals are read from Solana RPC before converting raw output into token units.

## Quote certification

The initial pre-cohort quote gate requires:

- at least 100 amount-specific quotes;
- at least 95% usable under the frozen chase ceiling;
- p95 quote-service latency <=2 seconds;
- p95 chain-observation-to-quote latency <=5 seconds.

This is a measurement gate only. Passing it never activates the portfolio automatically.

## Remaining activation work

The next activation layer must join risk latency and quote evidence into a single candidate-level gate, ensure quote timestamps are after risk completion, drive continuous strategy clocks, and then explicitly arm a new $500 prospective cohort. Until that is merged, `paper_signal_promotion_enabled` remains false.
