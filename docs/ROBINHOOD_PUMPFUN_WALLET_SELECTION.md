# Robinhood Chain wallet-universe selection

Robinhood Chain uses the same research-selection architecture as the Pump.fun wallet discovery plane.

## Capacity is a ceiling

The global high-priority universe may contain at most 12 wallets/entities. It is not required to contain 12. Empty slots are preferred to weak or negative wallets that do not deserve scarce observation bandwidth.

## Candidate process

1. Broadly sample already-ingested Robinhood Chain swaps with the Pump.fun discovery cadence: modulus 20, bounded scan 600.
2. Evaluate one discovered candidate at a time from up to 120 recent local swaps.
3. Require the same historical research gate used by Pump.fun: at least 5 closed episodes, 5 distinct tokens, return on capital above 5%, and profit factor above 1.05.
4. Historical evidence only grants prospective research bandwidth. It has no paper-trading promotion authority.
5. Start a fresh forward clock after a candidate passes the historical screen. Publicly researched high-ROI seed wallets likewise start a fresh prospective clock and are hypotheses, not a permanent whitelist.
6. Forward observations come only from the already-ingested Robinhood swap ledger; no wallet-specific provider polling is added.
7. Mature forward evidence is role-scored. A mature candidate with nonpositive forward geometric value releases its slot rather than remaining merely to fill capacity.
8. Better challengers can replace weaker incumbents. Lane, regime, venue, lifecycle, risk, and flow remain strategy context rather than separate wallet watchlists.

## Authority boundaries

Wallet selection alone cannot authorize or resize a paper trade. Exact executable buy/sell quotes, decision-time entity resolution, compatible forward paper evidence, risk controls, and the existing continuation strategy remain authoritative. The system remains paper-only with no signing, submission, or live-money authority.
