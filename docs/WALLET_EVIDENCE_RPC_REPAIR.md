# Wallet evidence and RPC load repair

Production on release `d69690e557c910d1fe18ce94eae65e4660d46df8` proved fresh realtime wallet marks are now timely (about 0.84s p50 / 5.4s p95 inside the unchanged 20-second SLA), but three downstream correctness/capacity defects remained:

1. copyability failures had no reason breakdown;
2. delayed wallet risk work repeatedly refreshed all six token-risk dimensions per signature, including launch/funding history, and could evaluate newly collected evidence against an older observation timestamp;
3. transient RPC failure while starting a realtime wallet epoch could leave a null anchor that later traffic could treat as an active prospective epoch.

This repair keeps all strategy/promotion/certification thresholds unchanged. It adds copyability rejection accounting, requires risk completion to use evidence that existed at observation time, prewarms missing token evidence only at the actual collection time for future observations, terminates old/unverifiable risk jobs fail-closed instead of retrying them indefinitely, coalesces token prewarming to at most once every 20 seconds, and makes the first genuine live WebSocket receipt an anchor-only boundary when RPC cannot establish the epoch anchor.

No private key, signing, transaction submission, paper authority, or live-money capability is added. No historical or delayed evidence gains promotion authority.
