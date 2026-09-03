# Post-minimum hydration capacity boundary

This repair adapts `post_minimum_hydration_capacity.patch` to the current production implementation, where the equivalent raw-dispatch batch logic lives in `full_scope_dispatch_capacity_repair.py` rather than the patch's older `raw_dispatch_full_batch_repair.py` path.

The durable raw receipt scope is unchanged: every unique frozen-program receipt remains persisted. Launch-like receipts and frozen-scout receipts keep their existing hydration behavior and priorities. Ordinary deterministic market-sample hydration is now used only while a source still needs normalized bootstrap coverage. Once the source minimum is satisfied, ordinary audit hydration stops so read-only public RPC capacity remains available to continuity and prospective evidence lanes.

Unchanged boundaries:

- all certification thresholds;
- the 12-second continuity recovery lease;
- the 3 x 1000 recovery bound;
- full raw receipt scope and no-drop durability;
- launch/scout hydration semantics;
- strategy and promotion thresholds;
- paper-only authority, with no signing or transaction submission capability.
