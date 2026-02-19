Producer engine unittests to consider duplicating (mbv1 + mbv2)
===============================================================

Dual-run candidates (existed in origin/master)
- unittest_idempotent_producer (request/produce path)
- unittest_handle_GetTelemetrySubscriptions (telemetry; probably single-run despite existing)

New in this branch (did not exist in origin/master)
- All unittest_msgset_writer_* subtests (writer path)
- unittest_handle_Produce_orphaned_msg_drain (request path)

Approach
- Only consider dual-running tests that existed in main to maintain parity (idempotent_producer; optionally telemetry).
- New tests can stay single-run unless we explicitly want mbv1 coverage for added functionality.

Notes
- There are no existing mbv1-specific unittests; these are the only producer-focused ones in-tree.
- Non-producer unittests don’t touch mbv1/mbv2 helpers.
