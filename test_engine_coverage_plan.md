Producer Engine Test Plan (v1 vs mbv2)
======================================

Goals
-----
- Exercise the producer stack under both engines (legacy mbv1 and mbv2).
- Catch regressions in routing/selection (`multibatch` vs `multibatch_v2`).
- Keep cost reasonable: reuse existing tests, avoid duplicating heavy suites.

Scope
-----
- Unit-level: msgset writer, request build/parse, batch collector basics.
- Functional/integration: producer send path with mock broker; engine selection.
- Out of scope: consumer tests, perf/benchmarks, non-producer code.

Plan
----
1) **Unit test dual-run helper**
   - Add a small helper to run existing producer unit tests twice, setting:
     - mbv1: `multibatch=true`, `multibatch_v2=false`
     - mbv2: `multibatch=false`, `multibatch_v2=true`
   - Target tests: `unittest_msgset_writer`, `unittest_request` (and any other producer-only unittests).
   - Avoid body duplication: shared test body, engine set via helper.

2) **Engine selection test (mock broker)**
   - Start mock broker; run produce with:
     - mbv1 config → assert mbv1 path taken (collector inactive, mbv1 handlers hit).
     - mbv2 config → assert collector active / mbv2 handlers hit.
     - both true → assert config parse fails (existing check).
   - Use existing mock/telemetry hooks if available; otherwise minimal counters/log hooks.

3) **Functional producer suite parameterization**
   - For existing producer functional tests (basic produce, retries, idempotent, transactional where already covered), run with two configs (mbv1, mbv2).
   - Keep the parameterization lightweight (loop in test runner or CTest matrix) to avoid duplicating code.
   - Skip heavy/perf suites for mbv2 until needed.

4) **CI hook**
   - Add a matrix entry (or test runner loop) to run producer tests under both engines.
   - Allow opt-out for local fast runs (single-engine default).

Open questions / decisions
--------------------------
- Which functional test harness to parameterize (pytest/CTest/custom)? pick minimal-touch approach.
- How to assert “mbv1 vs mbv2 path” in the mock test (e.g., counters, logs, or collector membership flag).

Next actions
------------
- Implement step 1 (unit dual-run helper) and step 2 (engine selection test).
- Then wire step 3 (runner parameterization) and add CI matrix entry (step 4).
