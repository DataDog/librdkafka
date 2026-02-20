# Codex Handoff (2026-02-20)

## Snapshot
- Repo: `/home/bits/go/src/github.com/DataDog/librdkafka`
- Branch: `rodrigo.silvamendoza/new-multibatch`
- HEAD: `35bf0c8454c55be5bc6b2dd0da9c250f5031f990`
- Tracked working tree: clean (`git status --short` shows no tracked modifications).
- There are many untracked files/docs/artifacts in root (plans, logs, images, etc.).

## Objective (current thread)
Build a safe, runtime-switchable producer safety valve with historical parity:
- keep legacy behavior available (v1 path),
- keep new producer path available (v2 path),
- avoid forcing new behavior by image replacement only.

## Decisions already made in this thread
- Multibatch engine naming uses `_mbv1` / `_mbv2` (not plain `_v1` / `_v2`) to avoid collision with MessageSet protocol versioning names.
- MessageSet protocol-version helpers should stay `_v2` (example: `rd_kafka_msgset_writer_write_msg_v2`).
- Tiny file-local helpers (example: `rd_kafka_arraycnt_wire_size`) do not need `_mbv2` suffix unless there is a real v1/v2 twin collision.
- For new v2-only functions with no legacy counterpart, still tag as `_mbv2` so ownership is explicit.
- In files where both variants exist, keep v1 definitions before v2 when feasible.
- Config direction preference captured from discussion:
  - `multibatch=true` => v1 engine.
  - `multibatch_v2=true` => v2 engine.
  - both enabled should be rejected as mutually exclusive.
  - `produce.request.max.partitions` should only apply to v2 (or be removed later).

## Existing planning docs to use as source of truth
- `legacy_producer_parity_plan.md` (primary plan, function/struct mapping + phases).
- `functions_changed.txt` (manual inventory + validation notes).
- `multibatch_plan.txt` (earlier implementation plan and context).
- `multibatch_review.txt`, `future_work_multibatch.txt`, `PRODUCE_PATH_GUIDE.md` (additional context).

## Rename progress already present

### In `src/rdkafka_msgset_writer.c` (engine path renamed to `_mbv2`)
- `rd_kafka_msgset_writer_init_mbv2`
- `rd_kafka_produce_calculator_init_mbv2`
- `rd_kafka_produce_ctx_init_mbv2`
- `rd_kafka_produce_ctx_append_toppar_mbv2`
- `rd_kafka_produce_ctx_finalize_mbv2`
- `rd_kafka_msgset_create_ProduceRequest_mbv2`
- plus helper cluster with `_mbv2` suffixes (`select_caps`, request sizing/alloc, topic-header helpers, writer finalize helpers).

### In `src/rdkafka_msgset_writer_v1.c` (renamed to `_mbv1`)
- Core writer path functions are `_mbv1` (alloc/init/write/finalize/create request).
- `rd_kafka_msgset_create_ProduceRequest_mbv1` exists and is declared in `src/rdkafka_msgset.h`.

### In headers/callers
- `src/rdkafka_msgset.h` declares `_mbv1`/`_mbv2` variants.
- `src/rdkafka_broker.c` uses `rd_kafka_produce_calculator_init_mbv2`.

## Known mismatches/open fixes (high priority)

### Hard build failure
- `src/rdkafka_conf.c:4126` uses `conf->multibaych` (typo) and fails compile.
  - Error from full build: `rd_kafka_conf_t has no member named multibaych; did you mean multibatch`.

### Unsuffixed callsites still referencing old names
- `src/rdkafka_request.c:6661`, `src/rdkafka_request.c:9225`, `src/rdkafka_request.c:9314`
  - still call `rd_kafka_msgset_create_ProduceRequest(...)` (should align to chosen engine symbol).
- `src/rdkafka_request.c:7037`, `src/rdkafka_request.c:7185`, `src/rdkafka_request.c:9522`
  - still call `rd_kafka_produce_ctx_finalize(...)` (should align to `_mbv2` if this path is v2).
- `src/rdkafka_msgset_writer.c:1975`, `src/rdkafka_msgset_writer.c:2601`
  - still call `rd_kafka_produce_ctx_finalize(...)`.
- `src/rdkafka_msgset_writer.c:1913`
  - still calls `rd_kafka_produce_finalize_topic_header(...)` while helper is currently `rd_kafka_produce_finalize_topic_header_mbv2`.

### Legacy helper naming mismatch
- `src/rdkafka_request.c:6847`
  - calls `rd_slice_read_into_buf(...)`, but current exposed symbol appears to be `rd_slice_read_into_buf_v1(...)` in `src/rdbuf.h`.

### Comment/docs drift
- `src/rdkafka_msgset.h:93` comment still mentions `rd_kafka_produce_ctx_finalize` without `_mbv2`.

## Items to review for naming consistency
- `src/rdkafka_msgset_writer.c` has `rd_kafka_msgset_writer_finalize_MessageSet_v2_header_mbv2`.
  - This function name mixes MessageSet protocol version (`v2`) and engine suffix (`_mbv2`).
  - Confirm if this is intended, since rule was to keep protocol-version names `_v2` to avoid confusion.

## Build/verification commands run
- `make -C src -B rdkafka_request.o`
  - succeeds with warnings, including implicit declarations at the unsuffixed callsites above.
- `make -C src -B rdkafka_msgset_writer.o`
  - succeeds with warnings, including implicit declarations at unsuffixed/mismatch callsites above.
- `make -C src`
  - fails at `src/rdkafka_conf.c:4126` due `multibaych` typo.

## Suggested resume sequence for next session
1. Fix compile stopper in `src/rdkafka_conf.c` (`multibaych` typo).
2. Resolve all unsuffixed legacy names listed above to the intended engine-specific names.
3. Decide/normalize any mixed protocol-vs-engine function names (notably `...MessageSet_v2_header_mbv2`).
4. Rebuild with `make -C src` and then run targeted producer tests.
5. Continue with runtime gating seam (engine dispatch + mutually exclusive config semantics).

