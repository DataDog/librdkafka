# Legacy Producer Parity Plan (Safety Valve)

## Goal
Provide a true historical-parity safety valve so CI and production can switch between:
- `legacy` producer path (behavior equivalent to `origin/master`)
- `v2` producer path (new collector/multibatch/adaptive path)

This is not a wrapper-only fallback. It restores historical producer internals and data contracts.


## Runtime Selection
- Add one producer-engine selector with values: `legacy`, `v2`.
- Default: `legacy`.
- Add env override for CI (name to be finalized in implementation).
- Precedence: explicit config > env > default.
- Selection occurs once, then broker produce flow dispatches to engine-specific path.


## Exact Legacy Function Set To Restore

### 1) Broker path (`src/rdkafka_broker.c`)
Restore legacy implementations (from `origin/master`) and keep callable:
- `buf_contains_toppar` (`origin/master` `src/rdkafka_broker.c:776`)
- `rd_kafka_broker_bufq_purge_by_toppar` (`origin/master` `src/rdkafka_broker.c:807`)
- `rd_kafka_toppar_producer_serve` legacy signature including `multi_batch_request` + `batch_bufq` (`origin/master` `src/rdkafka_broker.c:3880`)
- `rd_kafka_broker_produce_toppars` legacy flow with `batch_bufq` + `rd_kafka_MultiBatchProduceRequest` (`origin/master` `src/rdkafka_broker.c:4179`)
- `rd_kafka_broker_producer_serve` legacy loop (no adaptive-adjust hook) (`origin/master` `src/rdkafka_broker.c:4252`)
- `RD_KAFKA_OP_PARTITION_LEAVE` legacy per-toppar purge behavior (`origin/master` `src/rdkafka_broker.c:3360`)

Current v2 anchors to dispatch around:
- `src/rdkafka_broker.c:4560`
- `src/rdkafka_broker.c:4863`
- `src/rdkafka_broker.c:4996`


### 2) Request/response path (`src/rdkafka_request.c`)
Restore legacy produce request/reply machinery:
- `rd_kafka_handle_Produce_metadata_update` legacy signature (`origin/master` `src/rdkafka_request.c:3633`)
- `rd_kafka_find_msgbatch` (`origin/master` `src/rdkafka_request.c:3690`)
- `produce_reply_tags_cleaup` (`origin/master` `src/rdkafka_request.c:3710`)
- `rd_kafka_handle_Produce_parse` legacy signature `(results**, multi_batch_request)` (`origin/master` `src/rdkafka_request.c:3738`)
- `rd_kafka_handle_idempotent_Produce_error` legacy behavior/signature (`origin/master` `src/rdkafka_request.c:3929`)
- `rd_kafka_handle_Produce_error` legacy behavior/signature (`origin/master` `src/rdkafka_request.c:4284`)
- `rd_kafka_handle_idempotent_Produce_success` legacy behavior/signature (`origin/master` `src/rdkafka_request.c:4616`)
- `rd_kafka_msgbatch_handle_Produce_result_record_errors` (`origin/master` `src/rdkafka_request.c:4717`)
- `rd_kafka_msgbatch_handle_Produce_result` legacy signature (`origin/master` `src/rdkafka_request.c:4768`)
- `rd_kafka_handle_Produce` legacy callback (`origin/master` `src/rdkafka_request.c:4867`)
- `rd_kafka_handle_MultiBatchProduce` (`origin/master` `src/rdkafka_request.c:4911`)
- `rd_kafka_ProduceRequest` legacy signature with `skip_sending` + `batch_bufq` (`origin/master` `src/rdkafka_request.c:4950`)
- `rd_kafka_fill_MultiBatch_header` (`origin/master` `src/rdkafka_request.c:5016`)
- `rd_kafka_buf_cmp_by_topic` (`origin/master` `src/rdkafka_request.c:5039`)
- `rd_kafka_copy_batch_buf` (`origin/master` `src/rdkafka_request.c:5057`)
- `select_batches_to_include` (`origin/master` `src/rdkafka_request.c:5075`)
- `get_first_msg_timeout` (`origin/master` `src/rdkafka_request.c:5108`)
- `finalize_topic_encoding` (`origin/master` `src/rdkafka_request.c:5132`)
- `rd_kafka_MultiBatchProduceRequest` (`origin/master` `src/rdkafka_request.c:5157`)

Current v2 request anchors:
- `src/rdkafka_request.c:3914`
- `src/rdkafka_request.c:5201`
- `src/rdkafka_request.c:5358`
- `src/rdkafka_request.c:5404`
- `src/rdkafka_request.c:5445`
- `src/rdkafka_request.c:5516`


### 3) MessageSet writer path (`src/rdkafka_msgset_writer.c`)
Restore legacy single-batch writer and request constructor:
- `rd_kafka_msgset_writer_alloc_buf` (`origin/master` `src/rdkafka_msgset_writer.c:243`)
- `rd_kafka_msgset_writer_write_Produce_header` (`origin/master` `src/rdkafka_msgset_writer.c:432`)
- `rd_kafka_msgset_writer_init` legacy signature (`origin/master` `src/rdkafka_msgset_writer.c:490`)
- `rd_kafka_msgset_writer_write_msg` (`origin/master` `src/rdkafka_msgset_writer.c:774`)
- `rd_kafka_msgset_writer_write_msgq` (`origin/master` `src/rdkafka_msgset_writer.c:813`)
- `rd_kafka_msgset_writer_compress_gzip` / `lz4` / `zstd` / `compress` (`origin/master` `src/rdkafka_msgset_writer.c:1038`, `1118`, `1136`, `1156`)
- `rd_kafka_msgset_writer_finalize_MessageSet_v2_header` (`origin/master` `src/rdkafka_msgset_writer.c:1274`)
- `rd_kafka_msgset_writer_finalize` (`origin/master` `src/rdkafka_msgset_writer.c:1362`)
- `rd_kafka_msgset_create_ProduceRequest` legacy implementation (`origin/master` `src/rdkafka_msgset_writer.c:1455`)

Current v2 writer anchors:
- `src/rdkafka_msgset_writer.c:1972`
- `src/rdkafka_msgset_writer.c:2040`
- `src/rdkafka_msgset_writer.c:2223`
- `src/rdkafka_msgset_writer.c:2261`


## Required Data Contracts To Restore (for true parity)
These are mandatory, not optional:
- `rkbuf_u.Produce.batch_list`, `batch_start_pos`, `batch_end_pos` in `src/rdkafka_buf.h` (present in `origin/master`).
- Produce buffer destroy logic for `batch_list` in `src/rdkafka_buf.c` (`origin/master` behavior).
- `rd_kafka_Produce_result_t.errorcode` in `src/rdkafka_msg.h`.
- Legacy request prototypes in `src/rdkafka_request.h`:
  - old `rd_kafka_ProduceRequest(...)` signature
  - `rd_kafka_MultiBatchProduceRequest(...)`.
- Legacy `multibatch` config field/property in:
  - `src/rdkafka_conf.h`
  - `src/rdkafka_conf.c`.
- Legacy wakeup semantics in `rd_kafka_msgq_allow_wakeup_at` (`src/rdkafka_msg.c:1777`) if timing parity is required, not just functional parity.


## Engine Split Boundaries
- `legacy` engine owns:
  - legacy broker produce scheduling,
  - legacy request construction/parsing,
  - legacy multibatch aggregator path.
- `v2` engine owns:
  - collector + produce calculator/context path,
  - adaptive-batching integration.
- Shared code stays shared only if behavior is byte-for-byte equivalent for both engines.


## Broker Struct Strategy (Required)
- Do **not** duplicate `struct rd_kafka_broker_s`.
- Keep one broker core struct and isolate engine-specific state behind indirection.
- Add only minimal engine selection fields to core broker state, for example:
  - `rkb_producer_engine` (enum)
  - `rkb_producer_engine_state` (opaque pointer)
- Move v2-only fields out of core and into a v2-only state struct allocated only when engine=`v2`, including:
  - `rkb_batch_collector`
  - `rkb_adaptive_state`
  - `rkb_adaptive_params`
  - v2-only produce stats/averages/counters
- Legacy engine must not allocate or depend on v2 engine state.
- This keeps parity isolation while avoiding duplicated broker lifecycle/refcount/threading logic.


## Struct Partitioning Matrix

### Required v2 sub-state splits
1. `struct rd_kafka_broker_s` (`src/rdkafka_broker.h`)
- Keep one shared broker core.
- Move v2-only producer state under engine-owned state (not core fields), including:
  - batch collector state
  - adaptive state/params
  - v2-only produce stats/averages/counters

2. `struct rd_kafka_toppar_s` (`src/rdkafka_partition.h`)
- Add `producer_v2` sub-section for v2-only partition fields:
  - collector/batch links
  - collector membership flag
  - xmit enqueue timestamp and v2-only xmit mirrors for v2 stats visibility
- Legacy path must not depend on these fields being present/initialized.

3. `struct rd_kafka_buf_s` produce union (`src/rdkafka_buf.h`)
- Split produce request state into explicit legacy/v2 variants.
- Legacy variant must support historical multibatch contracts (`batch_list`, record-batch copy markers).
- v2 variant should hold only v2 request bookkeeping.


### Legacy compatibility contract fields (required)
1. `rd_kafka_Produce_result_t` (`src/rdkafka_msg.h`)
- Restore/retain legacy fields required by legacy parse and multibatch response mapping (including historical `errorcode` semantics).
- Keep v2 path free to use its own response bookkeeping path.

2. Request API shape (`src/rdkafka_request.h`)
- Maintain legacy request function signatures required by legacy engine path.
- Keep v2 request/context APIs in parallel without cross-coupling.


### Optional (recommended) grouping for clarity
1. `struct rd_kafka_conf_s` (`src/rdkafka_conf.h`)
- Group v2 producer knobs into a config sub-block for readability and ownership clarity:
  - `produce.request.max.partitions`
  - `broker.linger.ms` / `broker.batch.max.bytes`
  - `adaptive.*`
- This is a maintainability improvement, not a hard parity blocker.


### Keep shared (no extra split needed)
1. `struct rd_kafka_produce_calculator_s` (`src/rdkafka_msgset.h`)
2. `struct rd_kafka_produce_ctx_s` (`src/rdkafka_msgset.h`)
- These are already v2-scoped helper/context types and can remain as dedicated v2 machinery.

## Implementation Phases

### Phase 1: Introduce engine selector and dispatch seam
- Add selector config + env override.
- Wire dispatch in producer serve/prod-toppar call chain.
- No behavioral changes yet, only structure.

### Phase 2: Reintroduce legacy broker and request code paths
- Add/restore legacy broker flow and request flow listed above.
- Keep v2 path untouched.

### Phase 3: Reintroduce legacy writer internals + buffer contracts
- Restore legacy writer and multibatch data contracts (`batch_list`, markers, destroy path).
- Ensure legacy callbacks and parse flow consume restored structures.

### Phase 4: Compatibility cleanup
- Restore/bridge legacy headers/prototypes so both engines compile cleanly.
- Ensure no accidental cross-calls (legacy path must not depend on v2-only contracts).

### Phase 5: Validation matrix
- Run producer tests twice:
  - once with `legacy`
  - once with `v2`.
- Required focus tests:
  - idempotent/EOS produce behavior
  - retries/timeouts/reconnects
  - multibatch request/reply mapping
  - partition migration and purge behavior
  - flush semantics
  - adaptive path gated to `v2`.


## Non-Goals
- This plan does not attempt to deduplicate legacy/v2 logic first.
- This plan prioritizes safety valve and parity over cleanup.
- Refactoring for reduced duplication can happen after parity is proven.


## Parity Acceptance Criteria
- With engine=`legacy`, behavior matches `origin/master` for produce scheduling, request building/parsing, retry/idempotent handling, and multibatch behavior.
- Engine selection can be toggled without rebuilding images.
- CI can run both engines and report regressions independently.
