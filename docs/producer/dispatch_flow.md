Dispatch flow for produce paths
===============================

Entry gate (broker thread)
--------------------------
```
rd_kafka_broker_serve()
    -> if rk_conf.produce_engine == RD_KAFKA_PRODUCE_ENGINE_V2
           rd_kafka_broker_producer_serve_mbv2()
       else
           rd_kafka_broker_producer_serve_mbv1()
```

V1 path (legacy per-toppar batching + optional multibatch stitch)
------------------------------------------------------------------
High-level shape:
```
rd_kafka_broker_produce_toppars_mbv1()
    -> for each active toppar:
         rd_kafka_toppar_producer_serve_mbv1(...)
            -> move rktp_msgq -> rktp_xmit_msgq
            -> wait for batch_ready (buffering_max_us / batch thresholds)
            -> rd_kafka_ProduceRequest_mbv1(..., skip_sending, batch_bufq)
               -> rd_kafka_msgset_create_ProduceRequest_mbv1(...)
                  (encodes ONE topic/partition request)
               -> if skip_sending=false: enqueue now
                  else: keep prebuilt rkbuf in batch_bufq
    -> if multibatch enabled and batch_bufq not empty:
         rd_kafka_MultiBatchProduceRequest_mbv1(...)
```

How V1 "rips apart and stitches together" old produce requests:
- `rd_kafka_msgset_create_ProduceRequest_mbv1()` builds normal single-partition ProduceRequests first.
- While encoding each one, writer stores byte markers in rkbuf:
  - `rkbuf_u.rkbuf_produce.v1.batch_start_pos`
  - `rkbuf_u.rkbuf_produce.v1.batch_end_pos`
- `rd_kafka_MultiBatchProduceRequest_mbv1()` then:
  1. Sorts queued single-partition rkbufs by topic.
  2. Picks `[cur_ind, next_ind)` that fit `max_msg_size`.
  3. Allocates a new request rkbuf.
  4. For each source rkbuf in range:
     - Writes partition id.
     - Copies already-encoded batch bytes using
       `rd_kafka_copy_batch_buf_mbv1()` (slice from `batch_start_pos` to
       `batch_end_pos`).
     - Moves message ownership from source batch msgq into a new
       `rd_kafka_msgbatch_t` in `request_rkbuf->...v1.batch_list`
       (`rd_kafka_msgq_move`, no message re-encode).
  5. Finalizes topic/partition array counts and topic tags with
     `finalize_topic_encoding_mbv1()`.
  6. Sets request timeout from the earliest first-message timeout across all
     included batches (`get_first_msg_timeout_mbv1()`).
  7. Enqueues one stitched request with
     `rd_kafka_handle_MultiBatchProduce_mbv1`.

V1 response mapping back to original batches:
- Multi-batch parse (`rd_kafka_handle_Produce_parse_mbv1`) walks response
  topic/partition entries.
- For each entry, it finds the original msgbatch index via
  `rd_kafka_find_msgbatch_mbv1(topic, partition)` against `batch_list`.
- `rd_kafka_handle_MultiBatchProduce_mbv1` then applies each partition result
  to its corresponding original msgbatch with
  `rd_kafka_msgbatch_handle_Produce_result_mbv1`.

Notes:
- `multibatch` is disabled when idempotence is enabled
  (`multi_batch_request = false` in that mode).
- In multibatch mode, each toppar contributes at most one prebuilt batch per
  serve iteration before final stitch/send.

V2 path (broker-level collector + calculator + context writer)
---------------------------------------------------------------
High-level shape:
```
rd_kafka_broker_produce_toppars_mbv2()
    -> for each active toppar:
         rd_kafka_toppar_producer_serve_mbv2(...)
            -> move rktp_msgq -> rktp_xmit_msgq
            -> update wakeups/state, but do not build request yet
         if rktp_xmit_msgq non-empty:
            rd_kafka_broker_batch_collector_add_mbv2(rktp)

    -> rd_kafka_broker_batch_collector_maybe_send_mbv2(now, flushing)
         -> if send criteria met:
              rd_kafka_broker_batch_collector_send_mbv2()
                 -> produce_batch_try_init()
                 -> produce_batch_append(rktp) [uses calculator]
                 -> produce_batch_send()
                    -> rd_kafka_ProduceRequest_init_mbv2(...)
                    -> rd_kafka_ProduceRequest_append_mbv2(... per toppar)
                    -> rd_kafka_ProduceRequest_finalize_mbv2(...)
```

Collector send triggers (`rd_kafka_broker_batch_collector_maybe_send_mbv2`):
- `flush` requested.
- `broker_batch_max_bytes` reached.
- linger expired (`rd_kafka_adaptive_get_linger_us(rkb)`).
- Also requires broker `UP` and outbuf space.

V2 calculator details (`rd_kafka_produce_calculator_add`):
- Seeded once per candidate request by
  `rd_kafka_produce_calculator_init_mbv2()` using negotiated
  ApiVersion/MsgVersion and derived header sizes.
- Admission checks include:
  - `produce_request_max_partitions`.
  - Same `required_acks` and `request_timeout_ms` across all included topics.
  - Estimated request size <= `max_msg_size`.
- It tracks running totals: topic count, partition count, message count, payload
  bytes, plus variable-length field sizes.
- For a candidate toppar it computes how many messages fit given
  `batch_num_messages`, queue bytes, header overhead, and current request size.
- Current limitation: one message-set pass per toppar per request attempt
  (the loop breaks after first accepted pass), so remaining messages stay queued
  for later requests.

How V2 builds the request after calculator admission:
- `produce_batch_send()` groups accepted toppars by topic, then appends them in
  topic-grouped order.
- `rd_kafka_ProduceRequest_init_mbv2()` allocates a context (`rkpc`) plus
  per-toppar request bookkeeping map (`rkprc_toppar_info`).
- `rd_kafka_ProduceRequest_append_mbv2()` delegates to
  `rd_kafka_produce_ctx_append_toppar_mbv2()` to write topic/partition/message
  bytes and record appended counts.
- Appending also stores per-toppar sequencing and message pointer metadata in
  the request hash map (`assign_toppar_info`) for response handling.
- `rd_kafka_ProduceRequest_finalize_mbv2()` finalizes headers, sets timeout from
  earliest message timeout, updates in-flight counters, and enqueues callback
  `rd_kafka_handle_Produce_mbv2`.

V2 response mapping:
- Parser uses `(topic pointer, partition)` lookup in the request hash map
  (`get_toppar_info`) instead of scanning a list.
- It validates topic/partition counts against expected request counts and then
  applies result handling per tracked toppar.

Efficiency gains (why v2 tends to win)
--------------------------------------
Code-level reasons v2 is typically more efficient than v1:
- V1 builds per-toppar ProduceRequests first (`rd_kafka_ProduceRequest_mbv1`)
  and may later restitch them (`rd_kafka_MultiBatchProduceRequest_mbv1`), while
  v2 delays full request build until candidate partitions are selected.
- V1 multibatch performs extra byte-range copying from prebuilt buffers
  (`rd_kafka_copy_batch_buf_mbv1`) plus wrapper allocation/movement for each
  source batch (`rd_kafka_msgbatch_t` in `batch_list`), which is avoidable work.
- V2 uses broker-level collection (`rd_kafka_broker_batch_collector_*`) to make
  one send decision across many partitions, rather than primarily per-toppar
  send decisions.
- V2 runs a pre-admission calculator (`rd_kafka_produce_calculator_add`) before
  serialization, so non-fitting partitions are rejected early instead of
  building more temporary request state.
- V2 writes directly into one produce context (`rd_kafka_produce_ctx_*`) and
  finalizes once, reducing rebuild/re-encode/stitch churn.
- V1 multibatch response correlation scans the batch list by topic+partition
  (`rd_kafka_find_msgbatch_mbv1`), while v2 response correlation uses a
  preallocated per-request toppar map (`assign_toppar_info` /
  `get_toppar_info`) for more direct lookup.
- The net effect is usually fewer requests, fewer bytes of internal copying,
  and less broker-thread coordination overhead per delivered message.

Important caveats:
- V2 still pays collector bookkeeping costs (collector scans and wakeup logic),
  so gains are workload-dependent.
- Current v2 calculator path admits one message-set pass per toppar per request
  attempt; deeper queue draining happens over subsequent requests.

Key helpers by file
-------------------
- `src/rdkafka_broker.c`:
  - `rd_kafka_toppar_producer_serve_mbv1`
  - `rd_kafka_toppar_producer_serve_mbv2`
  - collector: `add/maybe_send/send/next_wakeup`
  - produce_batch helpers: `try_init/append/send`
- `src/rdkafka_request.c`:
  - V1: `rd_kafka_ProduceRequest_mbv1`,
    `rd_kafka_MultiBatchProduceRequest_mbv1`,
    `rd_kafka_handle_MultiBatchProduce_mbv1`
  - V2: `rd_kafka_ProduceRequest_init_mbv2`,
    `rd_kafka_ProduceRequest_append_mbv2`,
    `rd_kafka_ProduceRequest_finalize_mbv2`
- `src/rdkafka_msgset_writer_v1.c`:
  - `rd_kafka_msgset_create_ProduceRequest_mbv1` and v1 writer internals
- `src/rdkafka_msgset_writer_v2.c`:
  - calculator + produce context internals

Config gate summary
-------------------
- `produce.engine=v2` selects collector/calculator/context path.
- `produce.engine=v1` selects legacy per-toppar path.
- `multibatch` only changes v1 behavior (single-partition prebuild + stitch).
