Dispatch flow for produce paths
===============================

Entry gate (broker thread)
--------------------------
```
rd_kafka_broker_serve()
    -> if rk_conf.produce_engine == v2
           rd_kafka_broker_producer_serve_v2()
       else
           rd_kafka_broker_producer_serve_v1()
```

V1 path (legacy per-partition batching)
---------------------------------------
```
rd_kafka_broker_producer_serve_v1()
    -> rd_kafka_toppar_producer_serve_v1(rktp...)    // per-toppar batching
         -> rd_kafka_ProduceRequest_mbv1(...)
              -> rd_kafka_msgset_create_ProduceRequest_mbv1(...)
                 (single partition)
         -> optional: rd_kafka_MultiBatchProduceRequest_mbv1(...)
              -> rd_kafka_msgset_create_ProduceRequest_mbv1(...) per batch
```
Key helpers on this path live in:
- src/rdkafka_request.c: rd_kafka_ProduceRequest_mbv1, rd_kafka_MultiBatchProduceRequest_mbv1
- src/rdkafka_msgset_writer_v1.c: rd_kafka_msgset_create_ProduceRequest_mbv1 and writer helpers

V2 path (broker-level batch collector)
--------------------------------------
```
rd_kafka_broker_producer_serve_v2()
    -> rd_kafka_toppar_producer_serve_v2(rktp...)      // moves msgs to xmit
         -> rd_kafka_broker_batch_collector_add_mbv2(rktp)
    -> rd_kafka_broker_batch_collector_maybe_send_mbv2()
         -> rd_kafka_ProduceRequest_init_mbv2(...)
         -> rd_kafka_ProduceRequest_append_mbv2(...)   // per toppar
         -> rd_kafka_ProduceRequest_finalize_mbv2(...)
             -> rd_kafka_msgset_create_ProduceRequest_mbv2(...) per toppar
```
Key helpers on this path live in:
- src/rdkafka_request.c: rd_kafka_ProduceRequest_init_mbv2 / append / finalize
- src/rdkafka_msgset_writer_v2.c: rd_kafka_msgset_create_ProduceRequest_mbv2 and writer helpers
- src/rdkafka_broker.c: batch collector add / maybe_send / next_wakeup

Config gate summary
-------------------
- `produce.engine=v2` selects v2 path above.
- `produce.engine=v1` keeps v1 path.
- `multibatch` only affects the v1 path.
