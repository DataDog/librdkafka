/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Datadog Inc.
 * All rights reserved.
 *
 *
 */

/**
 * Verify that mbv2 applies batch.size to each partition MessageSet, not to the
 * full multi-partition ProduceRequest envelope.
 *
 * This is a live broker test. It creates one more partition than there are
 * brokers, with replication factor 1, so at least one broker leads multiple
 * partitions. Each partition's MessageSet fits under batch.size, but any broker
 * that owns two partitions must build a ProduceRequest larger than batch.size.
 */

#include "test.h"
#include "rdkafka.h"


#define MSGS_PER_PARTITION 100
#define PAYLOAD_SIZE       4096
#define BATCH_SIZE         512000


typedef struct {
        mtx_t lock;
        int dr_remains;
        int dr_errors;
        int64_t last_total_requests;
        int64_t last_produce_requests;
        int64_t last_stats_ts_us;
} test_ctx_t;


static void stats_cb_produce_requests(rd_kafka_t *rk,
                                      const rd_kafka_stats_t *stats,
                                      void *opaque) {
        test_ctx_t *ctx = opaque;
        int64_t total_requests = 0;
        int64_t produce_requests = 0;
        uint32_t i;

        (void)rk;

        for (i = 0; i < stats->broker_cnt; i++) {
                const rd_kafka_broker_stats_t *broker = &stats->brokers[i];
                uint32_t j;

                total_requests += broker->tx;
                for (j = 0; j < broker->req_cnt; j++)
                        if (!strcmp(broker->reqs[j].name, "Produce"))
                                produce_requests += broker->reqs[j].count;
        }

        mtx_lock(&ctx->lock);
        ctx->last_total_requests   = total_requests;
        ctx->last_produce_requests = produce_requests;
        ctx->last_stats_ts_us      = stats->ts_us;
        mtx_unlock(&ctx->lock);
}


static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
        test_ctx_t *ctx = opaque;

        (void)rk;

        mtx_lock(&ctx->lock);
        if (rkmessage->err)
                ctx->dr_errors++;
        ctx->dr_remains--;
        mtx_unlock(&ctx->lock);
}


static void stats_snapshot(test_ctx_t *ctx,
                           int64_t *total_requests,
                           int64_t *produce_requests,
                           int64_t *stats_ts_us) {
        mtx_lock(&ctx->lock);
        *total_requests   = ctx->last_total_requests;
        *produce_requests = ctx->last_produce_requests;
        *stats_ts_us      = ctx->last_stats_ts_us;
        mtx_unlock(&ctx->lock);
}


static int dr_snapshot(test_ctx_t *ctx, int *dr_errors) {
        int dr_remains;

        mtx_lock(&ctx->lock);
        dr_remains = ctx->dr_remains;
        *dr_errors = ctx->dr_errors;
        mtx_unlock(&ctx->lock);

        return dr_remains;
}


static int topic_unique_leader_count(rd_kafka_t *rk,
                                     const char *topic,
                                     int partition_cnt) {
        rd_kafka_topic_t *rkt;
        const rd_kafka_metadata_t *metadata;
        const struct rd_kafka_metadata_topic *mdt;
        rd_kafka_resp_err_t err;
        int32_t *leaders;
        int leader_cnt = 0;
        int i;

        rkt = rd_kafka_topic_new(rk, topic, NULL);
        TEST_ASSERT(rkt, "failed to create topic object for %s", topic);

        err = rd_kafka_metadata(rk, 0, rkt, &metadata, tmout_multip(10000));
        TEST_ASSERT(!err, "metadata(%s) failed: %s", topic,
                    rd_kafka_err2str(err));
        TEST_ASSERT(metadata->topic_cnt == 1,
                    "expected metadata for one topic, got %d",
                    metadata->topic_cnt);

        mdt = &metadata->topics[0];
        TEST_ASSERT(!mdt->err, "metadata(%s) returned topic error: %s", topic,
                    rd_kafka_err2str(mdt->err));
        TEST_ASSERT(mdt->partition_cnt == partition_cnt,
                    "metadata(%s) returned %d partitions, expected %d", topic,
                    mdt->partition_cnt, partition_cnt);

        leaders = calloc((size_t)partition_cnt, sizeof(*leaders));
        TEST_ASSERT(leaders, "OOM allocating leader list");

        for (i = 0; i < partition_cnt; i++) {
                int32_t leader = mdt->partitions[i].leader;
                rd_bool_t seen = rd_false;
                int j;

                TEST_ASSERT(leader >= 0,
                            "metadata(%s) partition %d has no leader", topic,
                            mdt->partitions[i].id);

                for (j = 0; j < leader_cnt; j++) {
                        if (leaders[j] == leader) {
                                seen = rd_true;
                                break;
                        }
                }

                if (!seen)
                        leaders[leader_cnt++] = leader;
        }

        free(leaders);
        rd_kafka_metadata_destroy(metadata);
        rd_kafka_topic_destroy(rkt);

        return leader_cnt;
}


static rd_kafka_t *create_producer(test_ctx_t *ctx) {
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 20);
        rd_kafka_conf_set_opaque(conf, ctx);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_stats_cb(conf, NULL);
        rd_kafka_conf_set_stats_cb_typed(conf, stats_cb_produce_requests);

        test_conf_set(conf, "produce.engine", "v2");
        test_conf_set(conf, "statistics.interval.ms", "20");
        test_conf_set(conf, "compression.type", "none");
        test_conf_set(conf, "batch.num.messages", "10000");
        test_conf_set(conf, "batch.size", "512000");
        test_conf_set(conf, "message.max.bytes", "50000000");
        test_conf_set(conf, "queue.buffering.max.messages", "1000000");
        test_conf_set(conf, "queue.buffering.max.kbytes", "100000");
        test_conf_set(conf, "broker.linger.ms", "2000");
        test_conf_set(conf, "broker.batch.max.bytes", "-1");
        test_conf_set(conf, "produce.request.max.partitions", "100000");
        test_conf_set(conf, "message.timeout.ms", "15000");

        return test_create_handle(RD_KAFKA_PRODUCER, conf);
}


static void wait_delivery(rd_kafka_t *rk, test_ctx_t *ctx) {
        int64_t deadline = test_clock() + (int64_t)tmout_multip(30000) * 1000;
        int dr_errors = 0;

        while (dr_snapshot(ctx, &dr_errors) > 0) {
                rd_kafka_poll(rk, 50);
                TEST_ASSERT(test_clock() < deadline,
                            "timed out waiting for delivery reports");
        }

        TEST_ASSERT(dr_errors == 0, "%d delivery reports failed", dr_errors);
        TEST_ASSERT(rd_kafka_flush(rk, tmout_multip(10000)) ==
                        RD_KAFKA_RESP_ERR_NO_ERROR,
                    "flush failed: %s",
                    rd_kafka_err2str(rd_kafka_last_error()));
}


static int64_t wait_produce_request_count(rd_kafka_t *rk,
                                          test_ctx_t *ctx,
                                          int64_t start_produce_requests,
                                          int64_t start_stats_ts_us) {
        int64_t total_requests;
        int64_t produce_requests;
        int64_t stats_ts_us;
        int64_t deadline = test_clock() + (int64_t)tmout_multip(5000) * 1000;

        do {
                rd_kafka_poll(rk, 50);
                stats_snapshot(ctx, &total_requests, &produce_requests,
                               &stats_ts_us);
                if (stats_ts_us > start_stats_ts_us &&
                    produce_requests > start_produce_requests)
                        return produce_requests - start_produce_requests;
        } while (test_clock() < deadline);

        TEST_FAIL("timed out waiting for Produce request stats: "
                  "start=%" PRId64 " end=%" PRId64 " start_ts=%" PRId64
                  " end_ts=%" PRId64,
                  start_produce_requests, produce_requests, start_stats_ts_us,
                  stats_ts_us);
}


int main_0208_multibatch_request_size(int argc, char **argv) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        size_t broker_cnt;
        int32_t *broker_ids;
        int partition_cnt;
        int expected_produce_requests;
        int produced_msgcnt;
        test_ctx_t ctx;
        rd_kafka_t *rk;
        char *payload;
        int64_t start_total_requests;
        int64_t start_produce_requests;
        int64_t start_stats_ts_us;
        int64_t produce_requests;
        rd_bool_t request_count_failed;
        int p;

        (void)argc;
        (void)argv;

        broker_ids = test_get_broker_ids(NULL, &broker_cnt);
        free(broker_ids);

        if (broker_cnt > 32) {
                TEST_SKIP("broker count %" PRIusz
                          " is too high for this request-size regression test",
                          broker_cnt);
                return 0;
        }

        partition_cnt   = (int)broker_cnt + 1;
        produced_msgcnt = partition_cnt * MSGS_PER_PARTITION;

        test_create_topic_wait_exists(NULL, topic, partition_cnt, 1, 30000);

        memset(&ctx, 0, sizeof(ctx));
        mtx_init(&ctx.lock, mtx_plain);
        ctx.dr_remains = produced_msgcnt;

        rk = create_producer(&ctx);

        expected_produce_requests =
            topic_unique_leader_count(rk, topic, partition_cnt);
        TEST_ASSERT(expected_produce_requests < partition_cnt,
                    "topic %s has no broker leading multiple partitions: "
                    "leaders=%d partitions=%d",
                    topic, expected_produce_requests, partition_cnt);

        payload = malloc(PAYLOAD_SIZE);
        TEST_ASSERT(payload, "OOM allocating payload");
        memset(payload, 'R', PAYLOAD_SIZE);

        for (p = 0; p < 3; p++)
                rd_kafka_poll(rk, 50);
        stats_snapshot(&ctx, &start_total_requests, &start_produce_requests,
                       &start_stats_ts_us);

        TEST_SAY("Producing %d messages to %d partitions on %" PRIusz
                 " brokers: payload=%d batch.size=%d expected_requests=%d\n",
                 produced_msgcnt, partition_cnt, broker_cnt, PAYLOAD_SIZE,
                 BATCH_SIZE, expected_produce_requests);

        for (p = 0; p < partition_cnt; p++) {
                int i;

                for (i = 0; i < MSGS_PER_PARTITION; i++) {
                        rd_kafka_resp_err_t err;
                        int64_t deadline =
                            test_clock() + (int64_t)tmout_multip(30000) * 1000;

                        do {
                                err = rd_kafka_producev(
                                    rk, RD_KAFKA_V_TOPIC(topic),
                                    RD_KAFKA_V_PARTITION(p),
                                    RD_KAFKA_V_VALUE(payload, PAYLOAD_SIZE),
                                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                    RD_KAFKA_V_END);
                                if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                                        rd_kafka_poll(rk, 50);
                        } while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL &&
                                 test_clock() < deadline);

                        TEST_ASSERT(!err,
                                    "producev(%s [%d], msg %d) failed: %s",
                                    topic, p, i, rd_kafka_err2str(err));
                }
        }

        wait_delivery(rk, &ctx);
        produce_requests =
            wait_produce_request_count(rk, &ctx, start_produce_requests,
                                       start_stats_ts_us);
        request_count_failed = produce_requests != expected_produce_requests;

        TEST_SAY("ProduceRequests=%" PRId64 " expected=%d\n", produce_requests,
                 expected_produce_requests);

        free(payload);
        rd_kafka_destroy(rk);
        mtx_destroy(&ctx.lock);

        TEST_ASSERT(!request_count_failed,
                    "expected one ProduceRequest per leader broker (%d), got %"
                    PRId64,
                    expected_produce_requests, produce_requests);

        return 0;
}
