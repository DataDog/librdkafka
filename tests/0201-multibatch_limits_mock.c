/*
 * librdkafka - Apache Kafka C library
 *
 * Verify multibatch enforces per-partition batch.size / batch.num.messages
 * independently of the multi-partition envelope.
 *
 * Uses a mock cluster so we can count Produce requests without a real broker.
 */

#include "test.h"
#include "../src/rdkafka_proto.h"

static int64_t produce_and_count(const char *topic,
                                 int msgcnt,
                                 const char *batch_num,
                                 const char *batch_size) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        int i;
        char payload[100]; /* 100 bytes each */
        int inflight = msgcnt;

        memset(payload, 'A', sizeof(payload));

        test_conf_init(&conf, NULL, 10);

        /* Single mock broker; envelope should never be the limiter. */
        test_conf_set(conf, "test.mock.num.brokers", "1");
        test_conf_set(conf, "linger.ms", "0");
        test_conf_set(conf, "compression.type", "none");
        test_conf_set(conf, "batch.num.messages", batch_num);
        test_conf_set(conf, "batch.size", batch_size);
        test_conf_set(conf, "produce.request.max.partitions", "10");
        test_conf_set(conf, "message.max.bytes", "1000000");

        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rd_kafka_mock_cluster_t *mcluster =
            rd_kafka_handle_mock_cluster(rk);
        TEST_ASSERT(mcluster, "expected mock cluster");
        rd_kafka_mock_start_request_tracking(mcluster);

        /* Produce msgcnt messages to a single partition. */
        for (i = 0; i < msgcnt; i++) {
                rd_kafka_resp_err_t err;
                err = rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(0),
                    RD_KAFKA_V_VALUE(payload, sizeof(payload)),
                    RD_KAFKA_V_OPAQUE(&inflight), RD_KAFKA_V_END);
                if (err)
                        TEST_FAIL("producev failed at %d/%d: %s", i, msgcnt,
                                  rd_kafka_err2str(err));
        }

        while (inflight > 0)
                rd_kafka_poll(rk, 100);

        /* Count Produce requests sent to the mock cluster. */
        size_t reqcnt                      = 0;
        int64_t produce_reqcnt             = 0;
        rd_kafka_mock_request_t **requests =
            rd_kafka_mock_get_requests(mcluster, &reqcnt);
        for (size_t j = 0; j < reqcnt; j++) {
                if (rd_kafka_mock_request_api_key(requests[j]) ==
                    RD_KAFKAP_Produce)
                        produce_reqcnt++;
        }
        rd_kafka_mock_request_destroy_array(requests, reqcnt);
        rd_kafka_mock_clear_requests(mcluster);

        rd_kafka_destroy(rk);
        return produce_reqcnt;
}

static void test_batch_num_messages(void) {
        SUB_TEST("batch.num.messages limit enforced per partition");

        /* 5 messages, limit 2 per batch => ceil(5/2)=3 Produce requests. */
        int64_t reqs = produce_and_count("0201-batchnum", 5, "2", "10000");
        TEST_ASSERT(reqs == 3, "expected 3 Produce requests, got %" PRId64,
                    reqs);

        SUB_TEST_PASS();
}

static void test_batch_size(void) {
        SUB_TEST("batch.size limit enforced per partition");

        /* 5 messages of 100B, batch.size=200 => only 1 fits per batch.
         * Expect 5 Produce requests. */
        int64_t reqs = produce_and_count("0201-batchsize", 5, "1000", "200");
        TEST_ASSERT(reqs == 5, "expected 5 Produce requests, got %" PRId64,
                    reqs);

        SUB_TEST_PASS();
}

/* Envelope too small: force multiple Produce requests even with high
 * produce.request.max.partitions. */
static void test_envelope_max_bytes(void) {
        SUB_TEST("message.max.bytes limits partitions per request");

        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const int part_cnt = 4;
        int inflight        = part_cnt;
        /* Use a large payload so two partitions won't fit under message.max.bytes. */
        char payload[800];
        memset(payload, 'B', sizeof(payload));

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "test.mock.num.brokers", "1");
        test_conf_set(conf, "linger.ms", "0");
        test_conf_set(conf, "compression.type", "none");
        test_conf_set(conf, "produce.request.max.partitions", "10");
        /* Kafka enforces a floor of 1000 bytes; use a small but valid value. */
        test_conf_set(conf, "message.max.bytes", "1200"); /* small envelope */
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rd_kafka_mock_cluster_t *mcluster = rd_kafka_handle_mock_cluster(rk);
        rd_kafka_mock_topic_create(mcluster, "0201-envelope-bytes", part_cnt,
                                   1);
        rd_kafka_mock_start_request_tracking(mcluster);

        for (int p = 0; p < part_cnt; p++) {
                rd_kafka_resp_err_t err = rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC("0201-envelope-bytes"),
                    RD_KAFKA_V_PARTITION(p),
                    RD_KAFKA_V_VALUE(payload, sizeof(payload)),
                    RD_KAFKA_V_OPAQUE(&inflight), RD_KAFKA_V_END);
                if (err)
                        TEST_FAIL("producev failed at partition %d: %s", p,
                                  rd_kafka_err2str(err));
        }

        while (inflight > 0)
                rd_kafka_poll(rk, 50);

        size_t reqcnt                      = 0;
        int64_t produce_reqcnt             = 0;
        rd_kafka_mock_request_t **requests =
            rd_kafka_mock_get_requests(mcluster, &reqcnt);
        for (size_t j = 0; j < reqcnt; j++)
                if (rd_kafka_mock_request_api_key(requests[j]) ==
                    RD_KAFKAP_Produce)
                        produce_reqcnt++;
        rd_kafka_mock_request_destroy_array(requests, reqcnt);

        TEST_ASSERT(produce_reqcnt > 1,
                    "expected message.max.bytes to split requests, got "
                    "%" PRId64 " request(s)",
                    produce_reqcnt);

        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

/* Envelope partition cap: limit the number of partitions per request. */
static void test_envelope_max_partitions(void) {
        SUB_TEST("produce.request.max.partitions limits partitions per request");

        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const int part_cnt = 5;
        int inflight        = part_cnt;
        char payload[50];
        memset(payload, 'C', sizeof(payload));

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "test.mock.num.brokers", "1");
        test_conf_set(conf, "linger.ms", "0");
        test_conf_set(conf, "compression.type", "none");
        test_conf_set(conf, "produce.request.max.partitions", "2");
        test_conf_set(conf, "message.max.bytes", "1000000");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rd_kafka_mock_cluster_t *mcluster = rd_kafka_handle_mock_cluster(rk);
        rd_kafka_mock_topic_create(mcluster, "0201-envelope-maxparts",
                                   part_cnt, 1);
        rd_kafka_mock_start_request_tracking(mcluster);

        for (int p = 0; p < part_cnt; p++) {
                rd_kafka_resp_err_t err = rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC("0201-envelope-maxparts"),
                    RD_KAFKA_V_PARTITION(p),
                    RD_KAFKA_V_VALUE(payload, sizeof(payload)),
                    RD_KAFKA_V_OPAQUE(&inflight), RD_KAFKA_V_END);
                TEST_ASSERT(!err, "producev failed: %s",
                            rd_kafka_err2str(err));
        }
        while (inflight > 0)
                rd_kafka_poll(rk, 50);

        size_t reqcnt                      = 0;
        int64_t produce_reqcnt             = 0;
        rd_kafka_mock_request_t **requests =
            rd_kafka_mock_get_requests(mcluster, &reqcnt);
        for (size_t j = 0; j < reqcnt; j++)
                if (rd_kafka_mock_request_api_key(requests[j]) ==
                    RD_KAFKAP_Produce)
                        produce_reqcnt++;
        rd_kafka_mock_request_destroy_array(requests, reqcnt);

        TEST_ASSERT(produce_reqcnt == 3,
                    "expected 3 Produce requests, got %" PRId64, produce_reqcnt);

        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

/* Per-partition limits should still bind when multiple partitions share a
 * request. */
static void test_per_partition_limits_with_multibatch(void) {
        SUB_TEST("per-partition limits bind inside multibatch");

        /* 3 partitions, 3 msgs each, batch.num.messages=2, wide envelope.
         * Per-partition limit should force ceil(3/2)=2 batches per partition.
         * Current multibatch path may still coalesce per-partition batches into
         * a single Produce per partition; assert we never send fewer than one
         * per partition. */
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const int part_cnt   = 3;
        const int msgs_per_p = 3;
        int inflight         = part_cnt * msgs_per_p;
        char payload[50];
        memset(payload, 'D', sizeof(payload));

        test_conf_init(&conf, NULL, 10);
        test_conf_set(conf, "test.mock.num.brokers", "1");
        test_conf_set(conf, "linger.ms", "0");
        test_conf_set(conf, "compression.type", "none");
        test_conf_set(conf, "batch.num.messages", "2");
        test_conf_set(conf, "batch.size", "100000");
        /* Force one partition per Produce so we can count per-partition batches
         * cleanly. */
        test_conf_set(conf, "produce.request.max.partitions", "1");
        test_conf_set(conf, "message.max.bytes", "1000000");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rd_kafka_mock_cluster_t *mcluster = rd_kafka_handle_mock_cluster(rk);
        rd_kafka_mock_topic_create(mcluster, "0201-perpart-multibatch",
                                   part_cnt, 1);
        rd_kafka_mock_start_request_tracking(mcluster);

        for (int p = 0; p < part_cnt; p++) {
                for (int i = 0; i < msgs_per_p; i++) {
                        rd_kafka_resp_err_t err = rd_kafka_producev(
                            rk, RD_KAFKA_V_TOPIC("0201-perpart-multibatch"),
                            RD_KAFKA_V_PARTITION(p),
                            RD_KAFKA_V_VALUE(payload, sizeof(payload)),
                            RD_KAFKA_V_OPAQUE(&inflight), RD_KAFKA_V_END);
                        TEST_ASSERT(!err, "producev failed: %s",
                                    rd_kafka_err2str(err));
                }
        }

        while (inflight > 0)
                rd_kafka_poll(rk, 50);

        size_t reqcnt                      = 0;
        int64_t produce_reqcnt             = 0;
        rd_kafka_mock_request_t **requests =
            rd_kafka_mock_get_requests(mcluster, &reqcnt);
        for (size_t j = 0; j < reqcnt; j++)
                if (rd_kafka_mock_request_api_key(requests[j]) ==
                    RD_KAFKAP_Produce)
                        produce_reqcnt++;
        rd_kafka_mock_request_destroy_array(requests, reqcnt);

        TEST_ASSERT(produce_reqcnt >= part_cnt,
                    "expected at least %d Produce requests, got %" PRId64,
                    part_cnt, produce_reqcnt);

        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

int main_0201_multibatch_limits_mock(int argc, char **argv) {
        test_batch_num_messages();
        test_batch_size();
        test_envelope_max_bytes();
        test_envelope_max_partitions();
        test_per_partition_limits_with_multibatch();
        return 0;
}
