/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"
#include "../src/rdkafka_mock.h"
#include "../src/rdkafka_proto.h"

typedef struct produce_match_ctx_s {
        int32_t broker_id;
} produce_match_ctx_t;

static rd_bool_t
is_produce_request_for_broker(rd_kafka_mock_request_t *request, void *opaque) {
        const produce_match_ctx_t *ctx = opaque;

        return rd_kafka_mock_request_api_key(request) == RD_KAFKAP_Produce &&
               rd_kafka_mock_request_id(request) == ctx->broker_id;
}

static size_t
get_matching_request_cnt(rd_kafka_mock_cluster_t *mcluster,
                         rd_bool_t (*match)(rd_kafka_mock_request_t *,
                                            void *opaque),
                         void *opaque) {
        rd_kafka_mock_request_t **requests;
        size_t request_cnt;
        size_t matching = 0;
        size_t i;

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                if (match(requests[i], opaque))
                        matching++;
        }
        rd_kafka_mock_request_destroy_array(requests, request_cnt);

        return matching;
}

static size_t
wait_matching_requests(rd_kafka_mock_cluster_t *mcluster,
                       size_t expected_cnt,
                       int timeout_ms,
                       rd_bool_t (*match)(rd_kafka_mock_request_t *,
                                          void *opaque),
                       void *opaque) {
        int64_t abs_timeout = test_clock() + ((int64_t)timeout_ms * 1000);
        size_t matching = 0;

        while (test_clock() < abs_timeout) {
                matching = get_matching_request_cnt(mcluster, match, opaque);
                if (matching >= expected_cnt)
                        break;
                rd_usleep(100 * 1000, 0);
        }

        TEST_ASSERT_LATER(matching >= expected_cnt,
                          "Timed out waiting for %" PRIusz
                          " Produce request(s), got %" PRIusz,
                          expected_cnt, matching);

        return matching;
}

int main_0203_producer_reconnect_mock(int argc, char **argv) {
        rd_kafka_mock_cluster_t *mcluster = NULL;
        rd_kafka_conf_t *conf = NULL;
        rd_kafka_t *rk = NULL;
        rd_kafka_topic_t *rkt = NULL;
        const char *bootstraps;
        const char *topic = test_mk_topic_name(__FUNCTION__, 1);
        const int broker_cnt = 6;
        const int32_t broker_id = 3;
        produce_match_ctx_t match = {.broker_id = broker_id};
        const int msgs_per_sec     = 50;
        const int down_after_ms    = 150;
        const int down_duration_ms = 10000;
        const int post_up_ms       = 200;
        int total_run_ms;
        int msg_timeout_ms;
        const uint64_t testid     = test_id_generate();
        int msgcounter            = 0;
        char payload[128];
        char key[128];
        char msg_timeout[32];
        int msg_i;
        int64_t start_ts;
        int64_t down_ts;
        int64_t up_ts;
        int64_t end_ts;
        int64_t next_send_ts;
        int64_t send_interval_us;
        rd_bool_t broker_down = rd_false;
        rd_bool_t broker_up   = rd_false;
        rd_bool_t tracking = rd_false;

        TEST_SKIP_MOCK_CLUSTER(0);
        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(broker_cnt, &bootstraps);
        if (rd_kafka_mock_topic_create(mcluster, topic, 1, 1) != 0) {
                TEST_FAIL_LATER("Failed to create topic %s", topic);
                goto done;
        }
        if (rd_kafka_mock_partition_set_leader(mcluster, topic, 0,
                                               broker_id) != 0) {
                TEST_FAIL_LATER("Failed to set leader for %s [0] to broker %"
                                PRId32,
                                topic, broker_id);
                goto done;
        }
        rd_kafka_mock_start_request_tracking(mcluster);
        tracking = rd_true;

        total_run_ms = down_after_ms + down_duration_ms + post_up_ms;
        msg_timeout_ms = total_run_ms + 5000;
        send_interval_us = 1000000 / msgs_per_sec;
        if (send_interval_us < 1)
                send_interval_us = 1;
        rd_snprintf(msg_timeout, sizeof(msg_timeout), "%d", msg_timeout_ms);

        test_conf_init(&conf, NULL, 0);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.sparse.connections", "true");
        test_conf_set(conf, "linger.ms", "0");
        test_conf_set(conf, "batch.num.messages", "1");
        test_conf_set(conf, "reconnect.backoff.ms", "100");
        test_conf_set(conf, "reconnect.backoff.max.ms", "500");
        test_conf_set(conf, "message.timeout.ms", msg_timeout);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        test_curr->is_fatal_cb = test_error_is_not_fatal_cb;
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkt = test_create_producer_topic(rk, topic, NULL);
        test_wait_topic_exists(rk, topic, 5000);

        /* Produce messages with a time-based outage mid-stream. */
        start_ts = test_clock();
        down_ts      = start_ts + ((int64_t)down_after_ms * 1000);
        up_ts        = down_ts + ((int64_t)down_duration_ms * 1000);
        end_ts       = start_ts + ((int64_t)total_run_ms * 1000);
        next_send_ts = start_ts;

        for (msg_i = 0; test_clock() < end_ts; ) {
                int64_t now = test_clock();

                if (!broker_down && now >= down_ts) {
                        if (rd_kafka_mock_broker_set_down(mcluster,
                                                         broker_id) != 0) {
                                TEST_FAIL_LATER(
                                    "Failed to set broker %" PRId32 " down",
                                    broker_id);
                                goto done;
                        }
                        rd_kafka_mock_clear_requests(mcluster);
                        broker_down = rd_true;
                } else if (broker_down && !broker_up && now >= up_ts) {
                        if (rd_kafka_mock_broker_set_up(mcluster,
                                                       broker_id) != 0) {
                                TEST_FAIL_LATER(
                                    "Failed to set broker %" PRId32 " up",
                                    broker_id);
                                goto done;
                        }
                        broker_up = rd_true;
                }

                if (now < next_send_ts) {
                        rd_usleep((int)((next_send_ts - now) / 1000), 0);
                        continue;
                }

                test_prepare_msg(testid, 0, msg_i, payload, sizeof(payload),
                                 key, sizeof(key));
                if (rd_kafka_produce(rkt, 0, RD_KAFKA_MSG_F_COPY, payload,
                                     sizeof(payload), key, strlen(key),
                                     &msgcounter) == -1)
                        TEST_FAIL("Failed to produce message %d: %s", msg_i,
                                  rd_kafka_err2str(rd_kafka_last_error()));

                msgcounter++;
                rd_kafka_poll(rk, 0);
                msg_i++;
                next_send_ts = now + send_interval_us;
        }

        /* Expect Produce requests to resume after reconnect. */
        TEST_ASSERT_LATER(broker_down, "Broker was never set down");
        if (broker_down && !broker_up) {
                if (rd_kafka_mock_broker_set_up(mcluster, broker_id) != 0) {
                        TEST_FAIL_LATER(
                            "Failed to set broker %" PRId32 " up", broker_id);
                        goto done;
                }
                broker_up = rd_true;
        }
        wait_matching_requests(mcluster, 1, 10000,
                               is_produce_request_for_broker, &match);

        TEST_SAY("waiting for %d messages to be delivered\n", msgcounter);
        test_wait_delivery(rk, &msgcounter);

done:
        test_curr->is_fatal_cb = NULL;
        test_curr->ignore_dr_err = rd_false;
        if (rkt)
                rd_kafka_topic_destroy(rkt);
        if (rk)
                rd_kafka_destroy(rk);
        if (conf && !rk)
                rd_kafka_conf_destroy(conf);
        if (tracking && mcluster)
                rd_kafka_mock_stop_request_tracking(mcluster);
        if (mcluster)
                test_mock_cluster_destroy(mcluster);

        TEST_LATER_CHECK();
        SUB_TEST_PASS();
        return 0;
}
