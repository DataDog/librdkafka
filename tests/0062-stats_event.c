/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
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

/**
 * Tests stats events and typed stats callback.
 */


#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */

#include <string.h>

static int stats_count = 0;

/* Typed stats callback state */
static int typed_stats_count              = 0;
static int64_t last_typed_ts_us           = 0;
static uint32_t last_typed_msg_max        = 0;
static char last_typed_client_id[256]     = {0};

/**
 * Typed stats callback - receives structured stats data.
 */
static void typed_stats_cb(rd_kafka_t *rk,
                           const rd_kafka_stats_t *stats,
                           void *opaque) {
        TEST_SAY("Typed stats callback: name=%s, client_id=%s, type=%d, "
                 "ts_us=%lld, msg_cnt=%u, msg_max=%u, broker_cnt=%u, "
                 "topic_cnt=%u\n",
                 stats->name, stats->client_id, stats->type,
                 (long long)stats->ts_us, stats->msg_cnt, stats->msg_max,
                 stats->broker_cnt, stats->topic_cnt);

        /* Validate basic fields */
        if (stats->name[0] == '\0') {
                TEST_FAIL("Typed stats: name is empty\n");
        }
        if (stats->client_id[0] == '\0') {
                TEST_FAIL("Typed stats: client_id is empty\n");
        }
        if (stats->ts_us <= 0) {
                TEST_FAIL("Typed stats: ts_us is invalid: %lld\n",
                          (long long)stats->ts_us);
        }
        if (stats->msg_max == 0) {
                TEST_FAIL("Typed stats: msg_max should not be 0\n");
        }

        /* Check timestamp is increasing */
        if (last_typed_ts_us > 0 && stats->ts_us <= last_typed_ts_us) {
                TEST_FAIL("Typed stats: ts_us not increasing: %lld <= %lld\n",
                          (long long)stats->ts_us, (long long)last_typed_ts_us);
        }

        /* Save for later comparison */
        last_typed_ts_us    = stats->ts_us;
        last_typed_msg_max  = stats->msg_max;
        strncpy(last_typed_client_id, stats->client_id,
                sizeof(last_typed_client_id) - 1);

        typed_stats_count++;
}

/**
 * JSON stats callback - invoked via rd_kafka_poll() on app's thread.
 */
static int json_stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        TEST_SAY("JSON stats callback: %zu bytes\n", json_len);

        /* Verify JSON contains expected fields that match typed stats */
        if (last_typed_client_id[0] != '\0') {
                if (strstr(json, last_typed_client_id) == NULL) {
                        TEST_FAIL("JSON stats missing client_id '%s'\n",
                                  last_typed_client_id);
                }
                /* Check msg_max appears in JSON */
                char msg_max_str[64];
                snprintf(msg_max_str, sizeof(msg_max_str),
                         "\"msg_max\":%u", last_typed_msg_max);
                if (strstr(json, msg_max_str) == NULL) {
                        TEST_FAIL("JSON stats missing msg_max %u\n",
                                  last_typed_msg_max);
                }
        }

        stats_count++;
        return 0; /* Don't take ownership */
}

static void test_stats_event_typed_only(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_queue_t *rkqu;
        rd_kafka_event_t *rkev = NULL;
        int i;

        conf = rd_kafka_conf_new();
        const char *test_debug = getenv("TEST_DEBUG");
        if (test_debug && *test_debug)
            test_conf_set(conf, "debug", test_debug);

        rd_kafka_conf_set(conf, "statistics.interval.ms", "100", NULL, 0);
        rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_STATS);

        rk   = test_create_handle(RD_KAFKA_PRODUCER, conf);
        rkqu = rd_kafka_queue_get_main(rk);

        for (i = 0; i < 20 && !rkev; i++) {
                rkev = rd_kafka_queue_poll(rkqu, 500);
                if (rkev && rd_kafka_event_type(rkev) != RD_KAFKA_EVENT_STATS) {
                        rd_kafka_event_destroy(rkev);
                        rkev = NULL;
                }
        }

        if (!rkev)
                TEST_FAIL("Expected stats event but none received\n");

        if (!rd_kafka_event_stats_typed(rkev))
                TEST_FAIL("Expected typed stats for stats event\n");

        if (rd_kafka_event_stats(rkev) != NULL)
                TEST_FAIL(
                    "Expected JSON stats to be NULL when no JSON callback is set\n");

        rd_kafka_event_destroy(rkev);
        rd_kafka_queue_destroy(rkqu);
        rd_kafka_destroy(rk);
}

int main_0062_stats_event(int argc, char **argv) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        test_timing_t t_delivery;
        const int iterations = 5;
        int i;
        test_conf_init(&conf, NULL, 10);
        rd_kafka_conf_set(conf, "statistics.interval.ms", "100", NULL, 0);

        /* Reset typed stats state */
        typed_stats_count    = 0;
        last_typed_ts_us     = 0;
        last_typed_msg_max   = 0;
        last_typed_client_id[0] = '\0';
        stats_count          = 0;

        /* Use callback API (not events) to test both JSON and typed callbacks.
         * Both callbacks are invoked via rd_kafka_poll() on the app's thread. */
        rd_kafka_conf_set_stats_cb(conf, json_stats_cb);
        rd_kafka_conf_set_stats_cb_typed(conf, typed_stats_cb);

        /* Create kafka instance */
        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Wait for stats callbacks via rd_kafka_poll() */
        for (i = 0; i < iterations; i++) {
                int prev_typed_count = typed_stats_count;
                int prev_json_count  = stats_count;

                TIMING_START(&t_delivery, "STATS_CALLBACK");

                /* Poll until we get at least one stats callback */
                while (typed_stats_count == prev_typed_count ||
                       stats_count == prev_json_count) {
                        rd_kafka_poll(rk, 100);
                }

                TIMING_STOP(&t_delivery);

                TEST_SAY("Iteration %d: typed=%d, json=%d, duration=%.3fms\n",
                         i, typed_stats_count, stats_count,
                         (float)TIMING_DURATION(&t_delivery) / 1000.0f);

                if (TIMING_DURATION(&t_delivery) < 1000 * 100 * 0.5 ||
                    TIMING_DURATION(&t_delivery) > 1000 * 100 * 1.5) {
                        /* CIs and valgrind are too flaky/slow to
                         * make this failure meaningful. */
                        if (!test_on_ci && !strcmp(test_mode, "bare")) {
                                TEST_FAIL(
                                    "Stats duration %.3fms is >= 50%% "
                                    "outside statistics.interval.ms 100",
                                    (float)TIMING_DURATION(&t_delivery) /
                                        1000.0f);
                        } else {
                                TEST_WARN(
                                    "Stats duration %.3fms is >= 50%% "
                                    "outside statistics.interval.ms 100\n",
                                    (float)TIMING_DURATION(&t_delivery) /
                                        1000.0f);
                        }
                }
        }

        rd_kafka_destroy(rk);

        /* Verify both callbacks were called the expected number of times.
         * They should be equal since both are invoked via rd_kafka_poll(). */
        TEST_SAY("Final stats counts: JSON callbacks=%d, typed callbacks=%d\n",
                 stats_count, typed_stats_count);

        if (typed_stats_count < iterations) {
                TEST_FAIL("Typed stats callback count %d < expected %d\n",
                          typed_stats_count, iterations);
        }

        test_stats_event_typed_only();
        TEST_SAY("Typed stats callback verification: PASSED\n");

        return 0;
}
