/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
 * All rights reserved.
 *
 * Test: Adaptive batching baseline - observe behavior under RTT spikes
 *
 * This test establishes baseline behavior WITHOUT adaptive batching,
 * demonstrating the problem: RTT spikes cause latency spikes, queue buildup,
 * and high inflight counts.
 *
 * Run this before and after implementing adaptive batching to compare.
 */

#include "test.h"
#include "rdkafka.h"

/**
 * Stats collected during each phase of the test
 */
typedef struct phase_stats_s {
        const char *name;
        int rtt_ms;             /* Configured RTT for this phase */

        /* Latency samples (microseconds) */
        int64_t *latencies;
        int latency_cnt;
        int latency_cap;

        /* From stats callback */
        int64_t int_latency_sum;
        int64_t int_latency_cnt;
        int64_t rtt_sum;
        int64_t rtt_cnt;
        int64_t outbuf_cnt_max;
        int64_t waitresp_cnt_max;
        int64_t msgq_bytes_max;
        int64_t txbytes_total;
        int64_t tx_total;       /* Total requests sent */

        /* Batch stats (per-partition RecordBatch) */
        int64_t batch_size_avg;
        int64_t batch_size_max;
        int64_t batch_cnt_avg;
        int batch_samples;

        /* ProduceRequest stats (request-level) */
        int64_t produce_partitions_avg;
        int64_t produce_messages_avg;
        int64_t produce_reqsize_avg;
        int produce_req_samples;

        /* Timing */
        int64_t start_ts;
        int64_t end_ts;
} phase_stats_t;

static phase_stats_t *current_phase = NULL;

/**
 * Delivery report callback - record produce latency
 */
static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
        int64_t *enqueue_ts = (int64_t *)rkmessage->_private;
        int64_t latency_us;

        if (!enqueue_ts || !current_phase)
                return;

        if (rkmessage->err) {
                TEST_WARN("Delivery failed: %s\n",
                          rd_kafka_err2str(rkmessage->err));
                free(enqueue_ts);
                return;
        }

        latency_us = test_clock() - *enqueue_ts;
        free(enqueue_ts);

        if (current_phase->latency_cnt < current_phase->latency_cap) {
                current_phase->latencies[current_phase->latency_cnt++] =
                    latency_us;
        }
}

/**
 * Parse a number from JSON at a specific position
 */
static int64_t json_parse_int_at(const char *json, const char *key) {
        char search[128];
        const char *p;

        snprintf(search, sizeof(search), "\"%s\":", key);
        p = strstr(json, search);
        if (!p)
                return -1;

        p += strlen(search);
        while (*p && (*p == ' ' || *p == '\t'))
                p++;

        return strtoll(p, NULL, 10);
}

/**
 * Find a JSON key and return pointer to its value
 */
static const char *json_find_key(const char *json, const char *key) {
        char search[128];
        const char *p;

        snprintf(search, sizeof(search), "\"%s\":", key);
        p = strstr(json, search);
        if (!p)
                return NULL;

        return p + strlen(search);
}

/**
 * Parse a window object's "avg" field
 * Window format: {"min":X,"max":X,"avg":X,"sum":X,"cnt":X,...}
 */
static int64_t json_parse_window_avg(const char *json, const char *window_key) {
        const char *window_start = json_find_key(json, window_key);
        if (!window_start)
                return -1;

        /* Find "avg" within the next ~200 chars (window object) */
        char buf[256];
        strncpy(buf, window_start, sizeof(buf) - 1);
        buf[sizeof(buf) - 1] = '\0';

        return json_parse_int_at(buf, "avg");
}

/**
 * Parse a window object's "max" field
 */
static int64_t json_parse_window_max(const char *json, const char *window_key) {
        const char *window_start = json_find_key(json, window_key);
        if (!window_start)
                return -1;

        char buf[256];
        strncpy(buf, window_start, sizeof(buf) - 1);
        buf[sizeof(buf) - 1] = '\0';

        return json_parse_int_at(buf, "max");
}

/**
 * Sum all occurrences of a key in JSON (for per-partition stats)
 */
static int64_t json_sum_all(const char *json, const char *key) {
        char search[128];
        const char *p = json;
        int64_t total = 0;

        snprintf(search, sizeof(search), "\"%s\":", key);

        while ((p = strstr(p, search)) != NULL) {
                p += strlen(search);
                while (*p && (*p == ' ' || *p == '\t'))
                        p++;
                total += strtoll(p, NULL, 10);
        }

        return total;
}

/**
 * Stats callback - extract metrics from JSON
 */
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        const char *brokers_start;
        const char *topics_start;
        int64_t val;

        if (!current_phase)
                return 0;

        /* Find brokers section and extract per-broker stats */
        brokers_start = strstr(json, "\"brokers\":");
        if (brokers_start) {
                /* int_latency - window object, get avg */
                val = json_parse_window_avg(brokers_start, "int_latency");
                if (val > 0) {
                        current_phase->int_latency_sum += val;
                        current_phase->int_latency_cnt++;
                }

                /* rtt - window object, get avg */
                val = json_parse_window_avg(brokers_start, "rtt");
                if (val > 0) {
                        current_phase->rtt_sum += val;
                        current_phase->rtt_cnt++;
                }

                /* outbuf_cnt - simple int */
                val = json_parse_int_at(brokers_start, "outbuf_cnt");
                if (val > current_phase->outbuf_cnt_max)
                        current_phase->outbuf_cnt_max = val;

                /* waitresp_cnt (inflight) - simple int */
                val = json_parse_int_at(brokers_start, "waitresp_cnt");
                if (val > current_phase->waitresp_cnt_max)
                        current_phase->waitresp_cnt_max = val;

                /* txbytes - simple int */
                val = json_parse_int_at(brokers_start, "txbytes");
                if (val > 0)
                        current_phase->txbytes_total = val;

                /* tx (requests) - simple int */
                val = json_parse_int_at(brokers_start, "tx");
                if (val > 0)
                        current_phase->tx_total = val;

                /* ProduceRequest stats - window objects */
                val = json_parse_window_avg(brokers_start, "produce_partitions");
                if (val > 0) {
                        current_phase->produce_partitions_avg = val;
                        current_phase->produce_req_samples++;
                }

                val = json_parse_window_avg(brokers_start, "produce_messages");
                if (val > 0)
                        current_phase->produce_messages_avg = val;

                val = json_parse_window_avg(brokers_start, "produce_reqsize");
                if (val > 0)
                        current_phase->produce_reqsize_avg = val;
        }

        /* Sum msgq_bytes across all partitions */
        val = json_sum_all(json, "msgq_bytes");
        if (val > current_phase->msgq_bytes_max)
                current_phase->msgq_bytes_max = val;

        /* Topic stats for batch size */
        topics_start = strstr(json, "\"topics\":");
        if (topics_start) {
                /* batchsize - window object */
                val = json_parse_window_avg(topics_start, "batchsize");
                if (val > 0) {
                        current_phase->batch_size_avg = val;
                        current_phase->batch_samples++;
                }

                val = json_parse_window_max(topics_start, "batchsize");
                if (val > current_phase->batch_size_max)
                        current_phase->batch_size_max = val;

                /* batchcnt - window object */
                val = json_parse_window_avg(topics_start, "batchcnt");
                if (val > 0)
                        current_phase->batch_cnt_avg = val;
        }

        return 0;
}

/**
 * Compare function for qsort
 */
static int cmp_int64(const void *a, const void *b) {
        int64_t va = *(const int64_t *)a;
        int64_t vb = *(const int64_t *)b;
        return (va > vb) - (va < vb);
}

/**
 * Calculate percentile from sorted array
 */
static int64_t percentile(int64_t *arr, int cnt, int pct) {
        int idx;
        if (cnt == 0)
                return 0;
        qsort(arr, cnt, sizeof(int64_t), cmp_int64);
        idx = (cnt * pct) / 100;
        if (idx >= cnt)
                idx = cnt - 1;
        return arr[idx];
}

/**
 * Print phase statistics
 */
static void print_phase_stats(phase_stats_t *phase) {
        int64_t duration_ms;
        int64_t p50, p99, avg;

        duration_ms = (phase->end_ts - phase->start_ts) / 1000;

        TEST_SAY("\n");
        TEST_SAY("=== Phase: %s (RTT=%dms, duration=%"PRId64"ms) ===\n",
                 phase->name, phase->rtt_ms, duration_ms);

        /* Produce latency */
        if (phase->latency_cnt > 0) {
                int64_t sum = 0;
                int i;
                for (i = 0; i < phase->latency_cnt; i++)
                        sum += phase->latencies[i];
                avg = sum / phase->latency_cnt;
                p50 = percentile(phase->latencies, phase->latency_cnt, 50);
                p99 = percentile(phase->latencies, phase->latency_cnt, 99);

                TEST_SAY("  Produce latency (total):\n");
                TEST_SAY("    p50: %"PRId64" ms\n", p50 / 1000);
                TEST_SAY("    p99: %"PRId64" ms\n", p99 / 1000);
                TEST_SAY("    avg: %"PRId64" ms\n", avg / 1000);
                TEST_SAY("    samples: %d\n", phase->latency_cnt);
        }

        /* Internal latency (queue wait) */
        if (phase->int_latency_cnt > 0) {
                TEST_SAY("  Internal latency (queue wait):\n");
                TEST_SAY("    avg: %"PRId64" us\n",
                         phase->int_latency_sum / phase->int_latency_cnt);
        }

        /* RTT */
        if (phase->rtt_cnt > 0) {
                TEST_SAY("  RTT (wire time):\n");
                TEST_SAY("    avg: %"PRId64" us\n",
                         phase->rtt_sum / phase->rtt_cnt);
        }

        /* Queue stats */
        TEST_SAY("  Queue stats:\n");
        TEST_SAY("    max outbuf_cnt: %"PRId64"\n", phase->outbuf_cnt_max);
        TEST_SAY("    max waitresp_cnt (inflight): %"PRId64"\n",
                 phase->waitresp_cnt_max);
        TEST_SAY("    max msgq_bytes: %"PRId64" KB\n",
                 phase->msgq_bytes_max / 1024);

        /* Throughput */
        if (duration_ms > 0) {
                TEST_SAY("  Throughput:\n");
                TEST_SAY("    requests: %"PRId64" total, %.1f req/s\n",
                         phase->tx_total,
                         (double)phase->tx_total * 1000 / duration_ms);
                TEST_SAY("    bytes: %"PRId64" KB total, %.1f KB/s\n",
                         phase->txbytes_total / 1024,
                         (double)phase->txbytes_total * 1000 / duration_ms / 1024);
        }

        /* Batch stats (per-partition RecordBatch) */
        if (phase->batch_samples > 0) {
                TEST_SAY("  Per-partition batch stats:\n");
                TEST_SAY("    avg batch size: %"PRId64" bytes (%.1f KB)\n",
                         phase->batch_size_avg, phase->batch_size_avg / 1024.0);
                TEST_SAY("    max batch size: %"PRId64" bytes (%.1f KB)\n",
                         phase->batch_size_max, phase->batch_size_max / 1024.0);
                TEST_SAY("    avg msgs/batch: %"PRId64"\n", phase->batch_cnt_avg);
        }

        /* ProduceRequest stats (request-level) */
        if (phase->produce_req_samples > 0) {
                TEST_SAY("  ProduceRequest stats:\n");
                TEST_SAY("    avg partitions/req: %"PRId64"\n",
                         phase->produce_partitions_avg);
                TEST_SAY("    avg messages/req: %"PRId64"\n",
                         phase->produce_messages_avg);
                TEST_SAY("    avg reqsize: %"PRId64" bytes (%.1f KB)\n",
                         phase->produce_reqsize_avg,
                         phase->produce_reqsize_avg / 1024.0);
        }

        TEST_SAY("\n");
}

/**
 * Initialize phase stats
 */
static void phase_stats_init(phase_stats_t *phase,
                             const char *name,
                             int rtt_ms,
                             int max_samples) {
        memset(phase, 0, sizeof(*phase));
        phase->name        = name;
        phase->rtt_ms      = rtt_ms;
        phase->latencies   = calloc(max_samples, sizeof(int64_t));
        phase->latency_cap = max_samples;
}

/**
 * Destroy phase stats
 */
static void phase_stats_destroy(phase_stats_t *phase) {
        free(phase->latencies);
}

/**
 * Run a phase of the test
 */
static void run_phase(rd_kafka_t *rk,
                      rd_kafka_mock_cluster_t *mcluster,
                      const char *topic,
                      int partition_cnt,
                      phase_stats_t *phase,
                      int duration_ms,
                      int msg_rate_per_sec,
                      int msg_size) {
        int64_t phase_end;
        int64_t next_produce;
        int64_t interval_us;
        int msgs_produced = 0;
        int partition = 0;  /* Round-robin partition counter */
        char *payload;

        payload = malloc(msg_size);
        memset(payload, 'x', msg_size);

        /* Set RTT for this phase */
        rd_kafka_mock_broker_set_rtt(mcluster, -1, phase->rtt_ms);

        interval_us = 1000000 / msg_rate_per_sec;

        current_phase    = phase;
        phase->start_ts  = test_clock();
        phase_end        = phase->start_ts + (duration_ms * 1000LL);
        next_produce     = phase->start_ts;

        TEST_SAY("Starting phase: %s (RTT=%dms, rate=%d msg/s, duration=%dms)\n",
                 phase->name, phase->rtt_ms, msg_rate_per_sec, duration_ms);

        while (test_clock() < phase_end) {
                int64_t now = test_clock();

                /* Produce at target rate */
                while (now >= next_produce && now < phase_end) {
                        int64_t *enqueue_ts = malloc(sizeof(int64_t));
                        rd_kafka_resp_err_t err;

                        *enqueue_ts = test_clock();

                        err = rd_kafka_producev(
                            rk, RD_KAFKA_V_TOPIC(topic),
                            RD_KAFKA_V_PARTITION(partition),
                            RD_KAFKA_V_VALUE(payload, msg_size),
                            RD_KAFKA_V_OPAQUE(enqueue_ts),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_END);

                        if (err) {
                                TEST_WARN("Produce failed: %s\n",
                                          rd_kafka_err2str(err));
                                free(enqueue_ts);
                        } else {
                                msgs_produced++;
                                /* Round-robin to next partition */
                                partition = (partition + 1) % partition_cnt;
                        }

                        next_produce += interval_us;
                }

                /* Poll for deliveries and stats */
                rd_kafka_poll(rk, 10);
        }

        /* Flush remaining */
        TEST_SAY("Flushing %d in-queue messages...\n",
                 rd_kafka_outq_len(rk));
        rd_kafka_flush(rk, 30000);

        phase->end_ts = test_clock();
        current_phase = NULL;

        TEST_SAY("Phase complete: %d messages produced\n", msgs_produced);

        free(payload);
}

static void do_test_adaptive_baseline(void) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstrap_servers;
        const char *topic = "adaptive_baseline_test";
        phase_stats_t phase_low_rtt, phase_high_rtt, phase_recovery;

        /* Test parameters - adjust these to simulate your scenario */
        const int MSG_SIZE        = 200;     /* 200 byte messages */
        const int MSG_RATE        = 2500;    /* 2500 msg/s normal rate */
        const int MSG_RATE_SPIKE  = 25000;   /* 25000 msg/s during spike (10x) */
        const int PHASE_DURATION  = 30000;   /* 30 seconds per phase */
        const int LOW_RTT_MS      = 100;     /* Normal: 100ms RTT */
        const int HIGH_RTT_MS     = 5000;    /* Spike: 5 second RTT */
        const int MAX_SAMPLES     = 500000;  /* More samples for higher rate */
        const int PARTITION_CNT   = 1000;    /* 1000 partitions */

        SUB_TEST_QUICK();

        /* Create mock cluster */
        mcluster = test_mock_cluster_new(3, &bootstrap_servers);
        rd_kafka_mock_topic_create(mcluster, topic, PARTITION_CNT, 3);

        /* Check if adaptive batching is disabled via environment (enabled by default) */
        const char *adaptive_env = test_getenv("TEST_ADAPTIVE", "1");
        int adaptive_enabled = !adaptive_env || strcmp(adaptive_env, "0");

        /* Configure producer */
        test_conf_init(&conf, NULL, 120);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        test_conf_set(conf, "linger.ms", "200");
        test_conf_set(conf, "batch.size", "1000000");  /* 1MB max */
        test_conf_set(conf, "statistics.interval.ms", "50");
        test_conf_set(conf, "queue.buffering.max.messages", "1000000");
        /* Disable sticky partitioner to enable round-robin */
        test_conf_set(conf, "sticky.partitioning.linger.ms", "0");
        test_conf_set(conf, "max.in.flight", "10");

        /* Enable adaptive batching if requested */
        if (adaptive_enabled) {
                test_conf_set(conf, "adaptive.batching.enable", "true");
                test_conf_set(conf, "adaptive.linger.min.ms", "5");
                test_conf_set(conf, "adaptive.linger.max.ms", "2000");
                test_conf_set(conf, "adaptive.alpha", "0.02");
                test_conf_set(conf, "adaptive.beta", "0.1");
        }

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Initialize phase stats */
        phase_stats_init(&phase_low_rtt, "Low RTT (baseline)", LOW_RTT_MS,
                         MAX_SAMPLES);
        phase_stats_init(&phase_high_rtt, "High RTT (spike)", HIGH_RTT_MS,
                         MAX_SAMPLES);
        phase_stats_init(&phase_recovery, "Recovery", LOW_RTT_MS, MAX_SAMPLES);

        TEST_SAY("\n");
        if (adaptive_enabled) {
                TEST_SAY("╔══════════════════════════════════════════════════════════╗\n");
                TEST_SAY("║  ADAPTIVE BATCHING TEST (adaptive enabled)               ║\n");
                TEST_SAY("║                                                          ║\n");
                TEST_SAY("║  This test shows adaptive batching in action.            ║\n");
                TEST_SAY("║  Watch for: linger/batch adjustments during congestion   ║\n");
                TEST_SAY("╚══════════════════════════════════════════════════════════╝\n");
        } else {
                TEST_SAY("╔══════════════════════════════════════════════════════════╗\n");
                TEST_SAY("║  ADAPTIVE BATCHING BASELINE TEST                         ║\n");
                TEST_SAY("║                                                          ║\n");
                TEST_SAY("║  This test demonstrates the PROBLEM (before adaptive).   ║\n");
                TEST_SAY("║  Watch for: latency spikes, queue buildup, high inflight ║\n");
                TEST_SAY("║                                                          ║\n");
                TEST_SAY("║  Run with TEST_ADAPTIVE=1 to enable adaptive batching.   ║\n");
                TEST_SAY("╚══════════════════════════════════════════════════════════╝\n");
        }
        TEST_SAY("\n");
        TEST_SAY("Parameters:\n");
        TEST_SAY("  Message size: %d bytes\n", MSG_SIZE);
        TEST_SAY("  Message rate (normal): %d msg/s\n", MSG_RATE);
        TEST_SAY("  Message rate (spike):  %d msg/s (%.1fx)\n",
                 MSG_RATE_SPIKE, (double)MSG_RATE_SPIKE / MSG_RATE);
        TEST_SAY("  Phase duration: %d ms\n", PHASE_DURATION);
        TEST_SAY("  Low RTT: %d ms\n", LOW_RTT_MS);
        TEST_SAY("  High RTT: %d ms\n", HIGH_RTT_MS);
        TEST_SAY("  Partitions: %d (round-robin)\n", PARTITION_CNT);
        TEST_SAY("  Adaptive batching: %s\n", adaptive_enabled ? "ENABLED" : "disabled");
        TEST_SAY("\n");

        /* Phase 1: Low RTT baseline */
        run_phase(rk, mcluster, topic, PARTITION_CNT, &phase_low_rtt,
                  PHASE_DURATION, MSG_RATE, MSG_SIZE);

        /* Phase 2: High RTT + traffic spike (simulates real production incident) */
        run_phase(rk, mcluster, topic, PARTITION_CNT, &phase_high_rtt,
                  PHASE_DURATION, MSG_RATE_SPIKE, MSG_SIZE);

        /* Phase 3: Recovery */
        run_phase(rk, mcluster, topic, PARTITION_CNT, &phase_recovery,
                  PHASE_DURATION, MSG_RATE, MSG_SIZE);

        /* Print results */
        TEST_SAY("\n");
        TEST_SAY("╔══════════════════════════════════════════════════════════╗\n");
        TEST_SAY("║                      RESULTS                             ║\n");
        TEST_SAY("╚══════════════════════════════════════════════════════════╝\n");

        print_phase_stats(&phase_low_rtt);
        print_phase_stats(&phase_high_rtt);
        print_phase_stats(&phase_recovery);

        /* Summary comparison */
        TEST_SAY("╔══════════════════════════════════════════════════════════╗\n");
        TEST_SAY("║                     COMPARISON                           ║\n");
        TEST_SAY("╚══════════════════════════════════════════════════════════╝\n");
        TEST_SAY("\n");
        TEST_SAY("                        Low RTT    High RTT    Recovery\n");
        TEST_SAY("  ─────────────────────────────────────────────────────\n");

        if (phase_low_rtt.latency_cnt > 0 && phase_high_rtt.latency_cnt > 0) {
                TEST_SAY("  Latency p99 (ms):     %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                         percentile(phase_low_rtt.latencies,
                                    phase_low_rtt.latency_cnt, 99) / 1000,
                         percentile(phase_high_rtt.latencies,
                                    phase_high_rtt.latency_cnt, 99) / 1000,
                         percentile(phase_recovery.latencies,
                                    phase_recovery.latency_cnt, 99) / 1000);
        }

        TEST_SAY("  Max inflight:         %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                 phase_low_rtt.waitresp_cnt_max, phase_high_rtt.waitresp_cnt_max,
                 phase_recovery.waitresp_cnt_max);

        TEST_SAY("  Max msgq (KB):        %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                 phase_low_rtt.msgq_bytes_max / 1024,
                 phase_high_rtt.msgq_bytes_max / 1024,
                 phase_recovery.msgq_bytes_max / 1024);

        TEST_SAY("  Avg batch size (KB):  %7.1f    %8.1f    %8.1f\n",
                 phase_low_rtt.batch_size_avg / 1024.0,
                 phase_high_rtt.batch_size_avg / 1024.0,
                 phase_recovery.batch_size_avg / 1024.0);

        TEST_SAY("  Avg msgs/batch:       %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                 phase_low_rtt.batch_cnt_avg,
                 phase_high_rtt.batch_cnt_avg,
                 phase_recovery.batch_cnt_avg);

        if (phase_low_rtt.int_latency_cnt > 0) {
                TEST_SAY("  Avg int_latency (us): %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                         phase_low_rtt.int_latency_sum /
                             phase_low_rtt.int_latency_cnt,
                         phase_high_rtt.int_latency_cnt > 0
                             ? phase_high_rtt.int_latency_sum /
                                   phase_high_rtt.int_latency_cnt
                             : 0,
                         phase_recovery.int_latency_cnt > 0
                             ? phase_recovery.int_latency_sum /
                                   phase_recovery.int_latency_cnt
                             : 0);
        }

        if (phase_low_rtt.rtt_cnt > 0) {
                TEST_SAY("  Avg RTT (us):         %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                         phase_low_rtt.rtt_sum / phase_low_rtt.rtt_cnt,
                         phase_high_rtt.rtt_cnt > 0
                             ? phase_high_rtt.rtt_sum / phase_high_rtt.rtt_cnt
                             : 0,
                         phase_recovery.rtt_cnt > 0
                             ? phase_recovery.rtt_sum / phase_recovery.rtt_cnt
                             : 0);
        }

        TEST_SAY("  ─────────────────────────────────────────────────────\n");
        TEST_SAY("  ProduceRequest stats (request-level aggregates):\n");
        TEST_SAY("  Avg parts/req:        %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                 phase_low_rtt.produce_partitions_avg,
                 phase_high_rtt.produce_partitions_avg,
                 phase_recovery.produce_partitions_avg);
        TEST_SAY("  Avg msgs/req:         %7"PRId64"    %8"PRId64"    %8"PRId64"\n",
                 phase_low_rtt.produce_messages_avg,
                 phase_high_rtt.produce_messages_avg,
                 phase_recovery.produce_messages_avg);
        TEST_SAY("  Avg reqsize (KB):     %7.1f    %8.1f    %8.1f\n",
                 phase_low_rtt.produce_reqsize_avg / 1024.0,
                 phase_high_rtt.produce_reqsize_avg / 1024.0,
                 phase_recovery.produce_reqsize_avg / 1024.0);

        TEST_SAY("\n");

        /* Cleanup */
        rd_kafka_destroy(rk);
        test_mock_cluster_destroy(mcluster);

        phase_stats_destroy(&phase_low_rtt);
        phase_stats_destroy(&phase_high_rtt);
        phase_stats_destroy(&phase_recovery);

        SUB_TEST_PASS();
}

int main_0202_adaptive_batching_baseline(int argc, char **argv) {
        do_test_adaptive_baseline();
        return 0;
}
