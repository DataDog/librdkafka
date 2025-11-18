/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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
 * MultiBatch Performance Benchmark
 *
 * Supports two benchmark modes:
 * 1. MAX THROUGHPUT: Send messages as fast as possible
 * 2. CONTROLLED RATE: Send at specified rate (msgs/sec)
 *
 * Measures:
 * - Throughput (messages/sec, MB/sec)
 * - Latency distribution (P50, P95, P99)
 * - ProduceRequest count (efficiency)
 * - Per-message latency sampling
 *
 * Usage:
 *   ./test-runner 0200
 *
 * Configuration via environment variables:
 *   MAX_PARTITIONS                Number of partitions (default: 1000)
 *   BENCH_MAX_THROUGHPUT_MESSAGES Messages for max throughput test (default: 100000)
 *   BENCH_CONTROLLED_RATES        Comma-separated rates, e.g. "10000,20000,50000" (default: 10000,20000,50000)
 *   BENCH_CONTROLLED_DURATION     Seconds per controlled rate test (default: 60)
 *   BENCH_MESSAGE_SIZE            Bytes per message (default: 256)
 *   BENCH_OUTPUT_DIR              Output directory (default: ./benchmark_output)
 *   BENCH_SKIP_MAX_THROUGHPUT     Set to "1" to skip max throughput phase
 *   BENCH_SKIP_CONTROLLED         Set to "1" to skip controlled rate phases
 *
 * Example:
 *   export MAX_PARTITIONS=1000
 *   export BENCH_MAX_THROUGHPUT_MESSAGES=100000
 *   export BENCH_CONTROLLED_RATES="10000,20000"
 *   export BENCH_CONTROLLED_DURATION=30
 *   ./test-runner 0200
 */

#include "test.h"
#include "rdkafka.h"
#include "../src/cJSON.h"
#include <math.h>
#include <sys/stat.h>
#include <time.h>

/* Benchmark mode */
typedef enum {
        BENCH_MODE_MAX_THROUGHPUT,
        BENCH_MODE_CONTROLLED_RATE
} bench_mode_t;

/* Benchmark configuration */
typedef struct {
        /* Common config */
        int partition_cnt;
        int message_size;
        char *output_dir;

        /* Max throughput mode */
        int max_throughput_messages;
        int skip_max_throughput;

        /* Controlled rate mode */
        int *controlled_rates;      /* Array of rates (msgs/sec) */
        int controlled_rate_cnt;
        int controlled_duration_sec;
        int skip_controlled;
} bench_config_t;

/* Per-message latency tracking */
typedef struct {
        int64_t enqueue_ts;
        int64_t delivery_ts;
        int partition;
        int msg_seq;
} msg_latency_t;

/* Per-phase statistics */
typedef struct {
        bench_mode_t mode;
        int rate_msgs_sec;              /* For controlled rate mode, 0 for max throughput */
        double duration_sec;
        int messages_sent;
        int messages_failed;
        double throughput_msgs_sec;
        double throughput_mb_sec;
        int64_t request_count;

        /* Latency statistics */
        double latency_p50_ms;
        double latency_p95_ms;
        double latency_p99_ms;

        /* Broker latency statistics (microseconds) */
        double int_latency_p50;
        double int_latency_p95;
        double int_latency_p99;
        double rtt_p50;
        double rtt_p95;
        double rtt_p99;
} phase_stats_t;

/* Benchmark context */
typedef struct {
        bench_config_t *config;

        /* Current phase being run */
        bench_mode_t current_mode;
        int current_rate;
        int64_t phase_start_ts;

        /* Message latencies (all messages) */
        msg_latency_t *msg_latencies;
        int msg_lat_cnt;
        int msg_lat_size;

        /* Sampled message latencies (every 100th) */
        msg_latency_t *sampled_latencies;
        int sampled_lat_cnt;
        int sampled_lat_size;

        /* Statistics from stats callback */
        int64_t last_produce_requests;
        double last_int_latency_p50;
        double last_int_latency_p95;
        double last_int_latency_p99;
        double last_rtt_p50;
        double last_rtt_p95;
        double last_rtt_p99;

        /* Results */
        phase_stats_t *phases;
        int phase_cnt;
        int phase_size;

        mtx_t lock;
} bench_ctx_t;

static bench_ctx_t *bench_ctx = NULL;

/**
 * Helper to extract double from JSON object
 */
static double get_json_double(cJSON *obj, const char *key, double default_val) {
        cJSON *item = cJSON_GetObjectItem(obj, key);
        return (item && cJSON_IsNumber(item)) ? cJSON_GetNumberValue(item) : default_val;
}

/**
 * Stats callback - extract statistics using cJSON
 */
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        cJSON *root = cJSON_Parse(json);
        if (!root) {
                return 0;
        }

        /* Sum up Produce requests from all brokers */
        int64_t total_produce_reqs = 0;
        double int_latency_p50 = 0, int_latency_p95 = 0, int_latency_p99 = 0;
        double rtt_p50 = 0, rtt_p95 = 0, rtt_p99 = 0;
        int broker_count = 0;

        cJSON *brokers = cJSON_GetObjectItem(root, "brokers");
        if (brokers && cJSON_IsObject(brokers)) {
                cJSON *broker = NULL;
                cJSON_ArrayForEach(broker, brokers) {
                        cJSON *req = cJSON_GetObjectItem(broker, "req");
                        if (req && cJSON_IsObject(req)) {
                                cJSON *produce = cJSON_GetObjectItem(req, "Produce");
                                if (produce && cJSON_IsNumber(produce)) {
                                        total_produce_reqs += (int64_t)cJSON_GetNumberValue(produce);
                                }
                        }

                        cJSON *int_lat = cJSON_GetObjectItem(broker, "int_latency");
                        if (int_lat && cJSON_IsObject(int_lat)) {
                                int_latency_p50 += get_json_double(int_lat, "p50", 0);
                                int_latency_p95 += get_json_double(int_lat, "p95", 0);
                                int_latency_p99 += get_json_double(int_lat, "p99", 0);
                        }

                        cJSON *rtt = cJSON_GetObjectItem(broker, "rtt");
                        if (rtt && cJSON_IsObject(rtt)) {
                                rtt_p50 += get_json_double(rtt, "p50", 0);
                                rtt_p95 += get_json_double(rtt, "p95", 0);
                                rtt_p99 += get_json_double(rtt, "p99", 0);
                        }

                        broker_count++;
                }
        }

        /* Average broker latencies */
        if (broker_count > 0) {
                int_latency_p50 /= broker_count;
                int_latency_p95 /= broker_count;
                int_latency_p99 /= broker_count;
                rtt_p50 /= broker_count;
                rtt_p95 /= broker_count;
                rtt_p99 /= broker_count;
        }

        /* Store in context */
        mtx_lock(&bench_ctx->lock);
        bench_ctx->last_produce_requests = total_produce_reqs;
        bench_ctx->last_int_latency_p50 = int_latency_p50;
        bench_ctx->last_int_latency_p95 = int_latency_p95;
        bench_ctx->last_int_latency_p99 = int_latency_p99;
        bench_ctx->last_rtt_p50 = rtt_p50;
        bench_ctx->last_rtt_p95 = rtt_p95;
        bench_ctx->last_rtt_p99 = rtt_p99;
        mtx_unlock(&bench_ctx->lock);

        cJSON_Delete(root);
        return 0;
}

/* Per-message opaque data */
typedef struct {
        int64_t enqueue_ts;
        int msg_seq;
} msg_opaque_t;

/**
 * Delivery report callback - record latency
 */
static void dr_cb(rd_kafka_t *rk,
                  const rd_kafka_message_t *rkmessage,
                  void *opaque) {
        int64_t now = test_clock();
        msg_opaque_t *msg_data = (msg_opaque_t *)rkmessage->_private;

        mtx_lock(&bench_ctx->lock);

        /* Always record full latency for percentile calculation */
        if (bench_ctx->msg_lat_cnt >= bench_ctx->msg_lat_size) {
                bench_ctx->msg_lat_size = bench_ctx->msg_lat_size ? bench_ctx->msg_lat_size * 2 : 100000;
                bench_ctx->msg_latencies = realloc(bench_ctx->msg_latencies,
                                                   sizeof(msg_latency_t) * bench_ctx->msg_lat_size);
        }

        int lat_idx = bench_ctx->msg_lat_cnt++;
        bench_ctx->msg_latencies[lat_idx].enqueue_ts = msg_data->enqueue_ts;
        bench_ctx->msg_latencies[lat_idx].delivery_ts = now;
        bench_ctx->msg_latencies[lat_idx].partition = rkmessage->partition;
        bench_ctx->msg_latencies[lat_idx].msg_seq = msg_data->msg_seq;

        /* Sample every 100th message for CSV output */
        if (msg_data->msg_seq % 100 == 0) {
                if (bench_ctx->sampled_lat_cnt >= bench_ctx->sampled_lat_size) {
                        bench_ctx->sampled_lat_size = bench_ctx->sampled_lat_size ? bench_ctx->sampled_lat_size * 2 : 10000;
                        bench_ctx->sampled_latencies = realloc(bench_ctx->sampled_latencies,
                                                               sizeof(msg_latency_t) * bench_ctx->sampled_lat_size);
                }

                int sample_idx = bench_ctx->sampled_lat_cnt++;
                bench_ctx->sampled_latencies[sample_idx] = bench_ctx->msg_latencies[lat_idx];
        }

        mtx_unlock(&bench_ctx->lock);

        if (rkmessage->err) {
                /* Don't fail test, just log error */
                TEST_WARN("Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
        }

        free(rkmessage->_private);
}

/**
 * Comparison function for qsort
 */
static int cmp_int64(const void *a, const void *b) {
        int64_t aa = *(int64_t *)a;
        int64_t bb = *(int64_t *)b;
        return (aa > bb) - (aa < bb);
}

/**
 * Calculate percentile from sorted array
 */
static int64_t percentile(int64_t *sorted, int cnt, double p) {
        if (cnt == 0)
                return 0;
        int idx = (int)(cnt * p / 100.0);
        if (idx >= cnt)
                idx = cnt - 1;
        return sorted[idx];
}

/**
 * Create output directory if it doesn't exist
 */
static void ensure_output_dir(const char *dir) {
        struct stat st = {0};
        if (stat(dir, &st) == -1) {
                mkdir(dir, 0755);
        }
}

/**
 * Create producer with standard configuration
 */
static rd_kafka_t *create_producer(bench_config_t *config, const char *topic) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        test_conf_init(&conf, NULL, 120);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);
        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        /* Standard MultiBatch configuration */
        test_conf_set(conf, "statistics.interval.ms", "50");
        test_conf_set(conf, "queue.buffering.max.messages", "1000000");
        test_conf_set(conf, "queue.buffering.max.kbytes", "102400"); /* 100MB */
        test_conf_set(conf, "linger.ms", "500");
        test_conf_set(conf, "compression.type", "lz4");
        test_conf_set(conf, "message.max.bytes", "100000000"); /* 100MB */
        test_conf_set(conf, "batch.size", "10000000"); /* 10MB */
        test_conf_set(conf, "batch.num.messages", "100000");
        test_conf_set(conf, "produce.request.max.partitions", "10000");
        test_conf_set(conf, "queue.buffering.backpressure.threshold", "1");

        /* Allow override via environment */
        const char *max_partitions = test_getenv("MAX_PARTITIONS", NULL);
        if (max_partitions) {
                test_conf_set(conf, "produce.request.max.partitions", max_partitions);
        }

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        return rk;
}

/**
 * Run max throughput benchmark
 */
static void run_max_throughput_phase(bench_config_t *config, const char *topic) {
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        int64_t start_ts, end_ts;
        int64_t start_requests;
        int messages_sent = 0;
        int messages_failed = 0;

        TEST_SAY("\n========================================\n");
        TEST_SAY("MAX THROUGHPUT MODE\n");
        TEST_SAY("========================================\n");
        TEST_SAY("Messages: %d\n", config->max_throughput_messages);
        TEST_SAY("Partitions: %d\n", config->partition_cnt);
        TEST_SAY("Message size: %d bytes\n", config->message_size);
        TEST_SAY("========================================\n\n");

        /* Create producer */
        rk = create_producer(config, topic);
        rkt = test_create_producer_topic(rk, topic, "acks", "1", NULL);

        /* Reset latency tracking */
        mtx_lock(&bench_ctx->lock);
        bench_ctx->msg_lat_cnt = 0;
        bench_ctx->sampled_lat_cnt = 0;
        bench_ctx->current_mode = BENCH_MODE_MAX_THROUGHPUT;
        bench_ctx->current_rate = 0;
        mtx_unlock(&bench_ctx->lock);

        /* Get initial stats */
        for (int i = 0; i < 3; i++)
                rd_kafka_poll(rk, 100);
        mtx_lock(&bench_ctx->lock);
        start_requests = bench_ctx->last_produce_requests;
        mtx_unlock(&bench_ctx->lock);

        /* Prepare payload */
        char *payload = malloc(config->message_size);
        memset(payload, 'x', config->message_size);

        /* Start timing and produce messages as fast as possible */
        start_ts = test_clock();

        for (int i = 0; i < config->max_throughput_messages; i++) {
                int32_t partition = i % config->partition_cnt;

                msg_opaque_t *msg_data = malloc(sizeof(msg_opaque_t));
                msg_data->enqueue_ts = test_clock();
                msg_data->msg_seq = i;

                if (rd_kafka_produce(rkt, partition,
                                    RD_KAFKA_MSG_F_COPY,
                                    payload, config->message_size,
                                    NULL, 0,
                                    msg_data) == -1) {
                        messages_failed++;
                        free(msg_data);
                } else {
                        messages_sent++;
                }

                /* Poll occasionally */
                if (i % 1000 == 0)
                        rd_kafka_poll(rk, 0);
        }

        free(payload);

        /* Wait for all deliveries */
        TEST_SAY("Waiting for delivery confirmations...\n");
        rd_kafka_flush(rk, 60000);
        end_ts = test_clock();

        /* Get final stats */
        for (int i = 0; i < 5; i++)
                rd_kafka_poll(rk, 100);

        mtx_lock(&bench_ctx->lock);
        int64_t end_requests = bench_ctx->last_produce_requests;

        /* Calculate latency percentiles */
        int64_t *latencies = malloc(sizeof(int64_t) * bench_ctx->msg_lat_cnt);
        for (int i = 0; i < bench_ctx->msg_lat_cnt; i++) {
                latencies[i] = bench_ctx->msg_latencies[i].delivery_ts -
                              bench_ctx->msg_latencies[i].enqueue_ts;
        }
        qsort(latencies, bench_ctx->msg_lat_cnt, sizeof(int64_t), cmp_int64);

        double p50 = percentile(latencies, bench_ctx->msg_lat_cnt, 50) / 1000.0;
        double p95 = percentile(latencies, bench_ctx->msg_lat_cnt, 95) / 1000.0;
        double p99 = percentile(latencies, bench_ctx->msg_lat_cnt, 99) / 1000.0;

        free(latencies);

        /* Store phase results */
        if (bench_ctx->phase_cnt >= bench_ctx->phase_size) {
                bench_ctx->phase_size = bench_ctx->phase_size ? bench_ctx->phase_size * 2 : 10;
                bench_ctx->phases = realloc(bench_ctx->phases,
                                           sizeof(phase_stats_t) * bench_ctx->phase_size);
        }

        phase_stats_t *phase = &bench_ctx->phases[bench_ctx->phase_cnt++];
        phase->mode = BENCH_MODE_MAX_THROUGHPUT;
        phase->rate_msgs_sec = 0;
        phase->duration_sec = (double)(end_ts - start_ts) / 1000000.0;
        phase->messages_sent = messages_sent;
        phase->messages_failed = messages_failed;
        phase->throughput_msgs_sec = messages_sent / phase->duration_sec;
        phase->throughput_mb_sec = (messages_sent * config->message_size) / phase->duration_sec / 1000000.0;
        phase->request_count = end_requests - start_requests;
        phase->latency_p50_ms = p50;
        phase->latency_p95_ms = p95;
        phase->latency_p99_ms = p99;
        phase->int_latency_p50 = bench_ctx->last_int_latency_p50;
        phase->int_latency_p95 = bench_ctx->last_int_latency_p95;
        phase->int_latency_p99 = bench_ctx->last_int_latency_p99;
        phase->rtt_p50 = bench_ctx->last_rtt_p50;
        phase->rtt_p95 = bench_ctx->last_rtt_p95;
        phase->rtt_p99 = bench_ctx->last_rtt_p99;

        mtx_unlock(&bench_ctx->lock);

        /* Print results */
        TEST_SAY("\nResults:\n");
        TEST_SAY("  Duration: %.2f sec\n", phase->duration_sec);
        TEST_SAY("  Messages sent: %d (failed: %d)\n", messages_sent, messages_failed);
        TEST_SAY("  Throughput: %.1f msgs/sec (%.2f MB/sec)\n",
                phase->throughput_msgs_sec, phase->throughput_mb_sec);
        TEST_SAY("  Requests: %lld (%.1f msgs/req)\n",
                (long long)phase->request_count,
                (double)messages_sent / phase->request_count);
        TEST_SAY("  Latency: p50=%.2fms, p95=%.2fms, p99=%.2fms\n",
                p50, p95, p99);

        /* Cleanup */
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
}

/**
 * Run controlled rate benchmark
 */
static void run_controlled_rate_phase(bench_config_t *config, const char *topic, int rate_msgs_sec) {
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        int64_t start_ts, end_ts;
        int64_t start_requests;
        int messages_sent = 0;
        int messages_failed = 0;
        int target_messages = rate_msgs_sec * config->controlled_duration_sec;

        TEST_SAY("\n========================================\n");
        TEST_SAY("CONTROLLED RATE MODE\n");
        TEST_SAY("========================================\n");
        TEST_SAY("Target rate: %d msgs/sec\n", rate_msgs_sec);
        TEST_SAY("Duration: %d seconds\n", config->controlled_duration_sec);
        TEST_SAY("Target messages: %d\n", target_messages);
        TEST_SAY("Partitions: %d\n", config->partition_cnt);
        TEST_SAY("Message size: %d bytes\n", config->message_size);
        TEST_SAY("========================================\n\n");

        /* Create producer */
        rk = create_producer(config, topic);
        rkt = test_create_producer_topic(rk, topic, "acks", "1", NULL);

        /* Reset latency tracking */
        mtx_lock(&bench_ctx->lock);
        bench_ctx->msg_lat_cnt = 0;
        bench_ctx->sampled_lat_cnt = 0;
        bench_ctx->current_mode = BENCH_MODE_CONTROLLED_RATE;
        bench_ctx->current_rate = rate_msgs_sec;
        mtx_unlock(&bench_ctx->lock);

        /* Get initial stats */
        for (int i = 0; i < 3; i++)
                rd_kafka_poll(rk, 100);
        mtx_lock(&bench_ctx->lock);
        start_requests = bench_ctx->last_produce_requests;
        mtx_unlock(&bench_ctx->lock);

        /* Prepare payload */
        char *payload = malloc(config->message_size);
        memset(payload, 'x', config->message_size);

        /* Start timing and produce at controlled rate */
        start_ts = test_clock();
        int64_t interval_us = 1000000 / rate_msgs_sec; /* Microseconds between messages */
        int64_t next_send_ts = start_ts;

        for (int i = 0; i < target_messages; i++) {
                /* Rate limiting: wait until it's time to send next message */
                int64_t now = test_clock();
                if (now < next_send_ts) {
                        int64_t sleep_us = next_send_ts - now;
                        if (sleep_us > 0 && sleep_us < 1000000) {
                                usleep((useconds_t)sleep_us);
                        }
                }

                int32_t partition = i % config->partition_cnt;

                msg_opaque_t *msg_data = malloc(sizeof(msg_opaque_t));
                msg_data->enqueue_ts = test_clock();
                msg_data->msg_seq = i;

                if (rd_kafka_produce(rkt, partition,
                                    RD_KAFKA_MSG_F_COPY,
                                    payload, config->message_size,
                                    NULL, 0,
                                    msg_data) == -1) {
                        messages_failed++;
                        free(msg_data);
                } else {
                        messages_sent++;
                }

                next_send_ts += interval_us;

                /* Poll occasionally */
                if (i % 1000 == 0)
                        rd_kafka_poll(rk, 0);
        }

        free(payload);

        /* Wait for all deliveries */
        TEST_SAY("Waiting for delivery confirmations...\n");
        rd_kafka_flush(rk, 60000);
        end_ts = test_clock();

        /* Get final stats */
        for (int i = 0; i < 5; i++)
                rd_kafka_poll(rk, 100);

        mtx_lock(&bench_ctx->lock);
        int64_t end_requests = bench_ctx->last_produce_requests;

        /* Calculate latency percentiles */
        int64_t *latencies = malloc(sizeof(int64_t) * bench_ctx->msg_lat_cnt);
        for (int i = 0; i < bench_ctx->msg_lat_cnt; i++) {
                latencies[i] = bench_ctx->msg_latencies[i].delivery_ts -
                              bench_ctx->msg_latencies[i].enqueue_ts;
        }
        qsort(latencies, bench_ctx->msg_lat_cnt, sizeof(int64_t), cmp_int64);

        double p50 = percentile(latencies, bench_ctx->msg_lat_cnt, 50) / 1000.0;
        double p95 = percentile(latencies, bench_ctx->msg_lat_cnt, 95) / 1000.0;
        double p99 = percentile(latencies, bench_ctx->msg_lat_cnt, 99) / 1000.0;

        free(latencies);

        /* Store phase results */
        if (bench_ctx->phase_cnt >= bench_ctx->phase_size) {
                bench_ctx->phase_size = bench_ctx->phase_size ? bench_ctx->phase_size * 2 : 10;
                bench_ctx->phases = realloc(bench_ctx->phases,
                                           sizeof(phase_stats_t) * bench_ctx->phase_size);
        }

        phase_stats_t *phase = &bench_ctx->phases[bench_ctx->phase_cnt++];
        phase->mode = BENCH_MODE_CONTROLLED_RATE;
        phase->rate_msgs_sec = rate_msgs_sec;
        phase->duration_sec = (double)(end_ts - start_ts) / 1000000.0;
        phase->messages_sent = messages_sent;
        phase->messages_failed = messages_failed;
        phase->throughput_msgs_sec = messages_sent / phase->duration_sec;
        phase->throughput_mb_sec = (messages_sent * config->message_size) / phase->duration_sec / 1000000.0;
        phase->request_count = end_requests - start_requests;
        phase->latency_p50_ms = p50;
        phase->latency_p95_ms = p95;
        phase->latency_p99_ms = p99;
        phase->int_latency_p50 = bench_ctx->last_int_latency_p50;
        phase->int_latency_p95 = bench_ctx->last_int_latency_p95;
        phase->int_latency_p99 = bench_ctx->last_int_latency_p99;
        phase->rtt_p50 = bench_ctx->last_rtt_p50;
        phase->rtt_p95 = bench_ctx->last_rtt_p95;
        phase->rtt_p99 = bench_ctx->last_rtt_p99;

        mtx_unlock(&bench_ctx->lock);

        /* Print results */
        TEST_SAY("\nResults:\n");
        TEST_SAY("  Duration: %.2f sec\n", phase->duration_sec);
        TEST_SAY("  Messages sent: %d (failed: %d)\n", messages_sent, messages_failed);
        TEST_SAY("  Actual rate: %.1f msgs/sec (target: %d)\n",
                phase->throughput_msgs_sec, rate_msgs_sec);
        TEST_SAY("  Throughput: %.2f MB/sec\n", phase->throughput_mb_sec);
        TEST_SAY("  Requests: %lld (%.1f msgs/req)\n",
                (long long)phase->request_count,
                (double)messages_sent / phase->request_count);
        TEST_SAY("  Latency: p50=%.2fms, p95=%.2fms, p99=%.2fms\n",
                p50, p95, p99);

        /* Cleanup */
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
}

/**
 * Output results to CSV and summary
 */
static void output_results(bench_config_t *config) {
        ensure_output_dir(config->output_dir);

        /* Write summary CSV */
        char summary_path[512];
        snprintf(summary_path, sizeof(summary_path), "%s/summary.csv", config->output_dir);
        FILE *summary_fp = fopen(summary_path, "w");
        if (!summary_fp) {
                TEST_WARN("Failed to open %s for writing\n", summary_path);
                return;
        }

        fprintf(summary_fp, "mode,rate_msgs_sec,duration_sec,messages_sent,messages_failed,"
                           "requests_sent,msgs_per_req,throughput_msgs_sec,throughput_mb_sec,"
                           "latency_p50_ms,latency_p95_ms,latency_p99_ms\n");

        for (int i = 0; i < bench_ctx->phase_cnt; i++) {
                phase_stats_t *p = &bench_ctx->phases[i];
                fprintf(summary_fp, "%s,%d,%.2f,%d,%d,%lld,%.1f,%.1f,%.2f,%.2f,%.2f,%.2f\n",
                       p->mode == BENCH_MODE_MAX_THROUGHPUT ? "max_throughput" : "controlled",
                       p->rate_msgs_sec,
                       p->duration_sec,
                       p->messages_sent,
                       p->messages_failed,
                       (long long)p->request_count,
                       (double)p->messages_sent / p->request_count,
                       p->throughput_msgs_sec,
                       p->throughput_mb_sec,
                       p->latency_p50_ms,
                       p->latency_p95_ms,
                       p->latency_p99_ms);
        }

        fclose(summary_fp);
        TEST_SAY("Wrote summary to %s\n", summary_path);

        /* Write per-message sampled latencies CSV */
        char latencies_path[512];
        snprintf(latencies_path, sizeof(latencies_path), "%s/per_message_latencies.csv", config->output_dir);
        FILE *latencies_fp = fopen(latencies_path, "w");
        if (!latencies_fp) {
                TEST_WARN("Failed to open %s for writing\n", latencies_path);
                return;
        }

        fprintf(latencies_fp, "msg_seq,enqueue_timestamp_us,delivery_timestamp_us,latency_us,partition\n");

        mtx_lock(&bench_ctx->lock);
        for (int i = 0; i < bench_ctx->sampled_lat_cnt; i++) {
                msg_latency_t *m = &bench_ctx->sampled_latencies[i];
                fprintf(latencies_fp, "%d,%lld,%lld,%lld,%d\n",
                       m->msg_seq,
                       (long long)m->enqueue_ts,
                       (long long)m->delivery_ts,
                       (long long)(m->delivery_ts - m->enqueue_ts),
                       m->partition);
        }
        mtx_unlock(&bench_ctx->lock);

        fclose(latencies_fp);
        TEST_SAY("Wrote sampled latencies to %s\n", latencies_path);

        /* Print human-readable summary */
        TEST_SAY("\n");
        TEST_SAY("================================================================================\n");
        TEST_SAY("                       BENCHMARK RESULTS SUMMARY\n");
        TEST_SAY("================================================================================\n");
        TEST_SAY("Configuration:\n");
        TEST_SAY("  Partitions: %d\n", config->partition_cnt);
        TEST_SAY("  Message size: %d bytes\n", config->message_size);
        TEST_SAY("  Output directory: %s\n", config->output_dir);
        TEST_SAY("\n");

        if (!config->skip_max_throughput) {
                TEST_SAY("MAX THROUGHPUT MODE:\n");
                phase_stats_t *p = &bench_ctx->phases[0];
                TEST_SAY("  Messages: %d (failed: %d)\n", p->messages_sent, p->messages_failed);
                TEST_SAY("  Duration: %.2f sec\n", p->duration_sec);
                TEST_SAY("  Throughput: %.1f msgs/sec (%.2f MB/sec)\n",
                        p->throughput_msgs_sec, p->throughput_mb_sec);
                TEST_SAY("  Requests: %lld (%.1f msgs/req)\n",
                        (long long)p->request_count,
                        (double)p->messages_sent / p->request_count);
                TEST_SAY("  Latency: p50=%.2fms, p95=%.2fms, p99=%.2fms\n",
                        p->latency_p50_ms, p->latency_p95_ms, p->latency_p99_ms);
                TEST_SAY("\n");
        }

        if (!config->skip_controlled) {
                TEST_SAY("CONTROLLED RATE MODE RESULTS:\n");
                TEST_SAY("%-12s %-12s %-12s %-12s %-12s %-12s %-12s\n",
                        "Rate", "Duration", "Messages", "Requests", "msgs/req", "p50 lat", "p99 lat");
                TEST_SAY("%-12s %-12s %-12s %-12s %-12s %-12s %-12s\n",
                        "(msg/s)", "(s)", "sent", "sent", "", "(ms)", "(ms)");
                TEST_SAY("------------ ------------ ------------ ------------ ------------ ------------ ------------\n");

                int start_idx = config->skip_max_throughput ? 0 : 1;
                for (int i = start_idx; i < bench_ctx->phase_cnt; i++) {
                        phase_stats_t *p = &bench_ctx->phases[i];
                        TEST_SAY("%-12d %-12.1f %-12d %-12lld %-12.1f %-12.2f %-12.2f\n",
                                p->rate_msgs_sec,
                                p->duration_sec,
                                p->messages_sent,
                                (long long)p->request_count,
                                (double)p->messages_sent / p->request_count,
                                p->latency_p50_ms,
                                p->latency_p99_ms);
                }
                TEST_SAY("\n");
        }

        TEST_SAY("================================================================================\n");
        TEST_SAY("Results saved to: %s/\n", config->output_dir);
        TEST_SAY("================================================================================\n");
}

/**
 * Parse configuration from environment variables
 *
 * Environment variables:
 *   MAX_PARTITIONS                Number of partitions (default: 1000)
 *   BENCH_MAX_THROUGHPUT_MESSAGES Messages for max throughput test (default: 100000)
 *   BENCH_CONTROLLED_RATES        Comma-separated rates, e.g. "10000,20000,50000" (default: 10000,20000,50000)
 *   BENCH_CONTROLLED_DURATION     Seconds per controlled rate test (default: 60)
 *   BENCH_MESSAGE_SIZE            Bytes per message (default: 256)
 *   BENCH_OUTPUT_DIR              Output directory (default: ./benchmark_output)
 *   BENCH_SKIP_MAX_THROUGHPUT     Set to "1" to skip max throughput phase
 *   BENCH_SKIP_CONTROLLED         Set to "1" to skip controlled rate phases
 */
static void parse_config(bench_config_t *config) {
        const char *val;

        /* Set defaults */
        config->partition_cnt = 1000;
        config->message_size = 256;
        config->max_throughput_messages = 100000;
        config->skip_max_throughput = 0;
        config->controlled_duration_sec = 60;
        config->skip_controlled = 0;
        config->output_dir = strdup("./benchmark_output");

        /* Default controlled rates */
        config->controlled_rate_cnt = 3;
        config->controlled_rates = malloc(sizeof(int) * 3);
        config->controlled_rates[0] = 10000;
        config->controlled_rates[1] = 20000;
        config->controlled_rates[2] = 50000;

        /* Override from environment */
        if ((val = test_getenv("MAX_PARTITIONS", NULL)))
                config->partition_cnt = atoi(val);

        if ((val = test_getenv("BENCH_MAX_THROUGHPUT_MESSAGES", NULL)))
                config->max_throughput_messages = atoi(val);

        if ((val = test_getenv("BENCH_CONTROLLED_RATES", NULL))) {
                /* Parse comma-separated rates */
                free(config->controlled_rates);
                config->controlled_rate_cnt = 0;
                config->controlled_rates = NULL;

                char *rates_str = strdup(val);
                char *token = strtok(rates_str, ",");
                while (token) {
                        config->controlled_rates = realloc(config->controlled_rates,
                                                          sizeof(int) * (config->controlled_rate_cnt + 1));
                        config->controlled_rates[config->controlled_rate_cnt++] = atoi(token);
                        token = strtok(NULL, ",");
                }
                free(rates_str);
        }

        if ((val = test_getenv("BENCH_CONTROLLED_DURATION", NULL)))
                config->controlled_duration_sec = atoi(val);

        if ((val = test_getenv("BENCH_MESSAGE_SIZE", NULL)))
                config->message_size = atoi(val);

        if ((val = test_getenv("BENCH_OUTPUT_DIR", NULL))) {
                free(config->output_dir);
                config->output_dir = strdup(val);
        }

        if ((val = test_getenv("BENCH_SKIP_MAX_THROUGHPUT", NULL)))
                config->skip_max_throughput = atoi(val);

        if ((val = test_getenv("BENCH_SKIP_CONTROLLED", NULL)))
                config->skip_controlled = atoi(val);
}

/**
 * Main benchmark entry point
 */
int main_0200_multibatch_benchmark(int argc, char **argv) {
        bench_config_t config;
        const char *topic;

        /* Parse configuration from environment */
        parse_config(&config);

        /* Initialize benchmark context */
        bench_ctx = calloc(1, sizeof(bench_ctx_t));
        bench_ctx->config = &config;
        mtx_init(&bench_ctx->lock, mtx_plain);

        /* Create topic */
        topic = test_mk_topic_name(__FUNCTION__, 1);

        /* Create a temporary producer just for topic creation */
        rd_kafka_t *rk_temp = create_producer(&config, topic);
        test_create_topic_wait_exists(rk_temp, topic, config.partition_cnt, 1, 30000);
        rd_kafka_destroy(rk_temp);

        TEST_SAY("\n");
        TEST_SAY("================================================================================\n");
        TEST_SAY("                    MULTIBATCH BENCHMARK SUITE\n");
        TEST_SAY("================================================================================\n");
        TEST_SAY("Topic: %s\n", topic);
        TEST_SAY("Partitions: %d\n", config.partition_cnt);
        TEST_SAY("Message size: %d bytes\n", config.message_size);
        TEST_SAY("Output directory: %s\n", config.output_dir);
        TEST_SAY("================================================================================\n");

        /* Run max throughput phase */
        if (!config.skip_max_throughput) {
                run_max_throughput_phase(&config, topic);
        }

        /* Run controlled rate phases */
        if (!config.skip_controlled) {
                for (int i = 0; i < config.controlled_rate_cnt; i++) {
                        run_controlled_rate_phase(&config, topic, config.controlled_rates[i]);
                }
        }

        /* Output results */
        output_results(&config);

        /* Cleanup */
        mtx_destroy(&bench_ctx->lock);
        if (bench_ctx->msg_latencies)
                free(bench_ctx->msg_latencies);
        if (bench_ctx->sampled_latencies)
                free(bench_ctx->sampled_latencies);
        if (bench_ctx->phases)
                free(bench_ctx->phases);
        free(bench_ctx);

        free(config.output_dir);
        free(config.controlled_rates);

        return 0;
}
