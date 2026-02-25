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
 * - Multibatch efficiency (partitions/req, request size, fill %, batch wait)
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
 *   BENCH_BATCH_NUM_MESSAGES      batch.num.messages (default: 100000)
 *   BENCH_BATCH_SIZE              batch.size (default: 1000000)
 *   BENCH_ENQUEUE_RETRY_MS        Retry budget per message on QUEUE_FULL (default: 1000)
 *   BENCH_OUTPUT_DIR              Output directory (default: ./benchmark_output)
 *   BENCH_SKIP_MAX_THROUGHPUT     Set to "1" to skip max throughput phase
 *   BENCH_SKIP_CONTROLLED         Set to "1" to skip controlled rate phases
 *   BENCH_V1_LINGER_MS            v1 linger.ms for v0/v0+multibatch profiles (default: 500)
 *
 * Broker-level batching configuration:
 *   BROKER_LINGER_MS              Broker-level linger in ms (default: 500)
 *   BROKER_BATCH_MAX_PARTITIONS   Early-send partition threshold (-1=disabled, default)
 *   BROKER_BATCH_MAX_BYTES        Early-send byte threshold (-1=disabled, default)
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
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* Forward declarations */
static void ensure_output_dir(const char *dir);

/* Benchmark mode */
typedef enum {
        BENCH_MODE_MAX_THROUGHPUT,
        BENCH_MODE_CONTROLLED_RATE
} bench_mode_t;

/* Producer implementation profile */
typedef enum {
        BENCH_PRODUCER_PROFILE_V0,
        BENCH_PRODUCER_PROFILE_V0_MULTIBATCH,
        BENCH_PRODUCER_PROFILE_V2
} bench_producer_profile_t;

/* Benchmark configuration */
typedef struct {
        /* Common config */
        int partition_cnt;
        int message_size;
        int enqueue_retry_ms;
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
        rd_kafka_resp_err_t dr_err;
} msg_latency_t;

/* Per-phase statistics */
typedef struct {
        bench_mode_t mode;
        bench_producer_profile_t producer_profile;
        int rate_msgs_sec;              /* For controlled rate mode, 0 for max throughput */
        double duration_sec;
        int messages_attempted;
        int messages_sent;
        int messages_failed;
        int dr_success;
        int dr_failed;
        int dr_total;
        int messages_undelivered;
        double throughput_msgs_sec;
        double throughput_mb_sec;
        int64_t request_count; /* Produce requests only */
        int64_t total_request_count;

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

        /* Multibatch efficiency statistics (per-broker averages) */
        double produce_partitions_p50;
        double produce_partitions_avg;
        double produce_reqsize_avg_bytes;
        double produce_fill_avg_permille;
        double batch_wait_p50_us;
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
        int64_t last_total_requests;
        int64_t last_produce_requests;
        double last_int_latency_p50;
        double last_int_latency_p95;
        double last_int_latency_p99;
        double last_rtt_p50;
        double last_rtt_p95;
        double last_rtt_p99;
        double last_produce_partitions_p50;
        double last_produce_partitions_avg;
        double last_produce_reqsize_avg_bytes;
        double last_produce_fill_avg_permille;
        double last_batch_wait_p50_us;
        int phase_dr_success;
        int phase_dr_failed;
        int phase_dr_total;

        /* Results */
        phase_stats_t *phases;
        int phase_cnt;
        int phase_size;

        mtx_t lock;
} bench_ctx_t;

static bench_ctx_t *bench_ctx = NULL;

static const char *
bench_producer_profile_name(bench_producer_profile_t producer_profile) {
        switch (producer_profile) {
        case BENCH_PRODUCER_PROFILE_V0:
                return "v0";
        case BENCH_PRODUCER_PROFILE_V0_MULTIBATCH:
                return "v0+multibatch";
        case BENCH_PRODUCER_PROFILE_V2:
                return "v2";
        default:
                return "unknown";
        }
}

/**
 * Typed stats callback - extract statistics directly from struct
 */
static void stats_cb(rd_kafka_t *rk,
                     const rd_kafka_stats_t *stats,
                     void *opaque) {
        uint32_t i;
        int64_t total_tx = 0;
        int64_t total_produce_tx = 0;
        double sum_int_latency_p50 = 0, sum_int_latency_p95 = 0, sum_int_latency_p99 = 0;
        double sum_rtt_p50 = 0, sum_rtt_p95 = 0, sum_rtt_p99 = 0;
        double sum_produce_partitions_p50 = 0, sum_produce_partitions_avg = 0;
        double sum_produce_reqsize_avg = 0, sum_produce_fill_avg = 0;
        double sum_batch_wait_p50 = 0;

        (void)rk;
        (void)opaque;

        if (!bench_ctx)
                return;

        mtx_lock(&bench_ctx->lock);

        /* Aggregate stats across all brokers */
        for (i = 0; i < stats->broker_cnt; i++) {
                const rd_kafka_broker_stats_t *broker = &stats->brokers[i];
                uint32_t req_i;

                /* Count total requests */
                total_tx += broker->tx;
                for (req_i = 0; req_i < broker->req_cnt; req_i++) {
                        if (!strcmp(broker->reqs[req_i].name, "Produce")) {
                                total_produce_tx += broker->reqs[req_i].count;
                                break;
                        }
                }

                /* Internal latency (time from produce() to broker send) */
                sum_int_latency_p50 += (double)broker->int_latency.p50;
                sum_int_latency_p95 += (double)broker->int_latency.p95;
                sum_int_latency_p99 += (double)broker->int_latency.p99;

                /* RTT (broker round-trip time) */
                sum_rtt_p50 += (double)broker->rtt.p50;
                sum_rtt_p95 += (double)broker->rtt.p95;
                sum_rtt_p99 += (double)broker->rtt.p99;

                /* Produce request partitions per request */
                sum_produce_partitions_p50 += (double)broker->produce_partitions.p50;
                sum_produce_partitions_avg += (double)broker->produce_partitions.avg;

                /* Produce request size */
                sum_produce_reqsize_avg += (double)broker->produce_reqsize.avg;

                /* Produce fill ratio (permille) */
                sum_produce_fill_avg += (double)broker->produce_fill.avg;

                /* Batch wait time (broker-level aggregate) */
                sum_batch_wait_p50 += (double)broker->batch_wait.p50;
        }

        /* Calculate averages across brokers */
        if (stats->broker_cnt > 0) {
                bench_ctx->last_total_requests = total_tx;
                bench_ctx->last_produce_requests = total_produce_tx;
                bench_ctx->last_int_latency_p50 = sum_int_latency_p50 / stats->broker_cnt;
                bench_ctx->last_int_latency_p95 = sum_int_latency_p95 / stats->broker_cnt;
                bench_ctx->last_int_latency_p99 = sum_int_latency_p99 / stats->broker_cnt;
                bench_ctx->last_rtt_p50 = sum_rtt_p50 / stats->broker_cnt;
                bench_ctx->last_rtt_p95 = sum_rtt_p95 / stats->broker_cnt;
                bench_ctx->last_rtt_p99 = sum_rtt_p99 / stats->broker_cnt;

                /* Only update produce stats if there was actual produce activity
                 * in this interval (cnt > 0). Otherwise zeros from "empty"
                 * intervals would overwrite valid data. This is important when
                 * statistics.interval.ms < broker.linger.ms. */
                if (sum_produce_partitions_avg > 0) {
                        bench_ctx->last_produce_partitions_p50 = sum_produce_partitions_p50 / stats->broker_cnt;
                        bench_ctx->last_produce_partitions_avg = sum_produce_partitions_avg / stats->broker_cnt;
                        bench_ctx->last_produce_reqsize_avg_bytes = sum_produce_reqsize_avg / stats->broker_cnt;
                        bench_ctx->last_produce_fill_avg_permille = sum_produce_fill_avg / stats->broker_cnt;
                        bench_ctx->last_batch_wait_p50_us = sum_batch_wait_p50 / stats->broker_cnt;
                }
        }

        mtx_unlock(&bench_ctx->lock);
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
        bench_ctx->msg_latencies[lat_idx].dr_err = rkmessage->err;
        bench_ctx->phase_dr_total++;
        if (rkmessage->err)
                bench_ctx->phase_dr_failed++;
        else
                bench_ctx->phase_dr_success++;

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
static rd_kafka_t *create_producer(bench_config_t *config,
                                   const char *topic,
                                   bench_producer_profile_t producer_profile) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const char *val;
        const char *v1_linger_ms;
        const char *batch_num_messages;
        const char *batch_size;

        (void)topic;

        test_conf_init(&conf, NULL, 120);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);
        /* Use typed stats callback only - disable JSON callback set by
         * test_conf_init to avoid double rd_avg_rollover consumption */
        rd_kafka_conf_set_stats_cb(conf, NULL);
        rd_kafka_conf_set_stats_cb_typed(conf, stats_cb);

        /* Standard MultiBatch configuration */
        test_conf_set(conf, "statistics.interval.ms", "50");
        test_conf_set(conf, "queue.buffering.max.messages", "1000000");
        test_conf_set(conf, "queue.buffering.max.kbytes", "102400"); /* 100MB */
        test_conf_set(conf, "compression.type", "lz4");
        test_conf_set(conf, "message.max.bytes", "100000000"); /* 100MB */
        batch_num_messages = test_getenv("BENCH_BATCH_NUM_MESSAGES", "100000");
        batch_size         = test_getenv("BENCH_BATCH_SIZE", "1000000");
        test_conf_set(conf, "batch.num.messages", batch_num_messages);
        test_conf_set(conf, "batch.size", batch_size);
        if (producer_profile == BENCH_PRODUCER_PROFILE_V2) {
                test_conf_set(conf, "produce.engine", "v2");
                /* test_conf_set(conf, "produce.request.max.partitions", "10000"); */
        } else {
                test_conf_set(conf, "produce.engine", "v1");
                test_conf_set(conf, "multibatch",
                              producer_profile == BENCH_PRODUCER_PROFILE_V0_MULTIBATCH
                                  ? "true"
                                  : "false");

                /* v1 path uses queue.buffering.max.ms for linger behavior. */
                v1_linger_ms =
                    test_getenv("BENCH_V1_LINGER_MS",
                                test_getenv("BROKER_LINGER_MS", "500"));
                test_conf_set(conf, "queue.buffering.max.ms", v1_linger_ms);
        }

        /* Broker-level batching configuration (new) */
        test_conf_set(conf, "broker.linger.ms", "500");  /* Default: 500ms */

        /* Allow broker.linger.ms override via environment */
        if ((val = test_getenv("BROKER_LINGER_MS", NULL)))
                test_conf_set(conf, "broker.linger.ms", val);

        /* Allow broker.batch.max.bytes override via environment
         * -1 = disabled (default), use broker.linger.ms only */
        test_conf_set(conf, "broker.batch.max.bytes", "10000000"); /* 10MB */
        if ((val = test_getenv("BROKER_BATCH_MAX_BYTES", NULL)))
                test_conf_set(conf, "broker.batch.max.bytes", val);

        /* Allow produce.request.max.partitions override via environment (v2 only) */
        if (producer_profile == BENCH_PRODUCER_PROFILE_V2 &&
            (val = test_getenv("MAX_PARTITIONS", NULL)))
                test_conf_set(conf, "produce.request.max.partitions", val);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        return rk;
}

/**
 * Safe division helpers used by summary/reporting paths.
 */
static double safe_div(double numerator, double denominator) {
        if (fabs(denominator) < 0.0000001)
                return 0.0;
        return numerator / denominator;
}

/**
 * Messages per ProduceRequest helper.
 */
static double msgs_per_req(int messages_sent, int64_t request_count) {
        if (request_count <= 0)
                return 0.0;
        return (double)messages_sent / (double)request_count;
}

static void reset_phase_tracking(bench_mode_t mode, int rate_msgs_sec) {
        mtx_lock(&bench_ctx->lock);
        bench_ctx->msg_lat_cnt = 0;
        bench_ctx->sampled_lat_cnt = 0;
        bench_ctx->current_mode = mode;
        bench_ctx->current_rate = rate_msgs_sec;
        bench_ctx->phase_dr_success = 0;
        bench_ctx->phase_dr_failed = 0;
        bench_ctx->phase_dr_total = 0;
        mtx_unlock(&bench_ctx->lock);
}

static void sample_request_counts(rd_kafka_t *rk,
                                  int poll_cnt,
                                  int64_t *produce_request_count,
                                  int64_t *total_request_count) {
        int i;

        for (i = 0; i < poll_cnt; i++)
                rd_kafka_poll(rk, 100);

        mtx_lock(&bench_ctx->lock);
        if (produce_request_count)
                *produce_request_count = bench_ctx->last_produce_requests;
        if (total_request_count)
                *total_request_count = bench_ctx->last_total_requests;
        mtx_unlock(&bench_ctx->lock);
}

static void flush_until_empty(rd_kafka_t *rk) {
        int flush_attempt = 0;
        int outq_len;
        rd_kafka_resp_err_t flush_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        int64_t flush_deadline_us = test_clock() + ((int64_t)tmout_multip(300000) * 1000);

        while ((outq_len = rd_kafka_outq_len(rk)) > 0) {
                flush_err = rd_kafka_flush(rk, 10000);
                flush_attempt++;
                outq_len = rd_kafka_outq_len(rk);

                if (outq_len == 0)
                        return;

                if ((flush_attempt % 6) == 0) {
                        TEST_SAY(
                            "Flush waiting: outq_len=%d attempt=%d last=%s\n",
                            outq_len, flush_attempt, rd_kafka_err2str(flush_err));
                }

                TEST_ASSERT(test_clock() < flush_deadline_us,
                            "Flush did not complete: outq_len=%d attempts=%d last=%s",
                            outq_len, flush_attempt, rd_kafka_err2str(flush_err));
        }
}

static void compute_latency_percentiles_locked(double *p50_ms,
                                               double *p95_ms,
                                               double *p99_ms,
                                               rd_bool_t success_only) {
        int i;
        int cnt = 0;
        int64_t *latencies;

        if (bench_ctx->msg_lat_cnt <= 0) {
                *p50_ms = 0.0;
                *p95_ms = 0.0;
                *p99_ms = 0.0;
                return;
        }

        latencies = malloc(sizeof(*latencies) * bench_ctx->msg_lat_cnt);
        TEST_ASSERT(latencies != NULL, "Failed to allocate latency array");

        for (i = 0; i < bench_ctx->msg_lat_cnt; i++) {
                if (success_only &&
                    bench_ctx->msg_latencies[i].dr_err !=
                        RD_KAFKA_RESP_ERR_NO_ERROR)
                        continue;

                latencies[cnt++] = bench_ctx->msg_latencies[i].delivery_ts -
                                   bench_ctx->msg_latencies[i].enqueue_ts;
        }

        if (cnt == 0) {
                *p50_ms = 0.0;
                *p95_ms = 0.0;
                *p99_ms = 0.0;
                free(latencies);
                return;
        }

        qsort(latencies, cnt, sizeof(*latencies), cmp_int64);
        *p50_ms = percentile(latencies, cnt, 50) / 1000.0;
        *p95_ms = percentile(latencies, cnt, 95) / 1000.0;
        *p99_ms = percentile(latencies, cnt, 99) / 1000.0;

        free(latencies);
}

static phase_stats_t *append_phase_locked(void) {
        if (bench_ctx->phase_cnt >= bench_ctx->phase_size) {
                phase_stats_t *new_phases;
                bench_ctx->phase_size =
                    bench_ctx->phase_size ? bench_ctx->phase_size * 2 : 10;
                new_phases = realloc(bench_ctx->phases,
                                     sizeof(*new_phases) * bench_ctx->phase_size);
                TEST_ASSERT(new_phases != NULL, "Failed to grow phase results");
                bench_ctx->phases = new_phases;
        }

        return &bench_ctx->phases[bench_ctx->phase_cnt++];
}

static void print_profile_runtime_config(bench_producer_profile_t producer_profile) {
        const char *broker_linger_ms =
            test_getenv("BROKER_LINGER_MS", "500");
        const char *broker_batch_max_bytes =
            test_getenv("BROKER_BATCH_MAX_BYTES", "10000000");
        const char *batch_num_messages =
            test_getenv("BENCH_BATCH_NUM_MESSAGES", "100000");
        const char *batch_size = test_getenv("BENCH_BATCH_SIZE", "1000000");
        const char *enqueue_retry_ms =
            test_getenv("BENCH_ENQUEUE_RETRY_MS", "1000");
        const char *max_partitions = test_getenv("MAX_PARTITIONS", NULL);
        const char *v1_linger_ms =
            test_getenv("BENCH_V1_LINGER_MS", broker_linger_ms);

        TEST_SAY("Profile config:\n");
        TEST_SAY("  produce.engine=%s\n",
                 producer_profile == BENCH_PRODUCER_PROFILE_V2 ? "v2" : "v1");
        if (producer_profile == BENCH_PRODUCER_PROFILE_V2) {
                TEST_SAY("  multibatch=(n/a, v2 engine)\n");
                TEST_SAY("  produce.request.max.partitions=%s\n",
                         max_partitions ? max_partitions : "(default)");
        } else {
                TEST_SAY("  multibatch=%s\n",
                         producer_profile ==
                                 BENCH_PRODUCER_PROFILE_V0_MULTIBATCH
                             ? "true"
                             : "false");
                TEST_SAY("  queue.buffering.max.ms=%s\n", v1_linger_ms);
        }
        TEST_SAY("  broker.linger.ms=%s\n", broker_linger_ms);
        TEST_SAY("  broker.batch.max.bytes=%s\n", broker_batch_max_bytes);
        TEST_SAY("  batch.num.messages=%s\n", batch_num_messages);
        TEST_SAY("  batch.size=%s\n", batch_size);
        TEST_SAY("  enqueue.retry.ms=%s\n", enqueue_retry_ms);
}

static void print_phase_header(const bench_config_t *config,
                               bench_mode_t mode,
                               bench_producer_profile_t producer_profile,
                               int rate_msgs_sec,
                               int target_messages) {
        TEST_SAY("\n========================================\n");
        TEST_SAY("%s\n",
                 mode == BENCH_MODE_MAX_THROUGHPUT ? "MAX THROUGHPUT MODE"
                                                   : "CONTROLLED RATE MODE");
        TEST_SAY("========================================\n");

        if (mode == BENCH_MODE_MAX_THROUGHPUT) {
                TEST_SAY("Producer profile: %s\n",
                         bench_producer_profile_name(producer_profile));
                TEST_SAY("Messages: %d\n", target_messages);
        } else {
                TEST_SAY("Target rate: %d msgs/sec\n", rate_msgs_sec);
                TEST_SAY("Producer profile: %s\n",
                         bench_producer_profile_name(producer_profile));
                TEST_SAY("Duration: %d seconds\n",
                         config->controlled_duration_sec);
                TEST_SAY("Target messages: %d\n", target_messages);
        }

        TEST_SAY("Partitions: %d\n", config->partition_cnt);
        TEST_SAY("Message size: %d bytes\n", config->message_size);
        print_profile_runtime_config(producer_profile);
        TEST_SAY("========================================\n\n");
}

static void produce_phase_messages(rd_kafka_t *rk,
                                   rd_kafka_topic_t *rkt,
                                   const bench_config_t *config,
                                   bench_mode_t mode,
                                   int rate_msgs_sec,
                                   int target_messages,
                                   void *payload,
                                   int *messages_sent,
                                   int *messages_failed) {
        int i;
        int poll_every = mode == BENCH_MODE_MAX_THROUGHPUT ? 1000 : 100;
        int64_t interval_us = 0;
        int64_t next_send_ts = 0;

        if (mode == BENCH_MODE_CONTROLLED_RATE) {
                interval_us = 1000000 / rate_msgs_sec;
                next_send_ts = test_clock();
        }

        for (i = 0; i < target_messages; i++) {
                int32_t partition;
                msg_opaque_t *msg_data;
                int64_t enqueue_deadline_us;
                rd_bool_t enqueued = rd_false;

                if (mode == BENCH_MODE_CONTROLLED_RATE) {
                        int64_t now = test_clock();
                        if (now < next_send_ts) {
                                int64_t sleep_us = next_send_ts - now;
                                if (sleep_us > 0 && sleep_us < 1000000)
                                        usleep((useconds_t)sleep_us);
                        }
                }

                partition = i % config->partition_cnt;
                msg_data = malloc(sizeof(*msg_data));
                TEST_ASSERT(msg_data != NULL, "Failed to allocate message metadata");
                msg_data->enqueue_ts = test_clock();
                msg_data->msg_seq = i;
                enqueue_deadline_us =
                    msg_data->enqueue_ts +
                    ((int64_t)config->enqueue_retry_ms * 1000);

                while (1) {
                        rd_kafka_resp_err_t err;
                        if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                                             payload, config->message_size, NULL, 0,
                                             msg_data) != -1) {
                                enqueued = rd_true;
                                break;
                        }

                        err = rd_kafka_last_error();
                        if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL ||
                            config->enqueue_retry_ms == 0 ||
                            test_clock() >= enqueue_deadline_us)
                                break;

                        rd_kafka_poll(rk, 50);
                }

                if (enqueued) {
                        (*messages_sent)++;
                } else {
                        (*messages_failed)++;
                        free(msg_data);
                }

                if (mode == BENCH_MODE_CONTROLLED_RATE)
                        next_send_ts += interval_us;

                if ((i % poll_every) == 0)
                        rd_kafka_poll(rk, 0);
        }
}

static void print_phase_results(const phase_stats_t *phase) {
        TEST_SAY("\nResults (%s):\n",
                 bench_producer_profile_name(phase->producer_profile));
        TEST_SAY("  Duration: %.2f sec\n", phase->duration_sec);
        TEST_SAY("  Attempted: %d\n", phase->messages_attempted);
        TEST_SAY("  Accepted: %d (enqueue_failed: %d)\n",
                 phase->messages_sent, phase->messages_failed);
        TEST_SAY("  Delivery reports: success=%d failed=%d total=%d undelivered=%d\n",
                 phase->dr_success, phase->dr_failed, phase->dr_total,
                 phase->messages_undelivered);

        if (phase->mode == BENCH_MODE_MAX_THROUGHPUT) {
                TEST_SAY("  Accepted throughput: %.1f msgs/sec (%.2f MB/sec)\n",
                         phase->throughput_msgs_sec, phase->throughput_mb_sec);
        } else {
                TEST_SAY("  Actual rate: %.1f msgs/sec (target: %d)\n",
                         phase->throughput_msgs_sec, phase->rate_msgs_sec);
                TEST_SAY("  Accepted throughput: %.2f MB/sec\n",
                         phase->throughput_mb_sec);
        }

        TEST_SAY("  Produce requests: %lld (%.1f accepted msgs/produce req)\n",
                 (long long)phase->request_count,
                 msgs_per_req(phase->messages_sent, phase->request_count));
        TEST_SAY("  Total requests (all API keys): %lld\n",
                 (long long)phase->total_request_count);
        TEST_SAY("  Latency (successful DR only): p50=%.2fms, p95=%.2fms, p99=%.2fms\n",
                 phase->latency_p50_ms, phase->latency_p95_ms,
                 phase->latency_p99_ms);
        TEST_SAY("  Multibatch: partitions/req p50=%.2f avg=%.2f, "
                 "reqsize avg=%.0f bytes, fill avg=%.1f%%, "
                 "batch_wait p50=%.2fms\n",
                 phase->produce_partitions_p50, phase->produce_partitions_avg,
                 phase->produce_reqsize_avg_bytes,
                 phase->produce_fill_avg_permille / 10.0,
                 phase->batch_wait_p50_us / 1000.0);
}

static void run_phase(bench_config_t *config,
                      const char *topic,
                      bench_mode_t mode,
                      int rate_msgs_sec,
                      bench_producer_profile_t producer_profile) {
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        int64_t start_ts, end_ts;
        int64_t start_produce_requests;
        int64_t start_total_requests;
        int64_t end_produce_requests;
        int64_t end_total_requests;
        int target_messages;
        int messages_sent = 0;
        int messages_failed = 0;
        char *payload;
        phase_stats_t phase_copy;
        double p50, p95, p99;

        if (mode == BENCH_MODE_MAX_THROUGHPUT)
                target_messages = config->max_throughput_messages;
        else
                target_messages = rate_msgs_sec * config->controlled_duration_sec;

        print_phase_header(config, mode, producer_profile, rate_msgs_sec,
                           target_messages);

        rk = create_producer(config, topic, producer_profile);
        rkt = test_create_producer_topic(rk, topic, "acks", "-1", NULL);

        reset_phase_tracking(mode, rate_msgs_sec);
        sample_request_counts(rk, 3, &start_produce_requests,
                              &start_total_requests);

        payload = malloc(config->message_size);
        TEST_ASSERT(payload != NULL, "Failed to allocate payload buffer");
        memset(payload, 'x', config->message_size);

        start_ts = test_clock();
        produce_phase_messages(rk, rkt, config, mode, rate_msgs_sec,
                               target_messages, payload, &messages_sent,
                               &messages_failed);
        free(payload);

        TEST_SAY("Waiting for delivery confirmations...\n");
        flush_until_empty(rk);
        end_ts = test_clock();

        sample_request_counts(rk, 5, &end_produce_requests,
                              &end_total_requests);

        mtx_lock(&bench_ctx->lock);

        compute_latency_percentiles_locked(&p50, &p95, &p99, rd_true);

        phase_stats_t *phase = append_phase_locked();
        phase->mode = mode;
        phase->producer_profile = producer_profile;
        phase->rate_msgs_sec =
            mode == BENCH_MODE_CONTROLLED_RATE ? rate_msgs_sec : 0;
        phase->duration_sec = (double)(end_ts - start_ts) / 1000000.0;
        phase->messages_attempted = target_messages;
        phase->messages_sent = messages_sent;
        phase->messages_failed = messages_failed;
        phase->dr_success = bench_ctx->phase_dr_success;
        phase->dr_failed = bench_ctx->phase_dr_failed;
        phase->dr_total = bench_ctx->phase_dr_total;
        phase->messages_undelivered =
            RD_MAX(0, phase->messages_sent - phase->dr_total);
        phase->throughput_msgs_sec =
            safe_div((double)messages_sent, phase->duration_sec);
        phase->throughput_mb_sec =
            safe_div((double)messages_sent * config->message_size,
                     phase->duration_sec * 1000000.0);
        phase->request_count =
            end_produce_requests - start_produce_requests;
        phase->total_request_count =
            end_total_requests - start_total_requests;
        phase->latency_p50_ms = p50;
        phase->latency_p95_ms = p95;
        phase->latency_p99_ms = p99;
        phase->int_latency_p50 = bench_ctx->last_int_latency_p50;
        phase->int_latency_p95 = bench_ctx->last_int_latency_p95;
        phase->int_latency_p99 = bench_ctx->last_int_latency_p99;
        phase->rtt_p50 = bench_ctx->last_rtt_p50;
        phase->rtt_p95 = bench_ctx->last_rtt_p95;
        phase->rtt_p99 = bench_ctx->last_rtt_p99;
        phase->produce_partitions_p50 = bench_ctx->last_produce_partitions_p50;
        phase->produce_partitions_avg = bench_ctx->last_produce_partitions_avg;
        phase->produce_reqsize_avg_bytes =
            bench_ctx->last_produce_reqsize_avg_bytes;
        phase->produce_fill_avg_permille =
            bench_ctx->last_produce_fill_avg_permille;
        phase->batch_wait_p50_us = bench_ctx->last_batch_wait_p50_us;

        phase_copy = *phase;
        mtx_unlock(&bench_ctx->lock);

        print_phase_results(&phase_copy);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
}

/**
 * Run max throughput benchmark
 */
static void run_max_throughput_phase(bench_config_t *config, const char *topic) {
        run_phase(config, topic, BENCH_MODE_MAX_THROUGHPUT, 0,
                  BENCH_PRODUCER_PROFILE_V2);
}

/**
 * Run controlled rate benchmark
 */
static void run_controlled_rate_phase(bench_config_t *config,
                                      const char *topic,
                                      int rate_msgs_sec,
                                      bench_producer_profile_t producer_profile) {
        run_phase(config, topic, BENCH_MODE_CONTROLLED_RATE, rate_msgs_sec,
                  producer_profile);
}

static const phase_stats_t *
find_controlled_phase(int rate_msgs_sec, bench_producer_profile_t producer_profile) {
        int i;
        for (i = 0; i < bench_ctx->phase_cnt; i++) {
                const phase_stats_t *phase = &bench_ctx->phases[i];
                if (phase->mode == BENCH_MODE_CONTROLLED_RATE &&
                    phase->rate_msgs_sec == rate_msgs_sec &&
                    phase->producer_profile == producer_profile)
                        return phase;
        }

        return NULL;
}

static double pct_delta(double value, double baseline) {
        if (fabs(baseline) < 0.0000001)
                return 0.0;
        return ((value - baseline) / baseline) * 100.0;
}

static const phase_stats_t *find_max_throughput_phase(void) {
        int i;
        for (i = 0; i < bench_ctx->phase_cnt; i++) {
                const phase_stats_t *phase = &bench_ctx->phases[i];
                if (phase->mode == BENCH_MODE_MAX_THROUGHPUT)
                        return phase;
        }

        return NULL;
}

static void write_summary_csv(const bench_config_t *config) {
        int i;
        char summary_path[512];
        FILE *summary_fp;

        snprintf(summary_path, sizeof(summary_path), "%s/summary.csv",
                 config->output_dir);
        summary_fp = fopen(summary_path, "w");
        if (!summary_fp) {
                TEST_WARN("Failed to open %s for writing\n", summary_path);
                return;
        }

        fprintf(summary_fp,
                "mode,producer_profile,rate_msgs_sec,duration_sec,messages_attempted,messages_accepted,"
                "messages_enqueue_failed,dr_success,dr_failed,dr_total,messages_undelivered,"
                "produce_requests_sent,total_requests_sent,msgs_per_produce_req,throughput_msgs_sec,throughput_mb_sec,"
                "latency_p50_ms,latency_p95_ms,latency_p99_ms,"
                "produce_partitions_p50,produce_partitions_avg,"
                "produce_reqsize_avg_bytes,produce_fill_avg_percent,"
                "batch_wait_p50_ms\n");

        for (i = 0; i < bench_ctx->phase_cnt; i++) {
                const phase_stats_t *p = &bench_ctx->phases[i];
                fprintf(summary_fp,
                        "%s,%s,%d,%.2f,%d,%d,%d,%d,%d,%d,%d,%lld,%lld,%.1f,%.1f,%.2f,%.2f,%.2f,%.2f,"
                        "%.2f,%.2f,%.0f,%.2f,%.2f\n",
                        p->mode == BENCH_MODE_MAX_THROUGHPUT ? "max_throughput"
                                                             : "controlled",
                        bench_producer_profile_name(p->producer_profile),
                        p->rate_msgs_sec, p->duration_sec,
                        p->messages_attempted, p->messages_sent,
                        p->messages_failed, p->dr_success, p->dr_failed,
                        p->dr_total, p->messages_undelivered,
                        (long long)p->request_count,
                        (long long)p->total_request_count,
                        msgs_per_req(p->messages_sent, p->request_count),
                        p->throughput_msgs_sec, p->throughput_mb_sec,
                        p->latency_p50_ms, p->latency_p95_ms, p->latency_p99_ms,
                        p->produce_partitions_p50, p->produce_partitions_avg,
                        p->produce_reqsize_avg_bytes,
                        p->produce_fill_avg_permille / 10.0,
                        p->batch_wait_p50_us / 1000.0);
        }

        fclose(summary_fp);
        TEST_SAY("Wrote summary to %s\n", summary_path);
}

static void write_sampled_latencies_csv(const bench_config_t *config) {
        int i;
        char latencies_path[512];
        FILE *latencies_fp;

        snprintf(latencies_path, sizeof(latencies_path),
                 "%s/per_message_latencies.csv", config->output_dir);
        latencies_fp = fopen(latencies_path, "w");
        if (!latencies_fp) {
                TEST_WARN("Failed to open %s for writing\n", latencies_path);
                return;
        }

        fprintf(latencies_fp,
                "msg_seq,enqueue_timestamp_us,delivery_timestamp_us,latency_us,partition,dr_err\n");

        mtx_lock(&bench_ctx->lock);
        for (i = 0; i < bench_ctx->sampled_lat_cnt; i++) {
                const msg_latency_t *m = &bench_ctx->sampled_latencies[i];
                fprintf(latencies_fp, "%d,%lld,%lld,%lld,%d,%d\n", m->msg_seq,
                        (long long)m->enqueue_ts, (long long)m->delivery_ts,
                        (long long)(m->delivery_ts - m->enqueue_ts),
                        m->partition, (int)m->dr_err);
        }
        mtx_unlock(&bench_ctx->lock);

        fclose(latencies_fp);
        TEST_SAY("Wrote sampled latencies to %s\n", latencies_path);
}

static void print_max_throughput_summary(void) {
        const phase_stats_t *p = find_max_throughput_phase();
        if (!p)
                return;

        TEST_SAY("MAX THROUGHPUT MODE:\n");
        TEST_SAY("  Attempted: %d\n", p->messages_attempted);
        TEST_SAY("  Accepted: %d (enqueue_failed: %d)\n", p->messages_sent,
                 p->messages_failed);
        TEST_SAY("  Delivery reports: success=%d failed=%d total=%d undelivered=%d\n",
                 p->dr_success, p->dr_failed, p->dr_total,
                 p->messages_undelivered);
        TEST_SAY("  Duration: %.2f sec\n", p->duration_sec);
        TEST_SAY("  Accepted throughput: %.1f msgs/sec (%.2f MB/sec)\n",
                 p->throughput_msgs_sec, p->throughput_mb_sec);
        TEST_SAY("  Produce requests: %lld (%.1f accepted msgs/produce req)\n",
                 (long long)p->request_count,
                 msgs_per_req(p->messages_sent, p->request_count));
        TEST_SAY("  Total requests (all API keys): %lld\n",
                 (long long)p->total_request_count);
        TEST_SAY("  Latency (successful DR only): p50=%.2fms, p95=%.2fms, p99=%.2fms\n",
                 p->latency_p50_ms, p->latency_p95_ms, p->latency_p99_ms);
        TEST_SAY("  Multibatch: partitions/req p50=%.2f avg=%.2f, "
                 "reqsize avg=%.0f bytes, fill avg=%.1f%%, "
                 "batch_wait p50=%.2fms\n",
                 p->produce_partitions_p50, p->produce_partitions_avg,
                 p->produce_reqsize_avg_bytes, p->produce_fill_avg_permille / 10.0,
                 p->batch_wait_p50_us / 1000.0);
        TEST_SAY("\n");
}

static void print_controlled_rate_table(void) {
        int i;
        TEST_SAY("CONTROLLED RATE MODE RESULTS:\n");
        TEST_SAY("%-10s %-16s %-8s %-10s %-10s %-10s %-10s %-11s %-10s %-10s %-12s %-12s\n",
                 "Rate", "Profile", "Dur(s)", "Attempted", "Accepted",
                 "Delivered", "Undeliv", "ProdReqs", "msgs/req", "p50 lat",
                 "p99 lat", "batchwait");
        TEST_SAY("---------- ---------------- -------- ---------- ---------- ---------- ---------- ----------- ---------- ---------- ------------ ------------\n");

        for (i = 0; i < bench_ctx->phase_cnt; i++) {
                const phase_stats_t *p = &bench_ctx->phases[i];

                if (p->mode != BENCH_MODE_CONTROLLED_RATE)
                        continue;

                TEST_SAY("%-10d %-16s %-8.1f %-10d %-10d %-10d %-10d %-11lld %-10.1f %-10.2f %-12.2f %-12.2f\n",
                         p->rate_msgs_sec,
                         bench_producer_profile_name(p->producer_profile),
                         p->duration_sec, p->messages_attempted,
                         p->messages_sent,
                         p->dr_success,
                         p->messages_undelivered,
                         (long long)p->request_count,
                         msgs_per_req(p->messages_sent, p->request_count),
                         p->latency_p50_ms,
                         p->latency_p99_ms,
                         p->batch_wait_p50_us / 1000.0);
        }
        TEST_SAY("\n");
}

static void print_controlled_rate_comparison(const bench_config_t *config) {
        int i;

        TEST_SAY("CONTROLLED RATE COMPARISON (baseline: v0)\n");
        TEST_SAY("%-10s %-16s %-18s %-18s %-18s\n", "Rate", "Variant",
                 "Throughput delta", "p99 latency delta", "msgs/req delta");
        TEST_SAY("%-10s %-16s %-18s %-18s %-18s\n", "(msg/s)", "", "(%)",
                 "(%)", "(%)");
        TEST_SAY("---------- ---------------- ------------------ ------------------ "
                 "------------------\n");

        for (i = 0; i < config->controlled_rate_cnt; i++) {
                int rate = config->controlled_rates[i];
                const phase_stats_t *v0 =
                    find_controlled_phase(rate, BENCH_PRODUCER_PROFILE_V0);
                const phase_stats_t *v0_multibatch = find_controlled_phase(
                    rate, BENCH_PRODUCER_PROFILE_V0_MULTIBATCH);
                const phase_stats_t *v2 =
                    find_controlled_phase(rate, BENCH_PRODUCER_PROFILE_V2);
                double v0_msgs_per_req;

                if (!v0)
                        continue;

                v0_msgs_per_req = msgs_per_req(v0->messages_sent, v0->request_count);

                if (v0_multibatch) {
                        TEST_SAY("%-10d %-16s %+17.2f%% %+17.2f%% %+17.2f%%\n", rate,
                                 "v0+multibatch",
                                 pct_delta(v0_multibatch->throughput_msgs_sec,
                                           v0->throughput_msgs_sec),
                                 pct_delta(v0_multibatch->latency_p99_ms,
                                           v0->latency_p99_ms),
                                 pct_delta(msgs_per_req(v0_multibatch->messages_sent,
                                                        v0_multibatch->request_count),
                                           v0_msgs_per_req));
                }

                if (v2) {
                        TEST_SAY("%-10d %-16s %+17.2f%% %+17.2f%% %+17.2f%%\n", rate,
                                 "v2",
                                 pct_delta(v2->throughput_msgs_sec,
                                           v0->throughput_msgs_sec),
                                 pct_delta(v2->latency_p99_ms, v0->latency_p99_ms),
                                 pct_delta(msgs_per_req(v2->messages_sent,
                                                        v2->request_count),
                                           v0_msgs_per_req));
                }
        }
        TEST_SAY("\n");
}

static void print_human_readable_summary(const bench_config_t *config) {
        TEST_SAY("\n");
        TEST_SAY("================================================================================\n");
        TEST_SAY("                       BENCHMARK RESULTS SUMMARY\n");
        TEST_SAY("================================================================================\n");
        TEST_SAY("Configuration:\n");
        TEST_SAY("  Partitions: %d\n", config->partition_cnt);
        TEST_SAY("  Message size: %d bytes\n", config->message_size);
        TEST_SAY("  Output directory: %s\n", config->output_dir);
        TEST_SAY("\n");

        if (!config->skip_max_throughput)
                print_max_throughput_summary();

        if (!config->skip_controlled) {
                print_controlled_rate_table();
                print_controlled_rate_comparison(config);
        }

        TEST_SAY("================================================================================\n");
        TEST_SAY("Results saved to: %s/\n", config->output_dir);
        TEST_SAY("================================================================================\n");
}

/**
 * Output results to CSV and summary
 */
static void output_results(bench_config_t *config) {
        ensure_output_dir(config->output_dir);
        write_summary_csv(config);
        write_sampled_latencies_csv(config);
        print_human_readable_summary(config);
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
 *   BENCH_BATCH_NUM_MESSAGES      batch.num.messages (default: 100000)
 *   BENCH_BATCH_SIZE              batch.size (default: 1000000)
 *   BENCH_ENQUEUE_RETRY_MS        Retry budget per message on QUEUE_FULL (default: 1000)
 *   BENCH_OUTPUT_DIR              Output directory (default: ./benchmark_output)
 *   BENCH_SKIP_MAX_THROUGHPUT     Set to "1" to skip max throughput phase
 *   BENCH_SKIP_CONTROLLED         Set to "1" to skip controlled rate phases
 *   BENCH_V1_LINGER_MS            v1 linger.ms for v0/v0+multibatch profiles (default: 500)
 */
static int parse_env_int_in_range(const char *name,
                                  const char *value,
                                  int min_value,
                                  int max_value) {
        char *endptr = NULL;
        long parsed;

        errno = 0;
        parsed = strtol(value, &endptr, 10);
        while (endptr && *endptr != '\0' && isspace((unsigned char)*endptr))
                endptr++;

        TEST_ASSERT(errno == 0 && endptr != value && endptr && *endptr == '\0',
                    "Invalid integer for %s: \"%s\"", name, value);
        TEST_ASSERT(parsed >= min_value && parsed <= max_value,
                    "Out-of-range value for %s: \"%s\" (expected %d..%d)", name,
                    value, min_value, max_value);

        return (int)parsed;
}

static int parse_env_bool(const char *name, const char *value) {
        return parse_env_int_in_range(name, value, 0, 1);
}

static void parse_controlled_rates(bench_config_t *config, const char *value) {
        char *rates_str = strdup(value);
        char *token;

        TEST_ASSERT(rates_str != NULL,
                    "Failed to allocate memory while parsing BENCH_CONTROLLED_RATES");

        free(config->controlled_rates);
        config->controlled_rates = NULL;
        config->controlled_rate_cnt = 0;

        token = strtok(rates_str, ",");
        while (token) {
                int parsed_rate = parse_env_int_in_range(
                    "BENCH_CONTROLLED_RATES", token, 1, INT_MAX);
                int *new_rates = realloc(
                    config->controlled_rates,
                    sizeof(*new_rates) * (config->controlled_rate_cnt + 1));
                TEST_ASSERT(new_rates != NULL,
                            "Failed to grow controlled rates array");
                config->controlled_rates = new_rates;
                config->controlled_rates[config->controlled_rate_cnt++] =
                    parsed_rate;
                token = strtok(NULL, ",");
        }

        free(rates_str);
        TEST_ASSERT(config->controlled_rate_cnt > 0,
                    "BENCH_CONTROLLED_RATES must include at least one positive rate");
}

static void parse_config(bench_config_t *config) {
        const char *val;

        /* Set defaults */
        config->partition_cnt = 1000;
        config->message_size = 256;
        config->enqueue_retry_ms = 1000;
        config->max_throughput_messages = 100000;
        config->skip_max_throughput = 0;
        config->controlled_duration_sec = 60;
        config->skip_controlled = 0;
        config->output_dir = strdup("./benchmark_output");
        TEST_ASSERT(config->output_dir != NULL,
                    "Failed to allocate default output_dir");

        /* Default controlled rates */
        config->controlled_rate_cnt = 3;
        config->controlled_rates = malloc(sizeof(int) * 3);
        TEST_ASSERT(config->controlled_rates != NULL,
                    "Failed to allocate default controlled rates");
        config->controlled_rates[0] = 10000;
        config->controlled_rates[1] = 20000;
        config->controlled_rates[2] = 50000;

        /* Override from environment */
        if ((val = test_getenv("MAX_PARTITIONS", NULL)))
                config->partition_cnt =
                    parse_env_int_in_range("MAX_PARTITIONS", val, 1, INT_MAX);

        if ((val = test_getenv("BENCH_MAX_THROUGHPUT_MESSAGES", NULL)))
                config->max_throughput_messages = parse_env_int_in_range(
                    "BENCH_MAX_THROUGHPUT_MESSAGES", val, 1, INT_MAX);

        if ((val = test_getenv("BENCH_CONTROLLED_RATES", NULL)))
                parse_controlled_rates(config, val);

        if ((val = test_getenv("BENCH_CONTROLLED_DURATION", NULL)))
                config->controlled_duration_sec = parse_env_int_in_range(
                    "BENCH_CONTROLLED_DURATION", val, 1, INT_MAX);

        if ((val = test_getenv("BENCH_MESSAGE_SIZE", NULL)))
                config->message_size =
                    parse_env_int_in_range("BENCH_MESSAGE_SIZE", val, 1, INT_MAX);

        if ((val = test_getenv("BENCH_ENQUEUE_RETRY_MS", NULL)))
                config->enqueue_retry_ms = parse_env_int_in_range(
                    "BENCH_ENQUEUE_RETRY_MS", val, 0, INT_MAX);

        if ((val = test_getenv("BENCH_OUTPUT_DIR", NULL))) {
                TEST_ASSERT(val[0] != '\0',
                            "BENCH_OUTPUT_DIR cannot be empty");
                free(config->output_dir);
                config->output_dir = strdup(val);
                TEST_ASSERT(config->output_dir != NULL,
                            "Failed to allocate BENCH_OUTPUT_DIR");
        }

        if ((val = test_getenv("BENCH_SKIP_MAX_THROUGHPUT", NULL)))
                config->skip_max_throughput =
                    parse_env_bool("BENCH_SKIP_MAX_THROUGHPUT", val);

        if ((val = test_getenv("BENCH_SKIP_CONTROLLED", NULL)))
                config->skip_controlled =
                    parse_env_bool("BENCH_SKIP_CONTROLLED", val);

        TEST_ASSERT(!(config->skip_max_throughput && config->skip_controlled),
                    "Both benchmark phases are disabled, enable at least one phase");

        if (!config->skip_controlled) {
                TEST_ASSERT(config->controlled_rate_cnt > 0,
                            "Controlled-rate phase requires at least one rate");
        }
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
        rd_kafka_t *rk_temp =
            create_producer(&config, topic, BENCH_PRODUCER_PROFILE_V2);
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
                        int rate = config.controlled_rates[i];
                        run_controlled_rate_phase(
                            &config, topic, rate, BENCH_PRODUCER_PROFILE_V0);
                        run_controlled_rate_phase(
                            &config, topic, rate,
                            BENCH_PRODUCER_PROFILE_V0_MULTIBATCH);
                        run_controlled_rate_phase(
                            &config, topic, rate, BENCH_PRODUCER_PROFILE_V2);
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
