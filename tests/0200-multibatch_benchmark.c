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
 * Measures:
 * - Throughput (messages/sec, MB/sec)
 * - Latency distribution (P50, P95, P99, P999)
 * - ProduceRequest count (efficiency)
 * - Per-stage latency breakdown
 *
 * Supports:
 * - Warmup phase
 * - Multiple measurement runs
 * - Different partition counts
 * - JSON output for analysis
 */

#include "test.h"
#include "rdkafka.h"
#include <math.h>

/* Benchmark configuration */
typedef struct {
        int partition_cnt;
        int message_cnt;     /* Per run */
        int message_size;
        int num_runs;        /* Number of measurement runs */
        int warmup_messages; /* Messages for warmup */
        const char *distribution; /* "uniform" or "skewed" */
} bench_config_t;

/* Per-message latency tracking */
typedef struct {
        int64_t enqueue_ts;   /* When message was enqueued */
        int64_t delivery_ts;  /* When delivery was confirmed */
        int partition;
} msg_latency_t;

/* Per-run statistics */
typedef struct {
        int run_number;
        double duration_sec;
        double throughput_msg_sec;
        double throughput_mb_sec;
        int64_t request_count; /* From stats */
        int64_t *latencies;    /* Array of latencies in microseconds */
        int latency_cnt;
        int latency_size;
} run_stats_t;

/* Benchmark context */
typedef struct {
        bench_config_t config;
        run_stats_t *runs;
        int run_cnt;
        msg_latency_t *msg_latencies;
        int msg_lat_cnt;
        int msg_lat_size;
        int64_t last_tx_requests; /* From stats callback */
        mtx_t lock;
} bench_ctx_t;

static bench_ctx_t *bench_ctx = NULL;

/**
 * Stats callback - extract request count
 */
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        /* Parse JSON to extract tx request count
         * For simplicity, we'll look for "tx" field.
         * A proper implementation would use a JSON parser. */
        const char *tx_str = strstr(json, "\"tx\":");
        if (tx_str) {
                int64_t tx_val;
                if (sscanf(tx_str + 5, "%" SCNd64, &tx_val) == 1) {
                        mtx_lock(&bench_ctx->lock);
                        bench_ctx->last_tx_requests = tx_val;
                        mtx_unlock(&bench_ctx->lock);
                }
        }
        return 0;
}

/**
 * Delivery report callback - record latency
 */
static void dr_cb(rd_kafka_t *rk,
                  const rd_kafka_message_t *rkmessage,
                  void *opaque) {
        if (rkmessage->err) {
                TEST_FAIL("Message delivery failed: %s",
                          rd_kafka_err2str(rkmessage->err));
                return;
        }

        int64_t now = test_clock();
        int64_t enqueue_ts = *(int64_t *)rkmessage->_private;

        mtx_lock(&bench_ctx->lock);

        /* Ensure capacity */
        if (bench_ctx->msg_lat_cnt >= bench_ctx->msg_lat_size) {
                bench_ctx->msg_lat_size = bench_ctx->msg_lat_size
                                          ? bench_ctx->msg_lat_size * 2
                                          : 10000;
                bench_ctx->msg_latencies = realloc(
                    bench_ctx->msg_latencies,
                    sizeof(msg_latency_t) * bench_ctx->msg_lat_size);
        }

        /* Record latency */
        bench_ctx->msg_latencies[bench_ctx->msg_lat_cnt].enqueue_ts = enqueue_ts;
        bench_ctx->msg_latencies[bench_ctx->msg_lat_cnt].delivery_ts = now;
        bench_ctx->msg_latencies[bench_ctx->msg_lat_cnt].partition =
            rkmessage->partition;
        bench_ctx->msg_lat_cnt++;

        mtx_unlock(&bench_ctx->lock);

        free(rkmessage->_private);
}

/**
 * Produce messages for a single run
 */
static void produce_run(rd_kafka_t *rk,
                        rd_kafka_topic_t *rkt,
                        bench_config_t *config,
                        int is_warmup) {
        int msgcnt = is_warmup ? config->warmup_messages : config->message_cnt;
        char *payload = malloc(config->message_size);
        memset(payload, 'x', config->message_size);

        if (!is_warmup) {
                /* Reset stats counter */
                mtx_lock(&bench_ctx->lock);
                bench_ctx->last_tx_requests = 0;
                bench_ctx->msg_lat_cnt = 0;
                mtx_unlock(&bench_ctx->lock);
        }

        TEST_SAY("%s: Producing %d messages to %d partitions\n",
                 is_warmup ? "Warmup" : "Measure", msgcnt, config->partition_cnt);

        for (int i = 0; i < msgcnt; i++) {
                int32_t partition;

                /* Partition selection based on distribution */
                if (!strcmp(config->distribution, "skewed")) {
                        /* 80% of messages go to partition 0 */
                        partition = (rand() % 100 < 80) ? 0 : (rand() % config->partition_cnt);
                } else {
                        /* Uniform distribution */
                        partition = i % config->partition_cnt;
                }

                /* Allocate timestamp tracking */
                int64_t *enqueue_ts = malloc(sizeof(int64_t));
                *enqueue_ts = test_clock();

                /* Produce */
                if (rd_kafka_produce(rkt, partition,
                                    RD_KAFKA_MSG_F_COPY,
                                    payload, config->message_size,
                                    NULL, 0,
                                    enqueue_ts) == -1) {
                        TEST_FAIL("Failed to produce message %d: %s",
                                  i, rd_kafka_err2str(rd_kafka_last_error()));
                        free(enqueue_ts);
                }

                /* Poll occasionally to trigger delivery callbacks */
                if (i % 100 == 0)
                        rd_kafka_poll(rk, 0);
        }

        free(payload);
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
 * Comparison function for qsort
 */
static int cmp_int64(const void *a, const void *b) {
        int64_t aa = *(int64_t *)a;
        int64_t bb = *(int64_t *)b;
        return (aa > bb) - (aa < bb);
}

/**
 * Run a single benchmark measurement
 */
static void run_benchmark_once(rd_kafka_t *rk,
                              rd_kafka_topic_t *rkt,
                              bench_config_t *config,
                              int run_number,
                              run_stats_t *stats) {
        int64_t start_ts, end_ts;
        int64_t start_tx_requests;

        TEST_SAY("=== Run %d/%d ===\n", run_number + 1, config->num_runs);

        /* Get initial stats */
        rd_kafka_poll(rk, 100); /* Trigger stats callback */
        mtx_lock(&bench_ctx->lock);
        start_tx_requests = bench_ctx->last_tx_requests;
        mtx_unlock(&bench_ctx->lock);

        /* Start timing */
        start_ts = test_clock();

        /* Produce messages */
        produce_run(rk, rkt, config, 0 /* not warmup */);

        /* Wait for all deliveries */
        TEST_SAY("Waiting for delivery confirmations...\n");
        rd_kafka_flush(rk, 60000);

        /* End timing */
        end_ts = test_clock();

        /* Get final stats */
        rd_kafka_poll(rk, 100); /* Trigger stats callback */
        mtx_lock(&bench_ctx->lock);
        int64_t end_tx_requests = bench_ctx->last_tx_requests;
        mtx_unlock(&bench_ctx->lock);

        /* Calculate statistics */
        stats->run_number = run_number;
        stats->duration_sec = (double)(end_ts - start_ts) / 1000000.0;
        stats->throughput_msg_sec = config->message_cnt / stats->duration_sec;
        stats->throughput_mb_sec =
            (config->message_cnt * config->message_size) /
            stats->duration_sec / 1000000.0;
        stats->request_count = end_tx_requests - start_tx_requests;

        /* Extract latencies */
        mtx_lock(&bench_ctx->lock);
        stats->latency_cnt = bench_ctx->msg_lat_cnt;
        stats->latencies = malloc(sizeof(int64_t) * stats->latency_cnt);
        for (int i = 0; i < stats->latency_cnt; i++) {
                stats->latencies[i] =
                    bench_ctx->msg_latencies[i].delivery_ts -
                    bench_ctx->msg_latencies[i].enqueue_ts;
        }
        mtx_unlock(&bench_ctx->lock);

        /* Sort for percentile calculation */
        qsort(stats->latencies, stats->latency_cnt, sizeof(int64_t), cmp_int64);

        TEST_SAY("Run %d complete: %.1f msg/sec, %.2f MB/sec, %" PRId64 " requests\n",
                 run_number + 1,
                 stats->throughput_msg_sec,
                 stats->throughput_mb_sec,
                 stats->request_count);
}

/**
 * Run complete benchmark suite
 */
static void run_benchmark_suite(bench_config_t *config) {
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_conf_t *topic_conf;
        const char *topic;
        test_timing_t t_total;

        TEST_SAY("\n");
        TEST_SAY("========================================\n");
        TEST_SAY("MultiBatch Benchmark\n");
        TEST_SAY("========================================\n");
        TEST_SAY("Config:\n");
        TEST_SAY("  Partitions: %d\n", config->partition_cnt);
        TEST_SAY("  Messages per run: %d\n", config->message_cnt);
        TEST_SAY("  Message size: %d bytes\n", config->message_size);
        TEST_SAY("  Runs: %d\n", config->num_runs);
        TEST_SAY("  Warmup messages: %d\n", config->warmup_messages);
        TEST_SAY("  Distribution: %s\n", config->distribution);
        TEST_SAY("========================================\n\n");

        /* Create topic with many partitions */
        topic = test_mk_topic_name(__FUNCTION__, 1);

        /* Setup producer */
        test_conf_init(&conf, &topic_conf, 120);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);
        rd_kafka_conf_set_stats_cb(conf, stats_cb);
        test_conf_set(conf, "statistics.interval.ms", "100");
        test_conf_set(conf, "queue.buffering.max.messages", "10000000");
        test_conf_set(conf, "linger.ms", "10");

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

        /* Create topic if it doesn't exist */
        test_create_topic_wait_exists(rk, topic, config->partition_cnt, 1, 10000);

        rkt = test_create_producer_topic(rk, topic, "acks", "1", NULL);

        /* Warmup phase */
        TEST_SAY("=== Warmup Phase ===\n");
        produce_run(rk, rkt, config, 1 /* is_warmup */);
        rd_kafka_flush(rk, 60000);
        TEST_SAY("Warmup complete\n\n");

        /* Measurement runs */
        TIMING_START(&t_total, "Total benchmark time");

        bench_ctx->runs = calloc(config->num_runs, sizeof(run_stats_t));
        bench_ctx->run_cnt = config->num_runs;

        for (int i = 0; i < config->num_runs; i++) {
                run_benchmark_once(rk, rkt, config, i, &bench_ctx->runs[i]);
        }

        TIMING_STOP(&t_total);

        /* Cleanup */
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
}

/**
 * Output results as JSON
 */
static void output_results(bench_config_t *config) {
        /* Calculate aggregate statistics across all runs */
        double throughput_sum = 0, throughput_mb_sum = 0;
        int64_t request_sum = 0;
        int total_latencies = 0;

        for (int i = 0; i < bench_ctx->run_cnt; i++) {
                throughput_sum += bench_ctx->runs[i].throughput_msg_sec;
                throughput_mb_sum += bench_ctx->runs[i].throughput_mb_sec;
                request_sum += bench_ctx->runs[i].request_count;
                total_latencies += bench_ctx->runs[i].latency_cnt;
        }

        double throughput_mean = throughput_sum / bench_ctx->run_cnt;
        double throughput_mb_mean = throughput_mb_sum / bench_ctx->run_cnt;
        double request_mean = (double)request_sum / bench_ctx->run_cnt;

        /* Merge all latencies for overall percentiles */
        int64_t *all_latencies = malloc(sizeof(int64_t) * total_latencies);
        int lat_idx = 0;
        for (int i = 0; i < bench_ctx->run_cnt; i++) {
                memcpy(&all_latencies[lat_idx],
                       bench_ctx->runs[i].latencies,
                       sizeof(int64_t) * bench_ctx->runs[i].latency_cnt);
                lat_idx += bench_ctx->runs[i].latency_cnt;
        }
        qsort(all_latencies, total_latencies, sizeof(int64_t), cmp_int64);

        /* Calculate percentiles (in milliseconds for readability) */
        double p50 = percentile(all_latencies, total_latencies, 50) / 1000.0;
        double p95 = percentile(all_latencies, total_latencies, 95) / 1000.0;
        double p99 = percentile(all_latencies, total_latencies, 99) / 1000.0;
        double p999 = percentile(all_latencies, total_latencies, 99.9) / 1000.0;

        /* Output JSON report */
        TEST_REPORT(
            "{"
            "\"config\": {"
            "  \"partition_cnt\": %d,"
            "  \"message_cnt\": %d,"
            "  \"message_size\": %d,"
            "  \"num_runs\": %d,"
            "  \"distribution\": \"%s\""
            "},"
            "\"results\": {"
            "  \"throughput_msg_sec\": %.1f,"
            "  \"throughput_mb_sec\": %.2f,"
            "  \"request_count_mean\": %.1f,"
            "  \"latency_p50_ms\": %.2f,"
            "  \"latency_p95_ms\": %.2f,"
            "  \"latency_p99_ms\": %.2f,"
            "  \"latency_p999_ms\": %.2f,"
            "  \"messages_per_request\": %.1f"
            "}"
            "}",
            config->partition_cnt,
            config->message_cnt,
            config->message_size,
            config->num_runs,
            config->distribution,
            throughput_mean,
            throughput_mb_mean,
            request_mean,
            p50, p95, p99, p999,
            (double)config->message_cnt / request_mean);

        /* Summary to stderr */
        TEST_SAY("\n");
        TEST_SAY("========================================\n");
        TEST_SAY("Benchmark Results (averaged over %d runs)\n", config->num_runs);
        TEST_SAY("========================================\n");
        TEST_SAY("Throughput: %.1f msg/sec (%.2f MB/sec)\n",
                 throughput_mean, throughput_mb_mean);
        TEST_SAY("Requests: %.1f req/run (%.1f msgs/req)\n",
                 request_mean, (double)config->message_cnt / request_mean);
        TEST_SAY("Latency:\n");
        TEST_SAY("  P50:  %.2f ms\n", p50);
        TEST_SAY("  P95:  %.2f ms\n", p95);
        TEST_SAY("  P99:  %.2f ms\n", p99);
        TEST_SAY("  P999: %.2f ms\n", p999);
        TEST_SAY("========================================\n");

        free(all_latencies);
}

/**
 * Main benchmark entry point
 */
int main_0200_multibatch_benchmark(int argc, char **argv) {
        bench_config_t config = {
            .partition_cnt = 1000,           /* Test with many partitions */
            .message_cnt = test_quick ? 10000 : 100000,
            .message_size = 1024,
            .num_runs = test_quick ? 2 : 5,
            .warmup_messages = test_quick ? 1000 : 10000,
            .distribution = "uniform"
        };

        /* Initialize benchmark context */
        bench_ctx = calloc(1, sizeof(bench_ctx_t));
        bench_ctx->config = config;
        mtx_init(&bench_ctx->lock, mtx_plain);

        /* Run benchmark */
        run_benchmark_suite(&config);

        /* Output results */
        output_results(&config);

        /* Cleanup */
        mtx_destroy(&bench_ctx->lock);
        if (bench_ctx->msg_latencies)
                free(bench_ctx->msg_latencies);
        for (int i = 0; i < bench_ctx->run_cnt; i++) {
                if (bench_ctx->runs[i].latencies)
                        free(bench_ctx->runs[i].latencies);
        }
        if (bench_ctx->runs)
                free(bench_ctx->runs);
        free(bench_ctx);

        return 0;
}
