/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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
 * Dedicated mbv2 perf benchmark.
 *
 * This benchmark is intentionally narrow:
 * - v2 producer engine only
 * - no delivery report callback
 * - no stats callback or periodic stats collection
 * - no per-message latency tracking or CSV output
 *
 * The goal is to make Linux perf profiles point at librdkafka's hot paths,
 * not the benchmark harness.
 *
 * Usage:
 *   ./test-runner 8002
 *
 * Configuration via environment variables:
 *   PERF_BENCH_TOPIC_COUNT            Number of topics (default: 1)
 *   PERF_BENCH_TOPIC_PARTITIONS       Partitions per topic (default: 1000)
 *   PERF_BENCH_WARMUP_MESSAGES        Messages to send before measurement
 *                                     (default: 0)
 *   PERF_BENCH_MESSAGES               Messages to send in measured phase
 *                                     (default: 500000)
 *   PERF_BENCH_MESSAGE_SIZE           Fixed payload size in bytes
 *                                     (default: 256)
 *   PERF_BENCH_MESSAGE_SIZE_MIN       Minimum payload size in bytes
 *                                     (default: PERF_BENCH_MESSAGE_SIZE)
 *   PERF_BENCH_MESSAGE_SIZE_MAX       Maximum payload size in bytes
 *                                     (default: PERF_BENCH_MESSAGE_SIZE)
 *   PERF_BENCH_ENQUEUE_RETRY_MS       Retry budget on QUEUE_FULL
 *                                     (default: 1000)
 *   PERF_BENCH_PRE_MEASURE_SLEEP_MS   Sleep after setup/warmup before measured
 *                                     phase (default: 0)
 *   PERF_BENCH_COMPRESSION            compression.type (default: lz4)
 *   PERF_BENCH_BATCH_NUM_MESSAGES     batch.num.messages (default: 100000)
 *   PERF_BENCH_BATCH_SIZE             batch.size (default: 1000000)
 *   PERF_BENCH_PRODUCE_MAX_PARTITIONS v2 produce.request.max.partitions
 *                                     override (default: unset)
 *   PERF_BENCH_PROFILE_PREFIX         Self-attach perf around measured phase
 *                                     and write <prefix>.data/.report.txt/
 *                                     .script.txt (default: disabled)
 *   PERF_BENCH_PROFILE_FREQ           perf record sample frequency
 *                                     (default: 999)
 *   PERF_BENCH_PROFILE_CALLGRAPH      perf --call-graph mode
 *                                     (default: fp)
 *   PERF_BENCH_PROFILE_ATTACH_WAIT_MS Wait after spawning perf before the
 *                                     measured phase starts (default: 250)
 *
 * Broker-level batching controls:
 *   BROKER_LINGER_MS                  broker.linger.ms (default: 500)
 *   BROKER_BATCH_MAX_BYTES            broker.batch.max.bytes
 *                                     (default: 10000000)
 *
 * Example:
 *   PERF_BENCH_TOPIC_COUNT=8 \
 *   PERF_BENCH_TOPIC_PARTITIONS=64 \
 *   PERF_BENCH_MESSAGES=1000000 \
 *   PERF_BENCH_PROFILE_PREFIX=perf-out/8002-cross-topic \
 *   ./test-runner 8002
 */

#include "test.h"
#include "rdkafka.h"
#include <ctype.h>
#include <errno.h>
#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <limits.h>
#include <string.h>

typedef struct {
        int topic_cnt;
        int partition_cnt;
        int warmup_messages;
        int measured_messages;
        int message_size_min;
        int message_size_max;
        int enqueue_retry_ms;
        int pre_measure_sleep_ms;
        const char *compression_type;
        const char *batch_num_messages;
        const char *batch_size;
        const char *produce_max_partitions;
        const char *broker_linger_ms;
        const char *broker_batch_max_bytes;
        const char *profile_prefix;
        const char *profile_callgraph;
        int profile_freq;
        int profile_attach_wait_ms;
} perf_bench_config_t;

typedef struct {
        const char *label;
        int start_seq;
        int messages_attempted;
        int messages_accepted;
        int enqueue_failures;
        uint64_t bytes_accepted;
        double enqueue_sec;
        double drain_sec;
        double total_sec;
} perf_phase_stats_t;

#ifndef _WIN32
typedef struct {
        const perf_bench_config_t *config;
        pid_t record_pid;
        char *data_path;
        char *report_path;
        char *script_path;
} perf_profile_t;
#endif

static int parse_env_int_in_range(const char *name,
                                  const char *value,
                                  int min_value,
                                  int max_value) {
        char *endptr = NULL;
        long parsed;

        errno  = 0;
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

static double safe_div(double numerator, double denominator) {
        if (denominator > -0.0000001 && denominator < 0.0000001)
                return 0.0;
        return numerator / denominator;
}

static uint32_t mix32(uint32_t x) {
        x ^= x >> 16;
        x *= 0x7feb352dU;
        x ^= x >> 15;
        x *= 0x846ca68bU;
        x ^= x >> 16;
        return x;
}

static size_t message_size_for_seq(const perf_bench_config_t *config, int msg_seq) {
        uint32_t span;

        if (config->message_size_min == config->message_size_max)
                return (size_t)config->message_size_min;

        span = (uint32_t)(config->message_size_max - config->message_size_min) + 1U;

        return (size_t)(config->message_size_min +
                        (int)(mix32((uint32_t)msg_seq) % span));
}

#ifndef _WIN32
static void ensure_dir_exists(const char *dir) {
        struct stat st;

        if (stat(dir, &st) == 0) {
                TEST_ASSERT(S_ISDIR(st.st_mode),
                            "Path exists but is not a directory: %s", dir);
                return;
        }

        TEST_ASSERT(errno == ENOENT, "Failed to stat directory %s: %s", dir,
                    strerror(errno));
        TEST_ASSERT(mkdir(dir, 0755) == 0 || errno == EEXIST,
                    "Failed to create directory %s: %s", dir, strerror(errno));
}

static void ensure_parent_dirs(const char *path) {
        char *copy;
        char *p;

        copy = rd_strdup(path);
        TEST_ASSERT(copy != NULL, "OOM duplicating path");

        for (p = copy + 1; *p != '\0'; p++) {
                if (*p != '/')
                        continue;

                *p = '\0';
                ensure_dir_exists(copy);
                *p = '/';
        }

        rd_free(copy);
}

static char *path_with_suffix(const char *prefix, const char *suffix) {
        size_t prefix_len = strlen(prefix);
        size_t suffix_len = strlen(suffix);
        char *path        = malloc(prefix_len + suffix_len + 1);

        TEST_ASSERT(path != NULL, "OOM allocating perf output path");

        memcpy(path, prefix, prefix_len);
        memcpy(path + prefix_len, suffix, suffix_len + 1);

        return path;
}

static void wait_for_process_success(pid_t pid,
                                     const char *label,
                                     rd_bool_t allow_sigint) {
        int status;
        pid_t r;

        do
                r = waitpid(pid, &status, 0);
        while (r == -1 && errno == EINTR);

        TEST_ASSERT(r == pid, "waitpid(%ld) failed for %s: %s", (long)pid, label,
                    strerror(errno));

        if (WIFSIGNALED(status)) {
                if (allow_sigint && WTERMSIG(status) == SIGINT)
                        return;

                TEST_FAIL("%s terminated by signal %d", label, WTERMSIG(status));
        }

        TEST_ASSERT(WIFEXITED(status), "%s did not exit cleanly", label);
        TEST_ASSERT(WEXITSTATUS(status) == 0, "%s exited with status %d", label,
                    WEXITSTATUS(status));
}

static void assert_process_still_running(pid_t pid, const char *label) {
        int status;
        pid_t r;

        do
                r = waitpid(pid, &status, WNOHANG);
        while (r == -1 && errno == EINTR);

        TEST_ASSERT(r != -1, "waitpid(%ld, WNOHANG) failed for %s: %s",
                    (long)pid, label, strerror(errno));

        if (r == 0)
                return;

        if (WIFSIGNALED(status))
                TEST_FAIL("%s died early from signal %d", label,
                          WTERMSIG(status));

        TEST_FAIL("%s died early with status %d", label,
                  WIFEXITED(status) ? WEXITSTATUS(status) : -1);
}

static void run_process_to_file(const char *label,
                                const char *outfile,
                                char *const argv[]) {
        pid_t pid;

        pid = fork();
        TEST_ASSERT(pid != -1, "fork() failed for %s: %s", label,
                    strerror(errno));

        if (pid == 0) {
                int fd = open(outfile, O_WRONLY | O_CREAT | O_TRUNC, 0644);

                if (fd == -1) {
                        fprintf(stderr, "open(%s) failed for %s: %s\n", outfile,
                                label, strerror(errno));
                        _exit(127);
                }

                if (dup2(fd, STDOUT_FILENO) == -1 ||
                    dup2(fd, STDERR_FILENO) == -1) {
                        fprintf(stderr, "dup2(%s) failed for %s: %s\n", outfile,
                                label, strerror(errno));
                        if (fd > STDERR_FILENO)
                                close(fd);
                        _exit(127);
                }

                if (fd > STDERR_FILENO)
                        close(fd);

                execvp(argv[0], argv);
                fprintf(stderr, "execvp(%s) failed for %s: %s\n", argv[0], label,
                        strerror(errno));
                _exit(127);
        }

        wait_for_process_success(pid, label, rd_false);
}

static void perf_profile_init(perf_profile_t *profile,
                              const perf_bench_config_t *config) {
        memset(profile, 0, sizeof(*profile));
        profile->config    = config;
        profile->record_pid = -1;

        if (!config->profile_prefix)
                return;

        profile->data_path   = path_with_suffix(config->profile_prefix, ".data");
        profile->report_path =
            path_with_suffix(config->profile_prefix, ".report.txt");
        profile->script_path =
            path_with_suffix(config->profile_prefix, ".script.txt");

        ensure_parent_dirs(profile->data_path);
}

static void perf_profile_destroy(perf_profile_t *profile) {
        free(profile->data_path);
        free(profile->report_path);
        free(profile->script_path);
}

static void perf_profile_start(perf_profile_t *profile) {
        char freq_str[32];
        char pid_str[32];
        char *argv[] = {"perf",
                        "record",
                        "-F",
                        freq_str,
                        "-g",
                        "--call-graph",
                        (char *)profile->config->profile_callgraph,
                        "-p",
                        pid_str,
                        "-o",
                        profile->data_path,
                        NULL};

        if (!profile->config->profile_prefix)
                return;

        rd_snprintf(freq_str, sizeof(freq_str), "%d",
                    profile->config->profile_freq);
        rd_snprintf(pid_str, sizeof(pid_str), "%ld", (long)getpid());

        TEST_SAY("\nSelf-attaching perf: %s\n", profile->data_path);

        profile->record_pid = fork();
        TEST_ASSERT(profile->record_pid != -1, "fork() failed for perf record: %s",
                    strerror(errno));

        if (profile->record_pid == 0) {
                execvp(argv[0], argv);
                fprintf(stderr, "execvp(perf) failed: %s\n", strerror(errno));
                _exit(127);
        }

        if (profile->config->profile_attach_wait_ms > 0) {
                usleep((useconds_t)((uint64_t)profile->config->profile_attach_wait_ms *
                                    1000));
        }

        assert_process_still_running(profile->record_pid, "perf record");
}

static void perf_profile_stop(perf_profile_t *profile) {
        if (!profile->config->profile_prefix || profile->record_pid == -1)
                return;

        TEST_SAY("Stopping perf capture\n");

        if (kill(profile->record_pid, SIGINT) == -1 && errno != ESRCH) {
                TEST_FAIL("kill(%ld, SIGINT) failed for perf record: %s",
                          (long)profile->record_pid, strerror(errno));
        }

        wait_for_process_success(profile->record_pid, "perf record", rd_true);
        profile->record_pid = -1;
}

static void perf_profile_write_outputs(perf_profile_t *profile) {
        char *report_argv[] = {"perf",
                               "report",
                               "-i",
                               profile->data_path,
                               "--stdio",
                               "--sort",
                               "comm,dso,symbol",
                               NULL};
        char *script_argv[] = {"perf", "script", "-i", profile->data_path, NULL};

        if (!profile->config->profile_prefix)
                return;

        TEST_SAY("Writing perf report: %s\n", profile->report_path);
        run_process_to_file("perf report", profile->report_path, report_argv);

        TEST_SAY("Writing perf script: %s\n", profile->script_path);
        run_process_to_file("perf script", profile->script_path, script_argv);
}
#endif

static void parse_config(perf_bench_config_t *config) {
        const char *val;

        memset(config, 0, sizeof(*config));

        config->topic_cnt             = 1;
        config->partition_cnt         = 1000;
        config->warmup_messages       = 0;
        config->measured_messages     = 500000;
        config->message_size_min      = 256;
        config->message_size_max      = 256;
        config->enqueue_retry_ms      = 1000;
        config->pre_measure_sleep_ms  = 0;
        config->compression_type      = "lz4";
        config->batch_num_messages    = "100000";
        config->batch_size            = "1000000";
        config->produce_max_partitions = NULL;
        config->broker_linger_ms      = "500";
        config->broker_batch_max_bytes = "10000000";
        config->profile_prefix        = NULL;
        config->profile_callgraph     = "fp";
        config->profile_freq          = 999;
        config->profile_attach_wait_ms = 250;

        if ((val = test_getenv("PERF_BENCH_TOPIC_COUNT", NULL)))
                config->topic_cnt = parse_env_int_in_range(
                    "PERF_BENCH_TOPIC_COUNT", val, 1, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_TOPIC_PARTITIONS", NULL)))
                config->partition_cnt = parse_env_int_in_range(
                    "PERF_BENCH_TOPIC_PARTITIONS", val, 1, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_WARMUP_MESSAGES", NULL)))
                config->warmup_messages = parse_env_int_in_range(
                    "PERF_BENCH_WARMUP_MESSAGES", val, 0, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_MESSAGES", NULL)))
                config->measured_messages = parse_env_int_in_range(
                    "PERF_BENCH_MESSAGES", val, 1, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_MESSAGE_SIZE", NULL)))
                config->message_size_min = config->message_size_max =
                    parse_env_int_in_range("PERF_BENCH_MESSAGE_SIZE", val, 1,
                                           INT_MAX);

        if ((val = test_getenv("PERF_BENCH_MESSAGE_SIZE_MIN", NULL)))
                config->message_size_min = parse_env_int_in_range(
                    "PERF_BENCH_MESSAGE_SIZE_MIN", val, 1, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_MESSAGE_SIZE_MAX", NULL)))
                config->message_size_max = parse_env_int_in_range(
                    "PERF_BENCH_MESSAGE_SIZE_MAX", val, 1, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_ENQUEUE_RETRY_MS", NULL)))
                config->enqueue_retry_ms = parse_env_int_in_range(
                    "PERF_BENCH_ENQUEUE_RETRY_MS", val, 0, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_PRE_MEASURE_SLEEP_MS", NULL)))
                config->pre_measure_sleep_ms = parse_env_int_in_range(
                    "PERF_BENCH_PRE_MEASURE_SLEEP_MS", val, 0, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_COMPRESSION", NULL))) {
                TEST_ASSERT(*val != '\0',
                            "PERF_BENCH_COMPRESSION cannot be empty");
                config->compression_type = val;
        }

        if ((val = test_getenv("PERF_BENCH_BATCH_NUM_MESSAGES", NULL))) {
                (void)parse_env_int_in_range("PERF_BENCH_BATCH_NUM_MESSAGES", val,
                                             1, INT_MAX);
                config->batch_num_messages = val;
        }

        if ((val = test_getenv("PERF_BENCH_BATCH_SIZE", NULL))) {
                (void)parse_env_int_in_range("PERF_BENCH_BATCH_SIZE", val, 1,
                                             INT_MAX);
                config->batch_size = val;
        }

        if ((val = test_getenv("PERF_BENCH_PRODUCE_MAX_PARTITIONS", NULL))) {
                (void)parse_env_int_in_range(
                    "PERF_BENCH_PRODUCE_MAX_PARTITIONS", val, 0, INT_MAX);
                config->produce_max_partitions = val;
        }

        if ((val = test_getenv("PERF_BENCH_PROFILE_PREFIX", NULL))) {
                TEST_ASSERT(*val != '\0',
                            "PERF_BENCH_PROFILE_PREFIX cannot be empty");
                config->profile_prefix = val;
        }

        if ((val = test_getenv("PERF_BENCH_PROFILE_FREQ", NULL)))
                config->profile_freq = parse_env_int_in_range(
                    "PERF_BENCH_PROFILE_FREQ", val, 1, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_PROFILE_ATTACH_WAIT_MS", NULL)))
                config->profile_attach_wait_ms = parse_env_int_in_range(
                    "PERF_BENCH_PROFILE_ATTACH_WAIT_MS", val, 0, INT_MAX);

        if ((val = test_getenv("PERF_BENCH_PROFILE_CALLGRAPH", NULL))) {
                TEST_ASSERT(*val != '\0',
                            "PERF_BENCH_PROFILE_CALLGRAPH cannot be empty");
                config->profile_callgraph = val;
        }

        if ((val = test_getenv("BROKER_LINGER_MS", NULL))) {
                (void)parse_env_int_in_range("BROKER_LINGER_MS", val, 0, INT_MAX);
                config->broker_linger_ms = val;
        }

        if ((val = test_getenv("BROKER_BATCH_MAX_BYTES", NULL))) {
                (void)parse_env_int_in_range("BROKER_BATCH_MAX_BYTES", val, -1,
                                             INT_MAX);
                config->broker_batch_max_bytes = val;
        }

        TEST_ASSERT(config->message_size_min <= config->message_size_max,
                    "Invalid message size range: min=%d max=%d",
                    config->message_size_min, config->message_size_max);
}

static void print_config(const perf_bench_config_t *config) {
        TEST_SAY("\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("               MBV2 PERF BENCHMARK (MANUAL)\n");
        TEST_SAY("============================================================\n");
        TEST_SAY("topics=%d partitions/topic=%d total_partitions=%d\n",
                 config->topic_cnt, config->partition_cnt,
                 config->topic_cnt * config->partition_cnt);
        if (config->message_size_min == config->message_size_max) {
                TEST_SAY("warmup_messages=%d measured_messages=%d message_size=%d\n",
                         config->warmup_messages, config->measured_messages,
                         config->message_size_min);
        } else {
                TEST_SAY("warmup_messages=%d measured_messages=%d "
                         "message_size_range=%d..%d (uniform deterministic)\n",
                         config->warmup_messages, config->measured_messages,
                         config->message_size_min, config->message_size_max);
        }
        TEST_SAY("enqueue_retry_ms=%d pre_measure_sleep_ms=%d\n",
                 config->enqueue_retry_ms, config->pre_measure_sleep_ms);
        TEST_SAY("produce.engine=v2 compression.type=%s\n",
                 config->compression_type);
        TEST_SAY("batch.num.messages=%s batch.size=%s\n",
                 config->batch_num_messages, config->batch_size);
        TEST_SAY("produce.request.max.partitions=%s\n",
                 config->produce_max_partitions
                     ? config->produce_max_partitions
                     : "(default)");
        TEST_SAY("broker.linger.ms=%s broker.batch.max.bytes=%s\n",
                 config->broker_linger_ms, config->broker_batch_max_bytes);
        TEST_SAY("callbacks=none stats=disabled\n");
        if (config->profile_prefix) {
                TEST_SAY("self-profile prefix=%s freq=%d callgraph=%s "
                         "attach_wait_ms=%d\n",
                         config->profile_prefix, config->profile_freq,
                         config->profile_callgraph,
                         config->profile_attach_wait_ms);
        }
        TEST_SAY("============================================================\n");
}

static rd_kafka_t *create_producer(const perf_bench_config_t *config) {
        rd_kafka_conf_t *conf;

        test_conf_init(&conf, NULL, 0);

        /* Strip benchmark noise out of the producer path. */
        rd_kafka_conf_set_stats_cb(conf, NULL);
        rd_kafka_conf_set_stats_cb_typed(conf, NULL);

        test_conf_set(conf, "statistics.interval.ms", "0");
        test_conf_set(conf, "produce.engine", "v2");
        test_conf_set(conf, "queue.buffering.max.messages", "1000000");
        test_conf_set(conf, "queue.buffering.max.kbytes", "102400");
        test_conf_set(conf, "compression.type", config->compression_type);
        test_conf_set(conf, "message.max.bytes", "100000000");
        test_conf_set(conf, "max.in.flight", "1");
        test_conf_set(conf, "batch.num.messages", config->batch_num_messages);
        test_conf_set(conf, "batch.size", config->batch_size);
        test_conf_set(conf, "broker.linger.ms", config->broker_linger_ms);
        test_conf_set(conf, "broker.batch.max.bytes",
                      config->broker_batch_max_bytes);

        if (config->produce_max_partitions) {
                test_conf_set(conf, "produce.request.max.partitions",
                              config->produce_max_partitions);
        }

        return test_create_handle(RD_KAFKA_PRODUCER, conf);
}

static void create_topics_and_handles(rd_kafka_t *rk,
                                      const perf_bench_config_t *config,
                                      char ***topic_namesp,
                                      rd_kafka_topic_t ***topic_handlesp) {
        char **topic_names;
        rd_kafka_topic_t **topic_handles;
        const char *base_topic;
        int t;

        topic_names = calloc((size_t)config->topic_cnt, sizeof(*topic_names));
        TEST_ASSERT(topic_names != NULL, "OOM allocating topic names");

        topic_handles =
            calloc((size_t)config->topic_cnt, sizeof(*topic_handles));
        TEST_ASSERT(topic_handles != NULL, "OOM allocating topic handles");

        base_topic = test_mk_topic_name("8002_mbv2_perf", 1);

        for (t = 0; t < config->topic_cnt; t++) {
                char topic_name[512];

                rd_snprintf(topic_name, sizeof(topic_name), "%s_t%d", base_topic,
                            t);
                topic_names[t] = rd_strdup(topic_name);
                TEST_ASSERT(topic_names[t] != NULL, "OOM duplicating topic name");

                test_create_topic_wait_exists(rk, topic_names[t],
                                              config->partition_cnt, 1, 30000);

                topic_handles[t] = test_create_producer_topic(rk, topic_names[t],
                                                              NULL);
        }

        *topic_namesp   = topic_names;
        *topic_handlesp = topic_handles;
}

static void destroy_topics_and_handles(const perf_bench_config_t *config,
                                       char **topic_names,
                                       rd_kafka_topic_t **topic_handles) {
        int t;

        if (!topic_names && !topic_handles)
                return;

        for (t = 0; t < config->topic_cnt; t++) {
                if (topic_handles && topic_handles[t])
                        rd_kafka_topic_destroy(topic_handles[t]);
                if (topic_names && topic_names[t])
                        rd_free(topic_names[t]);
        }

        if (topic_handles)
                free(topic_handles);
        if (topic_names)
                free(topic_names);
}

static void flush_until_empty(rd_kafka_t *rk) {
        int flush_attempt = 0;
        int outq_len;
        rd_kafka_resp_err_t flush_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        int64_t flush_deadline_us =
            test_clock() + ((int64_t)tmout_multip(300000) * 1000);

        while ((outq_len = rd_kafka_outq_len(rk)) > 0) {
                flush_err = rd_kafka_flush(rk, 10000);
                flush_attempt++;
                outq_len = rd_kafka_outq_len(rk);

                if (outq_len == 0)
                        return;

                if ((flush_attempt % 6) == 0) {
                        TEST_SAY("Flush waiting: outq_len=%d attempt=%d last=%s\n",
                                 outq_len, flush_attempt,
                                 rd_kafka_err2str(flush_err));
                }

                TEST_ASSERT(test_clock() < flush_deadline_us,
                            "Flush did not complete: outq_len=%d attempts=%d "
                            "last=%s",
                            outq_len, flush_attempt,
                            rd_kafka_err2str(flush_err));
        }
}

static perf_phase_stats_t run_phase(rd_kafka_t *rk,
                                    rd_kafka_topic_t **topic_handles,
                                    const perf_bench_config_t *config,
                                    const char *label,
                                    int start_seq,
                                    int message_count) {
        perf_phase_stats_t stats;
        char *payload;
        int i;
        int64_t enqueue_start_ts;
        int64_t enqueue_end_ts;
        int64_t end_ts;

        memset(&stats, 0, sizeof(stats));
        stats.label             = label;
        stats.start_seq         = start_seq;
        stats.messages_attempted = message_count;

        if (message_count == 0)
                return stats;

        payload = malloc((size_t)config->message_size_max);
        TEST_ASSERT(payload != NULL, "OOM allocating payload");
        memset(payload, 'x', (size_t)config->message_size_max);

        TEST_SAY("\n[%s] Producing %d messages\n", label, message_count);

        enqueue_start_ts = test_clock();

        for (i = 0; i < message_count; i++) {
                int msg_seq = start_seq + i;
                int topic_idx = msg_seq % config->topic_cnt;
                int32_t partition =
                    (int32_t)((msg_seq / config->topic_cnt) %
                              config->partition_cnt);
                size_t message_size = message_size_for_seq(config, msg_seq);
                int64_t enqueue_deadline_us =
                    test_clock() + ((int64_t)config->enqueue_retry_ms * 1000);

                while (1) {
                        rd_kafka_resp_err_t err;

                        if (rd_kafka_produce(topic_handles[topic_idx], partition,
                                             RD_KAFKA_MSG_F_COPY, payload,
                                             message_size, NULL, 0,
                                             NULL) != -1) {
                                stats.messages_accepted++;
                                stats.bytes_accepted += (uint64_t)message_size;
                                break;
                        }

                        err = rd_kafka_last_error();
                        if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL ||
                            config->enqueue_retry_ms == 0 ||
                            test_clock() >= enqueue_deadline_us) {
                                stats.enqueue_failures++;
                                break;
                        }

                        rd_kafka_poll(rk, 50);
                }
        }

        enqueue_end_ts    = test_clock();
        stats.enqueue_sec = (double)(enqueue_end_ts - enqueue_start_ts) / 1000000.0;

        TEST_SAY("[%s] Flushing %d accepted messages\n", label,
                 stats.messages_accepted);

        flush_until_empty(rk);

        end_ts           = test_clock();
        stats.total_sec  = (double)(end_ts - enqueue_start_ts) / 1000000.0;
        stats.drain_sec  = (double)(end_ts - enqueue_end_ts) / 1000000.0;

        free(payload);
        return stats;
}

static void print_phase_results(const perf_phase_stats_t *stats) {
        double bytes_total = (double)stats->bytes_accepted;

        TEST_SAY("[%s] attempted=%d accepted=%d enqueue_failed=%d\n", stats->label,
                 stats->messages_attempted, stats->messages_accepted,
                 stats->enqueue_failures);
        TEST_SAY("[%s] bytes_accepted=%" PRIu64 " avg_message_size=%.1f\n",
                 stats->label, stats->bytes_accepted,
                 safe_div(bytes_total, (double)stats->messages_accepted));
        TEST_SAY("[%s] enqueue=%.3fs drain=%.3fs total=%.3fs\n", stats->label,
                 stats->enqueue_sec, stats->drain_sec, stats->total_sec);
        TEST_SAY("[%s] enqueue_throughput=%.1f msgs/s total_throughput=%.1f "
                 "msgs/s total_throughput_mb=%.2f MB/s\n",
                 stats->label,
                 safe_div((double)stats->messages_accepted, stats->enqueue_sec),
                 safe_div((double)stats->messages_accepted, stats->total_sec),
                 safe_div(bytes_total, stats->total_sec * 1000000.0));
}

int main_8002_multibatch_perf(int argc, char **argv) {
        perf_bench_config_t config;
        rd_kafka_t *rk;
        char **topic_names         = NULL;
        rd_kafka_topic_t **handles = NULL;
        perf_phase_stats_t warmup_stats;
        perf_phase_stats_t measured_stats;
#ifndef _WIN32
        perf_profile_t profile;
#endif

        (void)argc;
        (void)argv;

        test_timeout_set(1800);
        parse_config(&config);
        print_config(&config);

#ifdef _WIN32
        TEST_ASSERT(!config.profile_prefix,
                    "PERF_BENCH_PROFILE_PREFIX is not supported on Windows");
#else
        perf_profile_init(&profile, &config);
#endif

        rk = create_producer(&config);
        create_topics_and_handles(rk, &config, &topic_names, &handles);

        if (config.warmup_messages > 0) {
                warmup_stats = run_phase(rk, handles, &config, "warmup", 0,
                                         config.warmup_messages);
                print_phase_results(&warmup_stats);
        }

        if (config.pre_measure_sleep_ms > 0) {
                TEST_SAY("\nSleeping %dms before measured phase.\n",
                         config.pre_measure_sleep_ms);
                usleep((useconds_t)((uint64_t)config.pre_measure_sleep_ms * 1000));
        }

#ifndef _WIN32
        perf_profile_start(&profile);
#endif
        measured_stats = run_phase(rk, handles, &config, "measured",
                                   config.warmup_messages,
                                   config.measured_messages);
#ifndef _WIN32
        perf_profile_stop(&profile);
        perf_profile_write_outputs(&profile);
#endif
        print_phase_results(&measured_stats);

        destroy_topics_and_handles(&config, topic_names, handles);
        rd_kafka_destroy(rk);

#ifndef _WIN32
        perf_profile_destroy(&profile);
#endif

        return 0;
}
