/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Datadog Inc.
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
 * Microbenchmark for rd_kafka_zstd_compress().
 *
 * The benchmark deliberately bypasses producer queueing and broker I/O so that
 * the measured interval contains only slice traversal, output allocation, and
 * zstd compression.  `--reuse 1` enables the client's ZSTD context pool
 * (`compression.zstd.context.reuse=true`), `--reuse 0` creates and frees a
 * context per compression, so both behaviors run from the same binary.
 *
 * The Makefile links with --wrap for ZSTD_createCStream() and
 * ZSTD_freeCStream(), providing exact context lifecycle counts without relying
 * on a particular allocator or zstd implementation.  This makes the expected
 * change easy to verify:
 *
 *   --reuse 0: context_creates == iterations, context_frees == iterations
 *   --reuse 1: context_creates == 0 after warmup, context_frees == 0
 *
 * The pooled context is freed after the measured interval, outside the timer.
 *
 * Build:
 *   make -C tests zstd_context_benchmark
 *
 * Example:
 *   taskset -c 2 ./tests/zstd_context_benchmark \
 *       --size 262144 --segment-size 4096 --iterations 10000 \
 *       --warmup 1000 --level 3 --pattern records --reuse 1
 */

#include "../src/rdkafka_int.h"
#include "../src/rdkafka_zstd.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zstd.h>


typedef enum {
        PAYLOAD_ZEROS,
        PAYLOAD_RECORDS,
        PAYLOAD_RANDOM,
} payload_pattern_t;

typedef struct {
        size_t size;
        size_t segment_size;
        uint64_t iterations;
        uint64_t warmup_iterations;
        int compression_level;
        payload_pattern_t pattern;
        rd_bool_t reuse;
} benchmark_config_t;

typedef struct {
        uint64_t iterations;
        uint64_t input_bytes;
        uint64_t output_bytes;
        uint64_t checksum;
        uint64_t context_creates;
        uint64_t context_frees;
        double elapsed_sec;
} benchmark_result_t;


static uint64_t context_create_calls;
static uint64_t context_free_calls;


ZSTD_CStream *__real_ZSTD_createCStream(void);
size_t __real_ZSTD_freeCStream(ZSTD_CStream *zcs);


ZSTD_CStream *__wrap_ZSTD_createCStream(void) {
        context_create_calls++;
        return __real_ZSTD_createCStream();
}


size_t __wrap_ZSTD_freeCStream(ZSTD_CStream *zcs) {
        context_free_calls++;
        return __real_ZSTD_freeCStream(zcs);
}


static void usage(const char *argv0) {
        fprintf(stderr,
                "Usage: %s [options]\n"
                "  --size BYTES          Input bytes per compression "
                "(default: 262144)\n"
                "  --segment-size BYTES  rd_buf segment bytes "
                "(default: 4096)\n"
                "  --iterations N        Measured compressions "
                "(default: 10000)\n"
                "  --warmup N            Warmup compressions "
                "(default: 1000)\n"
                "  --level N             zstd compression level "
                "(default: 3)\n"
                "  --pattern NAME        zeros, records, or random "
                "(default: records)\n"
                "  --reuse 0|1           Reuse the zstd context "
                "(default: 1)\n"
                "  --help                Show this help\n",
                argv0);
}


static uint64_t parse_u64(const char *option,
                          const char *value,
                          uint64_t min_value,
                          uint64_t max_value) {
        char *end = NULL;
        unsigned long long parsed;

        errno  = 0;
        parsed = strtoull(value, &end, 10);
        if (errno || end == value || *end != '\0' || parsed < min_value ||
            parsed > max_value) {
                fprintf(stderr, "Invalid %s: %s\n", option, value);
                exit(2);
        }

        return (uint64_t)parsed;
}


static int
parse_int(const char *option, const char *value, int min_value, int max_value) {
        char *end = NULL;
        long parsed;

        errno  = 0;
        parsed = strtol(value, &end, 10);
        if (errno || end == value || *end != '\0' || parsed < min_value ||
            parsed > max_value) {
                fprintf(stderr, "Invalid %s: %s\n", option, value);
                exit(2);
        }

        return (int)parsed;
}


static payload_pattern_t parse_pattern(const char *value) {
        if (!strcmp(value, "zeros"))
                return PAYLOAD_ZEROS;
        if (!strcmp(value, "records"))
                return PAYLOAD_RECORDS;
        if (!strcmp(value, "random"))
                return PAYLOAD_RANDOM;

        fprintf(stderr, "Invalid --pattern: %s\n", value);
        exit(2);
}


static const char *pattern_name(payload_pattern_t pattern) {
        switch (pattern) {
        case PAYLOAD_ZEROS:
                return "zeros";
        case PAYLOAD_RECORDS:
                return "records";
        case PAYLOAD_RANDOM:
                return "random";
        }

        return "unknown";
}


static benchmark_config_t parse_args(int argc, char **argv) {
        benchmark_config_t config = {
            .size              = 256 * 1024,
            .segment_size      = 4 * 1024,
            .iterations        = 10000,
            .warmup_iterations = 1000,
            .compression_level = 3,
            .pattern           = PAYLOAD_RECORDS,
            .reuse             = rd_true,
        };
        int i;

        for (i = 1; i < argc; i++) {
                const char *option = argv[i];

                if (!strcmp(option, "--help")) {
                        usage(argv[0]);
                        exit(0);
                }

                if (i + 1 >= argc) {
                        fprintf(stderr, "Missing value for %s\n", option);
                        usage(argv[0]);
                        exit(2);
                }

                if (!strcmp(option, "--size")) {
                        config.size = (size_t)parse_u64(option, argv[++i], 1,
                                                        (uint64_t)SIZE_MAX);
                } else if (!strcmp(option, "--segment-size")) {
                        config.segment_size = (size_t)parse_u64(
                            option, argv[++i], 1, (uint64_t)SIZE_MAX);
                } else if (!strcmp(option, "--iterations")) {
                        config.iterations =
                            parse_u64(option, argv[++i], 1, UINT64_MAX);
                } else if (!strcmp(option, "--warmup")) {
                        config.warmup_iterations =
                            parse_u64(option, argv[++i], 0, UINT64_MAX);
                } else if (!strcmp(option, "--level")) {
                        config.compression_level =
                            parse_int(option, argv[++i], -131072, 22);
                } else if (!strcmp(option, "--pattern")) {
                        config.pattern = parse_pattern(argv[++i]);
                } else if (!strcmp(option, "--reuse")) {
                        config.reuse =
                            (rd_bool_t)parse_u64(option, argv[++i], 0, 1);
                } else {
                        fprintf(stderr, "Unknown option: %s\n", option);
                        usage(argv[0]);
                        exit(2);
                }
        }

        if (config.segment_size > config.size)
                config.segment_size = config.size;

        return config;
}


static uint32_t xorshift32(uint32_t *state) {
        uint32_t value = *state;

        value ^= value << 13;
        value ^= value >> 17;
        value ^= value << 5;
        *state = value;
        return value;
}


static void
fill_payload(unsigned char *payload, size_t size, payload_pattern_t pattern) {
        uint32_t random_state = 0x9e3779b9U;
        size_t i;

        if (pattern == PAYLOAD_ZEROS) {
                memset(payload, 0, size);
                return;
        }

        if (pattern == PAYLOAD_RANDOM) {
                for (i = 0; i < size; i++)
                        payload[i] = (unsigned char)xorshift32(&random_state);
                return;
        }

        /* Approximate a stream of records with stable field names and values
         * that vary enough to prevent the all-zero best case. */
        for (i = 0; i < size; i++) {
                static const char record[] =
                    "{\"service\":\"prof-analyzer\",\"env\":\"prod\","
                    "\"message\":\"sample payload for zstd\",\"value\":";
                size_t offset = i % 128;

                if (offset < sizeof(record) - 1)
                        payload[i] = (unsigned char)record[offset];
                else if (offset < 120)
                        payload[i] = (unsigned char)('a' + ((i / 128) % 26));
                else
                        payload[i] = (unsigned char)xorshift32(&random_state);
        }
}


static void init_input(rd_buf_t *input,
                       const unsigned char *payload,
                       size_t size,
                       size_t segment_size) {
        size_t segment_count = (size + segment_size - 1) / segment_size;
        size_t offset;

        rd_buf_init(input, segment_count, 0);
        for (offset = 0; offset < size; offset += segment_size) {
                size_t length = RD_MIN(segment_size, size - offset);
                rd_buf_push(input, payload + offset, length, NULL);
        }
}


static double monotonic_seconds(void) {
        struct timespec now;

        if (clock_gettime(CLOCK_MONOTONIC_RAW, &now) != 0) {
                perror("clock_gettime");
                exit(1);
        }

        return (double)now.tv_sec + ((double)now.tv_nsec / 1000000000.0);
}


static benchmark_result_t run_phase(rd_kafka_broker_t *rkb,
                                    const rd_buf_t *input,
                                    const benchmark_config_t *config,
                                    uint64_t iterations,
                                    rd_bool_t measured) {
        benchmark_result_t result = RD_ZERO_INIT;
        double start;
        uint64_t i;

        if (measured) {
                context_create_calls = 0;
                context_free_calls   = 0;
        }

        start = monotonic_seconds();
        for (i = 0; i < iterations; i++) {
                rd_slice_t slice;
                rd_kafka_resp_err_t err;
                void *output      = NULL;
                size_t output_len = 0;

                rd_slice_init_full(&slice, input);
                err = rd_kafka_zstd_compress(rkb, config->compression_level,
                                             &slice, &output, &output_len);
                if (err) {
                        fprintf(stderr,
                                "Compression failed at iteration %" PRIu64
                                ": %s\n",
                                i, rd_kafka_err2str(err));
                        exit(1);
                }

                result.output_bytes += (uint64_t)output_len;
                if (output_len > 0) {
                        const unsigned char *bytes = output;
                        result.checksum ^= (uint64_t)output_len;
                        result.checksum ^= (uint64_t)bytes[0] << 8;
                        result.checksum ^= (uint64_t)bytes[output_len / 2]
                                           << 16;
                        result.checksum ^= (uint64_t)bytes[output_len - 1]
                                           << 24;
                        result.checksum ^= i;
                        result.checksum *= UINT64_C(1099511628211);
                }
                rd_free(output);
        }

        result.elapsed_sec     = monotonic_seconds() - start;
        result.iterations      = iterations;
        result.input_bytes     = (uint64_t)config->size * iterations;
        result.context_creates = context_create_calls;
        result.context_frees   = context_free_calls;
        return result;
}


static void print_result(const benchmark_config_t *config,
                         const benchmark_result_t *result) {
        double ns_per_op =
            result->elapsed_sec * 1000000000.0 / (double)result->iterations;
        double mib_per_sec = ((double)result->input_bytes / (1024.0 * 1024.0)) /
                             result->elapsed_sec;
        double ratio =
            (double)result->output_bytes / (double)result->input_bytes;

        printf("zstd_version=%s\n", ZSTD_versionString());
        printf("input_size_bytes=%" PRIusz "\n", config->size);
        printf("segment_size_bytes=%" PRIusz "\n", config->segment_size);
        printf("segments=%" PRIusz "\n",
               (config->size + config->segment_size - 1) /
                   config->segment_size);
        printf("pattern=%s\n", pattern_name(config->pattern));
        printf("compression_level=%d\n", config->compression_level);
        printf("reuse=%d\n", (int)config->reuse);
        printf("warmup_iterations=%" PRIu64 "\n", config->warmup_iterations);
        printf("iterations=%" PRIu64 "\n", result->iterations);
        printf("elapsed_seconds=%.9f\n", result->elapsed_sec);
        printf("nanoseconds_per_compression=%.2f\n", ns_per_op);
        printf("input_mib_per_second=%.2f\n", mib_per_sec);
        printf("compression_ratio=%.6f\n", ratio);
        printf("context_creates=%" PRIu64 "\n", result->context_creates);
        printf("context_frees=%" PRIu64 "\n", result->context_frees);
        printf("checksum=%" PRIu64 "\n", result->checksum);
}


int main(int argc, char **argv) {
        benchmark_config_t config = parse_args(argc, argv);
        benchmark_result_t result;
        rd_kafka_t rk         = RD_ZERO_INIT;
        rd_kafka_broker_t rkb = RD_ZERO_INIT;
        rd_buf_t input;
        unsigned char *payload;

        payload = malloc(config.size);
        if (!payload) {
                fprintf(stderr, "Unable to allocate %" PRIusz " input bytes\n",
                        config.size);
                return 1;
        }

        fill_payload(payload, config.size, config.pattern);
        init_input(&input, payload, config.size, config.segment_size);

        rkb.rkb_rk = &rk;
        if (config.reuse)
                rd_kafka_zstd_pool_init(&rk.rk_zstd_pool, 1);

        (void)run_phase(&rkb, &input, &config, config.warmup_iterations,
                        rd_false);
        result = run_phase(&rkb, &input, &config, config.iterations, rd_true);

        rd_kafka_zstd_pool_destroy(&rk.rk_zstd_pool);

        print_result(&config, &result);

        rd_buf_destroy(&input);
        free(payload);
        return 0;
}
