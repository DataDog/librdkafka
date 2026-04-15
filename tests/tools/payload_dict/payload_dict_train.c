/*
 * librdkafka - Apache Kafka C library
 *
 * Train and evaluate zstd dictionaries from the payload corpus format used by
 * payload dictionary experiments.
 */

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <zdict.h>
#include <zstd.h>

#include "../../corpus_utils.h"

#define TRAIN_PERCENT_DEFAULT 80U
#define COMPRESSION_LEVEL_DEFAULT 3
#define DICT_SIZES_DEFAULT "16384,32768,65536,102400"
#define RNG_SEED_DEFAULT 1ULL
#define MAX_DICT_CANDIDATES 16U

typedef struct sample_ref_s {
        const uint8_t *data;
        size_t len;
} sample_ref_t;

typedef struct eval_stats_s {
        uint64_t raw_bytes;
        uint64_t no_dict_bytes;
        uint64_t dict_bytes;
        uint64_t better_count;
        uint64_t worse_count;
        uint64_t equal_count;
} eval_stats_t;

typedef struct train_opts_s {
        const char *corpus_path;
        const char *output_dir;
        corpus_message_format_t input_format;
        unsigned train_percent;
        int compression_level;
        uint64_t seed;
        size_t dict_sizes[MAX_DICT_CANDIDATES];
        size_t dict_size_cnt;
} train_opts_t;

static void usage(const char *argv0) {
        fprintf(stderr,
                "Usage: %s [options] CORPUS OUTPUT_DIR\n"
                "\n"
                "Options:\n"
                "  --sizes S1,S2,...     Dictionary sizes to try (default: %s)\n"
                "  --input-format F      message or partition-dump "
                "(default: message)\n"
                "  --train-percent N     Percent of samples used for training "
                "(default: %u)\n"
                "  --level N             Zstd compression level for evaluation "
                "(default: %d)\n"
                "  --seed N              Shuffle seed (default: %" PRIu64 ")\n",
                argv0, DICT_SIZES_DEFAULT, TRAIN_PERCENT_DEFAULT,
                COMPRESSION_LEVEL_DEFAULT, (uint64_t)RNG_SEED_DEFAULT);
}

static int parse_u64(const char *s, uint64_t *out) {
        char *end = NULL;
        unsigned long long v;

        errno = 0;
        v     = strtoull(s, &end, 10);
        if (errno != 0 || !end || *end != '\0')
                return -1;

        *out = (uint64_t)v;
        return 0;
}

static int parse_sizes(const char *arg, train_opts_t *opts) {
        char *copy;
        char *token;
        char *saveptr = NULL;

        copy = strdup(arg);
        if (!copy)
                return -1;

        opts->dict_size_cnt = 0;
        for (token = strtok_r(copy, ",", &saveptr); token;
             token = strtok_r(NULL, ",", &saveptr)) {
                uint64_t size = 0;

                if (opts->dict_size_cnt == MAX_DICT_CANDIDATES ||
                    parse_u64(token, &size) != 0 || size == 0 ||
                    size > SIZE_MAX) {
                        free(copy);
                        return -1;
                }

                opts->dict_sizes[opts->dict_size_cnt++] = (size_t)size;
        }

        free(copy);
        return opts->dict_size_cnt > 0 ? 0 : -1;
}

static int parse_opts(int argc, char **argv, train_opts_t *opts) {
        int i;
        uint64_t tmp = 0;

        memset(opts, 0, sizeof(*opts));
        opts->input_format      = CORPUS_MESSAGE_FORMAT_COUNTED;
        opts->train_percent     = TRAIN_PERCENT_DEFAULT;
        opts->compression_level = COMPRESSION_LEVEL_DEFAULT;
        opts->seed              = RNG_SEED_DEFAULT;

        if (parse_sizes(DICT_SIZES_DEFAULT, opts) != 0)
                return -1;

        for (i = 1; i < argc; i++) {
                if (!strcmp(argv[i], "--sizes")) {
                        if (++i == argc || parse_sizes(argv[i], opts) != 0)
                                return -1;
                } else if (!strcmp(argv[i], "--input-format")) {
                        if (++i == argc ||
                            corpus_message_format_parse(argv[i],
                                                        &opts->input_format) !=
                                0)
                                return -1;
                } else if (!strcmp(argv[i], "--train-percent")) {
                        if (++i == argc || parse_u64(argv[i], &tmp) != 0 ||
                            tmp == 0 || tmp >= 100)
                                return -1;
                        opts->train_percent = (unsigned)tmp;
                } else if (!strcmp(argv[i], "--level")) {
                        if (++i == argc || parse_u64(argv[i], &tmp) != 0 ||
                            tmp > INT_MAX)
                                return -1;
                        opts->compression_level = (int)tmp;
                } else if (!strcmp(argv[i], "--seed")) {
                        if (++i == argc || parse_u64(argv[i], &tmp) != 0)
                                return -1;
                        opts->seed = tmp;
                } else if (argv[i][0] == '-') {
                        return -1;
                } else if (!opts->corpus_path) {
                        opts->corpus_path = argv[i];
                } else if (!opts->output_dir) {
                        opts->output_dir = argv[i];
                } else {
                        return -1;
                }
        }

        return (opts->corpus_path && opts->output_dir) ? 0 : -1;
}

static uint64_t rng_next(uint64_t *state) {
        uint64_t x = *state ? *state : 0x9e3779b97f4a7c15ULL;

        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *state = x;
        return x * 2685821657736338717ULL;
}

static void shuffle_samples(sample_ref_t *samples, size_t count, uint64_t seed) {
        uint64_t state = seed;
        size_t i;

        if (count < 2)
                return;

        for (i = count - 1; i > 0; i--) {
                size_t j = (size_t)(rng_next(&state) % (uint64_t)(i + 1));
                sample_ref_t tmp = samples[i];
                samples[i]       = samples[j];
                samples[j]       = tmp;
        }
}

static int ensure_output_dir(const char *path) {
        struct stat st;

        if (stat(path, &st) == 0)
                return S_ISDIR(st.st_mode) ? 0 : -1;

        if (errno != ENOENT)
                return -1;

        return mkdir(path, 0755);
}

static int load_message_value_samples(const corpus_file_t *corpus_file,
                                      corpus_message_format_t input_format,
                                      sample_ref_t **out_samples,
                                      size_t *out_count,
                                      uint64_t *out_total_bytes,
                                      uint64_t *out_skipped_empty) {
        corpus_reader_t reader;
        corpus_scan_result_t scan;
        sample_ref_t *samples;
        size_t sample_count = 0;
        uint64_t total_bytes = 0;
        uint64_t skipped_empty = 0;
        uint32_t i;

        if (!corpus_scan(corpus_file, input_format, 0, &scan))
                return -1;

        if (!corpus_reader_init(&reader, corpus_file, input_format))
                return -1;

        samples = calloc(scan.message_count ? scan.message_count : 1,
                         sizeof(*samples));
        if (!samples)
                return -1;

        for (i = 0; i < scan.message_count; i++) {
                corpus_msg_t msg;
                int r = corpus_reader_read_one(&reader, &msg);

                if (r != 1) {
                        free(samples);
                        return -1;
                }

                if (msg.value_len == 0) {
                        skipped_empty++;
                        continue;
                }

                samples[sample_count].data = msg.value;
                samples[sample_count].len  = msg.value_len;
                sample_count++;
                total_bytes += msg.value_len;
        }

        *out_samples       = samples;
        *out_count         = sample_count;
        *out_total_bytes   = total_bytes;
        *out_skipped_empty = skipped_empty;
        return 0;
}

static int load_batch_samples(const corpus_file_t *corpus_file,
                              sample_ref_t **out_samples,
                              size_t *out_count,
                              uint64_t *out_total_bytes,
                              uint64_t *out_skipped_empty) {
        corpus_buf_t buf;
        uint32_t sample_count_u32 = 0;
        sample_ref_t *samples;
        size_t sample_count = 0;
        uint64_t total_bytes = 0;
        uint64_t skipped_empty = 0;
        uint32_t i;

        buf.p   = corpus_file->data;
        buf.end = corpus_file->data + corpus_file->len;

        if (!corpus_read_batch_header_mem(&buf, &sample_count_u32))
                return -1;

        samples = calloc(sample_count_u32 ? sample_count_u32 : 1,
                         sizeof(*samples));
        if (!samples)
                return -1;

        for (i = 0; i < sample_count_u32; i++) {
                corpus_sample_t sample;

                if (!corpus_read_one_sample_mem(&buf, &sample)) {
                        free(samples);
                        return -1;
                }

                if (sample.len == 0) {
                        skipped_empty++;
                        continue;
                }

                samples[sample_count].data = sample.data;
                samples[sample_count].len  = sample.len;
                sample_count++;
                total_bytes += sample.len;
        }

        *out_samples       = samples;
        *out_count         = sample_count;
        *out_total_bytes   = total_bytes;
        *out_skipped_empty = skipped_empty;
        return 0;
}

static int load_samples(const corpus_file_t *corpus_file,
                        corpus_message_format_t input_format,
                        sample_ref_t **out_samples,
                        size_t *out_count,
                        uint64_t *out_total_bytes,
                        uint64_t *out_skipped_empty,
                        const char **out_format_name) {
        if (corpus_has_batch_samples(corpus_file)) {
                *out_format_name = "batch-sample-corpus";
                return load_batch_samples(corpus_file, out_samples, out_count,
                                          out_total_bytes, out_skipped_empty);
        }

        *out_format_name =
            input_format == CORPUS_MESSAGE_FORMAT_PARTITION_DUMP
                ? "partition-dump"
                : "message-corpus";
        return load_message_value_samples(corpus_file, input_format, out_samples,
                                          out_count, out_total_bytes,
                                          out_skipped_empty);
}

static int build_training_buffer(const sample_ref_t *samples,
                                 size_t count,
                                 uint8_t **out_buf,
                                 size_t **out_sizes,
                                 size_t *out_count,
                                 size_t *out_total_bytes,
                                 uint64_t *out_skipped_small) {
        size_t total_bytes = 0;
        size_t kept = 0;
        size_t *sizes = NULL;
        uint8_t *buf = NULL;
        uint8_t *dst;
        uint64_t skipped_small = 0;
        size_t i;

        for (i = 0; i < count; i++) {
                if (samples[i].len < 8) {
                        skipped_small++;
                        continue;
                }

                total_bytes += samples[i].len;
                kept++;
        }

        if (kept == 0)
                return -1;

        sizes = malloc(kept * sizeof(*sizes));
        buf   = malloc(total_bytes);
        if (!sizes || !buf) {
                free(sizes);
                free(buf);
                return -1;
        }

        dst  = buf;
        kept = 0;
        for (i = 0; i < count; i++) {
                if (samples[i].len < 8)
                        continue;

                memcpy(dst, samples[i].data, samples[i].len);
                sizes[kept++] = samples[i].len;
                dst += samples[i].len;
        }

        *out_buf         = buf;
        *out_sizes       = sizes;
        *out_count       = kept;
        *out_total_bytes = total_bytes;
        *out_skipped_small = skipped_small;
        return 0;
}

static int write_dict_file(const char *output_dir,
                           size_t requested_size,
                           const void *dict_buf,
                           size_t dict_size) {
        char path[1024];
        FILE *fp;

        snprintf(path, sizeof(path), "%s/dict-%zuk.zstd",
                 output_dir, requested_size / 1024);

        fp = fopen(path, "wb");
        if (!fp)
                return -1;

        if (fwrite(dict_buf, 1, dict_size, fp) != dict_size) {
                fclose(fp);
                return -1;
        }

        if (fclose(fp) != 0)
                return -1;

        return 0;
}

static int evaluate_dictionary(const sample_ref_t *holdout,
                               size_t holdout_count,
                               int compression_level,
                               const void *dict_buf,
                               size_t dict_size,
                               unsigned expected_dict_id,
                               eval_stats_t *stats_out) {
        ZSTD_CCtx *plain_cctx = NULL;
        ZSTD_CCtx *dict_cctx  = NULL;
        ZSTD_DCtx *ddctx      = NULL;
        ZSTD_CDict *cdict     = NULL;
        ZSTD_DDict *ddict     = NULL;
        void *plain_buf       = NULL;
        void *dict_buf_dst    = NULL;
        void *decode_buf      = NULL;
        size_t max_sample_len = 0;
        size_t max_dst_cap    = 0;
        size_t i;
        eval_stats_t stats;

        memset(&stats, 0, sizeof(stats));

        for (i = 0; i < holdout_count; i++) {
                if (holdout[i].len > max_sample_len)
                        max_sample_len = holdout[i].len;
        }

        max_dst_cap = ZSTD_compressBound(max_sample_len);
        if (max_dst_cap == 0)
                return -1;

        plain_buf    = malloc(max_dst_cap);
        dict_buf_dst = malloc(max_dst_cap);
        decode_buf   = malloc(max_sample_len ? max_sample_len : 1);
        plain_cctx   = ZSTD_createCCtx();
        dict_cctx    = ZSTD_createCCtx();
        ddctx        = ZSTD_createDCtx();
        cdict        = ZSTD_createCDict(dict_buf, dict_size, compression_level);
        ddict        = ZSTD_createDDict(dict_buf, dict_size);

        if (!plain_buf || !dict_buf_dst || !decode_buf || !plain_cctx ||
            !dict_cctx || !ddctx || !cdict || !ddict)
                goto fail;

        for (i = 0; i < holdout_count; i++) {
                size_t plain_len;
                size_t dict_len;
                size_t decoded_len;
                unsigned frame_dict_id;

                plain_len = ZSTD_compressCCtx(
                    plain_cctx, plain_buf, max_dst_cap, holdout[i].data,
                    holdout[i].len, compression_level);
                if (ZSTD_isError(plain_len))
                        goto fail;

                dict_len = ZSTD_compress_usingCDict(
                    dict_cctx, dict_buf_dst, max_dst_cap, holdout[i].data,
                    holdout[i].len, cdict);
                if (ZSTD_isError(dict_len))
                        goto fail;

                frame_dict_id = ZSTD_getDictID_fromFrame(dict_buf_dst, dict_len);
                if (frame_dict_id != expected_dict_id) {
                        fprintf(stderr,
                                "frame dictID mismatch: expected %u, got %u\n",
                                expected_dict_id, frame_dict_id);
                        goto fail;
                }

                decoded_len = ZSTD_decompress_usingDDict(
                    ddctx, decode_buf, holdout[i].len, dict_buf_dst, dict_len,
                    ddict);
                if (ZSTD_isError(decoded_len) || decoded_len != holdout[i].len ||
                    memcmp(decode_buf, holdout[i].data, holdout[i].len) != 0) {
                        fprintf(stderr, "dictionary round-trip verification failed\n");
                        goto fail;
                }

                stats.raw_bytes += holdout[i].len;
                stats.no_dict_bytes += plain_len;
                stats.dict_bytes += dict_len;

                if (dict_len < plain_len)
                        stats.better_count++;
                else if (dict_len > plain_len)
                        stats.worse_count++;
                else
                        stats.equal_count++;
        }

        *stats_out = stats;

        ZSTD_freeCDict(cdict);
        ZSTD_freeDDict(ddict);
        ZSTD_freeCCtx(plain_cctx);
        ZSTD_freeCCtx(dict_cctx);
        ZSTD_freeDCtx(ddctx);
        free(plain_buf);
        free(dict_buf_dst);
        free(decode_buf);
        return 0;

fail:
        if (cdict)
                ZSTD_freeCDict(cdict);
        if (ddict)
                ZSTD_freeDDict(ddict);
        if (plain_cctx)
                ZSTD_freeCCtx(plain_cctx);
        if (dict_cctx)
                ZSTD_freeCCtx(dict_cctx);
        if (ddctx)
                ZSTD_freeDCtx(ddctx);
        free(plain_buf);
        free(dict_buf_dst);
        free(decode_buf);
        return -1;
}

static void print_eval(size_t requested_size,
                       size_t actual_size,
                       unsigned dict_id,
                       const eval_stats_t *stats) {
        double no_dict_ratio;
        double dict_ratio;
        double improvement_pct;

        no_dict_ratio =
            stats->raw_bytes ? (double)stats->no_dict_bytes / stats->raw_bytes
                             : 0.0;
        dict_ratio =
            stats->raw_bytes ? (double)stats->dict_bytes / stats->raw_bytes : 0.0;
        improvement_pct = stats->no_dict_bytes
                              ? 100.0 *
                                    ((double)stats->no_dict_bytes -
                                     (double)stats->dict_bytes) /
                                    (double)stats->no_dict_bytes
                              : 0.0;

        printf("dict requested=%zu actual=%zu dictID=%u\n", requested_size,
               actual_size, dict_id);
        printf("  holdout raw_bytes=%" PRIu64 " no_dict_bytes=%" PRIu64
               " dict_bytes=%" PRIu64 "\n",
               stats->raw_bytes, stats->no_dict_bytes, stats->dict_bytes);
        printf("  no_dict_ratio=%.6f dict_ratio=%.6f improvement_vs_no_dict=%.2f%%\n",
               no_dict_ratio, dict_ratio, improvement_pct);
        printf("  better=%" PRIu64 " worse=%" PRIu64 " equal=%" PRIu64 "\n",
               stats->better_count, stats->worse_count, stats->equal_count);
}

int main(int argc, char **argv) {
        train_opts_t opts;
        corpus_file_t corpus_file;
        sample_ref_t *samples = NULL;
        const char *format_name = NULL;
        uint64_t total_value_bytes = 0;
        uint64_t skipped_empty = 0;
        size_t sample_count = 0;
        size_t train_count;
        sample_ref_t *train_samples;
        sample_ref_t *holdout_samples;
        uint8_t *train_buf = NULL;
        size_t *train_sizes = NULL;
        size_t train_sample_count = 0;
        size_t train_total_bytes = 0;
        uint64_t skipped_small = 0;
        size_t i;
        double best_improvement = -1.0;
        size_t best_requested_size = 0;

        if (parse_opts(argc, argv, &opts) != 0) {
                usage(argv[0]);
                return 1;
        }

        if (ensure_output_dir(opts.output_dir) != 0) {
                fprintf(stderr, "failed to create output dir %s: %s\n",
                        opts.output_dir, strerror(errno));
                return 1;
        }

        if (!corpus_file_load(opts.corpus_path, &corpus_file)) {
                fprintf(stderr, "failed to load corpus from %s\n",
                        opts.corpus_path);
                return 1;
        }

        if (load_samples(&corpus_file, opts.input_format, &samples, &sample_count,
                         &total_value_bytes, &skipped_empty,
                         &format_name) != 0) {
                fprintf(stderr, "failed to parse training samples from corpus\n");
                corpus_file_unload(&corpus_file);
                return 1;
        }

        printf("loaded corpus=%s format=%s samples=%zu total_sample_bytes=%" PRIu64
               " skipped_empty=%" PRIu64 "\n",
               opts.corpus_path, format_name, sample_count, total_value_bytes,
               skipped_empty);

        if (sample_count < 10) {
                fprintf(stderr, "need at least 10 non-empty samples\n");
                free(samples);
                corpus_file_unload(&corpus_file);
                return 1;
        }

        shuffle_samples(samples, sample_count, opts.seed);

        train_count = (sample_count * opts.train_percent) / 100U;
        if (train_count == 0 || train_count >= sample_count) {
                fprintf(stderr, "invalid train/holdout split\n");
                free(samples);
                corpus_file_unload(&corpus_file);
                return 1;
        }

        train_samples   = samples;
        holdout_samples = samples + train_count;

        if (build_training_buffer(train_samples, train_count, &train_buf,
                                  &train_sizes, &train_sample_count,
                                  &train_total_bytes, &skipped_small) != 0) {
                fprintf(stderr, "failed to build training buffer\n");
                free(samples);
                corpus_file_unload(&corpus_file);
                return 1;
        }

        printf("train_samples=%zu holdout_samples=%zu train_bytes=%zu "
               "skipped_small_train=%" PRIu64 "\n",
               train_sample_count, sample_count - train_count, train_total_bytes,
               skipped_small);

        for (i = 0; i < opts.dict_size_cnt; i++) {
                size_t requested_size = opts.dict_sizes[i];
                void *dict_buf;
                size_t dict_size;
                unsigned dict_id;
                eval_stats_t stats;
                double improvement_pct;

                dict_buf = malloc(requested_size);
                if (!dict_buf) {
                        fprintf(stderr, "OOM allocating dict buffer for %zu\n",
                                requested_size);
                        continue;
                }

                dict_size = ZDICT_trainFromBuffer(dict_buf, requested_size,
                                                  train_buf, train_sizes,
                                                  (unsigned)train_sample_count);
                if (ZDICT_isError(dict_size)) {
                        fprintf(stderr,
                                "training failed for requested dict size %zu: %s\n",
                                requested_size, ZDICT_getErrorName(dict_size));
                        free(dict_buf);
                        continue;
                }

                dict_id = ZDICT_getDictID(dict_buf, dict_size);
                if (dict_id == 0) {
                        fprintf(stderr,
                                "trained dictionary for size %zu is not conformant\n",
                                requested_size);
                        free(dict_buf);
                        continue;
                }

                if (evaluate_dictionary(holdout_samples, sample_count - train_count,
                                        opts.compression_level, dict_buf,
                                        dict_size, dict_id, &stats) != 0) {
                        fprintf(stderr, "evaluation failed for dict size %zu\n",
                                requested_size);
                        free(dict_buf);
                        continue;
                }

                if (write_dict_file(opts.output_dir, requested_size, dict_buf,
                                    dict_size) != 0) {
                        fprintf(stderr,
                                "failed to write dictionary file for size %zu\n",
                                requested_size);
                        free(dict_buf);
                        continue;
                }

                print_eval(requested_size, dict_size, dict_id, &stats);

                improvement_pct = stats.no_dict_bytes
                                      ? 100.0 *
                                            ((double)stats.no_dict_bytes -
                                             (double)stats.dict_bytes) /
                                            (double)stats.no_dict_bytes
                                      : 0.0;
                if (improvement_pct > best_improvement ||
                    (improvement_pct == best_improvement &&
                     requested_size < best_requested_size)) {
                        best_improvement    = improvement_pct;
                        best_requested_size = requested_size;
                }

                free(dict_buf);
        }

        if (best_improvement >= 0.0) {
                printf("best requested dict size=%zu improvement_vs_no_dict=%.2f%%\n",
                       best_requested_size, best_improvement);
        } else {
                fprintf(stderr, "no dictionary candidate succeeded\n");
        }

        free(train_buf);
        free(train_sizes);
        free(samples);
        corpus_file_unload(&corpus_file);

        return best_improvement >= 0.0 ? 0 : 1;
}
