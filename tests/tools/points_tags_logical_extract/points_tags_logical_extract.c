/*
 * librdkafka - Apache Kafka C library
 *
 * Rewrite the points-tags wrapped payload corpus into a logical-message corpus.
 *
 * Input corpus format:
 *   u32 message_count (little-endian)
 *   repeated message_count times:
 *     u32 key_len   (little-endian)
 *     u32 value_len (little-endian)
 *     key bytes
 *     value bytes
 *
 * Output corpus format is the same, but each value becomes:
 *   [magic + encoded header + raw binary section + raw strings section]
 *
 * This is intentionally not the original on-wire format. It is the
 * "replace the app zstd with one dictionary-compressed payload" experiment.
 */

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../corpus_utils.h"
#include "../../points_tags_logical_utils.h"

typedef struct extract_stats_s {
        uint32_t messages_seen;
        uint32_t messages_written;
        uint64_t key_bytes;
        uint64_t wrapped_value_bytes;
        uint64_t logical_value_bytes;
        uint64_t binary_section_bytes;
        uint64_t strings_section_bytes;
} extract_stats_t;

static void usage(const char *argv0) {
        fprintf(stderr,
                "Usage: %s [--max-messages N] [--input-format F] "
                "[--output-format F] OUTPUT INPUT_CORPUS\n",
                argv0);
}

static int write_u32_le(FILE *fp, uint32_t value) {
        uint8_t buf[4];

        buf[0] = (uint8_t)(value & 0xffU);
        buf[1] = (uint8_t)((value >> 8) & 0xffU);
        buf[2] = (uint8_t)((value >> 16) & 0xffU);
        buf[3] = (uint8_t)((value >> 24) & 0xffU);

        return fwrite(buf, 1, sizeof(buf), fp) == sizeof(buf);
}

int main(int argc, char **argv) {
        const char *output_path;
        const char *input_path;
        const char *msg_limit_str = NULL;
        const char *input_format_str = NULL;
        const char *output_format_str = NULL;
        corpus_message_format_t input_format =
            CORPUS_MESSAGE_FORMAT_COUNTED;
        corpus_message_format_t output_format =
            CORPUS_MESSAGE_FORMAT_COUNTED;
        uint32_t msg_limit        = 0;
        corpus_file_t input_file;
        corpus_reader_t reader;
        FILE *out          = NULL;
        extract_stats_t stats;
        int argi;

        memset(&input_file, 0, sizeof(input_file));
        memset(&stats, 0, sizeof(stats));

        for (argi = 1; argi < argc; argi++) {
                if (!strcmp(argv[argi], "--help") || !strcmp(argv[argi], "-h")) {
                        usage(argv[0]);
                        return 0;
                }

                if (!strcmp(argv[argi], "--max-messages")) {
                        if (argi + 1 >= argc) {
                                usage(argv[0]);
                                fprintf(stderr,
                                        "--max-messages needs a value\n");
                                return 1;
                        }

                        msg_limit_str = argv[++argi];
                        continue;
                }

                if (!strcmp(argv[argi], "--input-format")) {
                        if (argi + 1 >= argc) {
                                usage(argv[0]);
                                fprintf(stderr,
                                        "--input-format needs a value\n");
                                return 1;
                        }

                        input_format_str = argv[++argi];
                        continue;
                }

                if (!strcmp(argv[argi], "--output-format")) {
                        if (argi + 1 >= argc) {
                                usage(argv[0]);
                                fprintf(stderr,
                                        "--output-format needs a value\n");
                                return 1;
                        }

                        output_format_str = argv[++argi];
                        continue;
                }

                break;
        }

        if (argc - argi != 2) {
                usage(argv[0]);
                return 1;
        }

        output_path = argv[argi];
        input_path  = argv[argi + 1];

        if (msg_limit_str) {
                char *endptr = NULL;
                unsigned long parsed;

                errno  = 0;
                parsed = strtoul(msg_limit_str, &endptr, 10);
                if (errno || !endptr || *endptr != '\0' || parsed == 0 ||
                    parsed > UINT32_MAX) {
                        fprintf(stderr, "invalid --max-messages value: %s\n",
                                msg_limit_str);
                        return 1;
                }

                msg_limit = (uint32_t)parsed;
        }

        if (input_format_str &&
            corpus_message_format_parse(input_format_str, &input_format) != 0) {
                fprintf(stderr, "invalid --input-format value: %s\n",
                        input_format_str);
                return 1;
        }

        if (output_format_str) {
                if (corpus_message_format_parse(output_format_str,
                                                &output_format) != 0) {
                        fprintf(stderr, "invalid --output-format value: %s\n",
                                output_format_str);
                        return 1;
                }
        } else {
                output_format = input_format;
        }

        if (!corpus_file_load(input_path, &input_file)) {
                fprintf(stderr, "failed to load input corpus: %s\n", input_path);
                return 1;
        }

        if (!corpus_reader_init(&reader, &input_file, input_format)) {
                fprintf(stderr, "failed to initialize input corpus %s with "
                                "format %s\n",
                        input_path, corpus_message_format_name(input_format));
                corpus_file_unload(&input_file);
                return 1;
        }

        out = fopen(output_path, "wb");
        if (!out) {
                fprintf(stderr, "failed to open output %s: %s\n", output_path,
                        strerror(errno));
                corpus_file_unload(&input_file);
                return 1;
        }

        if (output_format == CORPUS_MESSAGE_FORMAT_COUNTED &&
            !write_u32_le(out, 0)) {
                fprintf(stderr, "failed to write output header to %s\n",
                        output_path);
                fclose(out);
                corpus_file_unload(&input_file);
                return 1;
        }

        for (uint32_t i = 0; msg_limit == 0 || i < msg_limit; i++) {
                corpus_msg_t msg;
                uint8_t *logical_value = NULL;
                size_t logical_value_len;
                size_t binary_len;
                size_t strings_len;
                char errstr[256];
                int r;

                r = corpus_reader_read_one(&reader, &msg);
                if (r == 0)
                        break;

                if (r < 0) {
                        fprintf(stderr,
                                "failed to read message %" PRIu32
                                " from %s\n",
                                i, input_path);
                        fclose(out);
                        corpus_file_unload(&input_file);
                        return 1;
                }

                stats.messages_seen++;

                if (!points_tags_rewrite_logical_value(
                        msg.value, msg.value_len, &logical_value,
                        &logical_value_len, &binary_len, &strings_len, errstr,
                        sizeof(errstr))) {
                        fprintf(stderr,
                                "message %" PRIu32 " rewrite failed: %s\n",
                                i, errstr);
                        fclose(out);
                        corpus_file_unload(&input_file);
                        return 1;
                }

                if (logical_value_len > UINT32_MAX) {
                        fprintf(stderr,
                                "logical value too large for u32 length: %zu\n",
                                logical_value_len);
                        free(logical_value);
                        fclose(out);
                        corpus_file_unload(&input_file);
                        return 1;
                }

                if ((output_format == CORPUS_MESSAGE_FORMAT_COUNTED &&
                     (!write_u32_le(out, msg.key_len) ||
                      !write_u32_le(out, (uint32_t)logical_value_len) ||
                      (msg.key_len > 0 &&
                       fwrite(msg.key, 1, msg.key_len, out) != msg.key_len) ||
                      (logical_value_len > 0 &&
                       fwrite(logical_value, 1, logical_value_len, out) !=
                           logical_value_len))) ||
                    (output_format == CORPUS_MESSAGE_FORMAT_PARTITION_DUMP &&
                     (!write_u32_le(out, (uint32_t)logical_value_len) ||
                      !write_u32_le(out, msg.partition) ||
                      (logical_value_len > 0 &&
                       fwrite(logical_value, 1, logical_value_len, out) !=
                           logical_value_len)))) {
                        fprintf(stderr,
                                "failed to write rewritten message %" PRIu32
                                " to %s\n",
                                i, output_path);
                        free(logical_value);
                        fclose(out);
                        corpus_file_unload(&input_file);
                        return 1;
                }

                stats.messages_written++;
                stats.key_bytes += msg.key_len;
                stats.wrapped_value_bytes += msg.value_len;
                stats.logical_value_bytes += logical_value_len;
                stats.binary_section_bytes += binary_len;
                stats.strings_section_bytes += strings_len;

                free(logical_value);
        }

        if (output_format == CORPUS_MESSAGE_FORMAT_COUNTED) {
                if (fseek(out, 0, SEEK_SET) != 0 ||
                    !write_u32_le(out, stats.messages_written)) {
                        fprintf(stderr,
                                "failed to backfill message count in %s\n",
                                output_path);
                        fclose(out);
                        corpus_file_unload(&input_file);
                        return 1;
                }
        }

        fclose(out);
        corpus_file_unload(&input_file);

        fprintf(stdout,
                "wrote output=%s output_format=%s input=%s input_format=%s "
                "messages=%" PRIu32
                " key_bytes=%" PRIu64 " wrapped_value_bytes=%" PRIu64
                " logical_value_bytes=%" PRIu64
                " binary_section_bytes=%" PRIu64
                " strings_section_bytes=%" PRIu64 "\n",
                output_path, corpus_message_format_name(output_format),
                input_path, corpus_message_format_name(input_format),
                stats.messages_written,
                stats.key_bytes, stats.wrapped_value_bytes,
                stats.logical_value_bytes, stats.binary_section_bytes,
                stats.strings_section_bytes);
        return 0;
}
