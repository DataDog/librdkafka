/*
 * librdkafka - Apache Kafka C library
 *
 * Shared helpers for rewriting points-tags wrapped payloads into logical
 * messages:
 *   [magic + encoded header + raw binary section + raw strings section]
 */

#ifndef TESTS_POINTS_TAGS_LOGICAL_UTILS_H
#define TESTS_POINTS_TAGS_LOGICAL_UTILS_H

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zstd.h>

#define POINTS_TAGS_MAGIC_V3 0x4e4b4a03U
#define POINTS_TAGS_MAGIC_V4 0x4e4b4a04U

static int points_tags_read_uvarint_mem(const uint8_t **pp,
                                        const uint8_t *end,
                                        uint64_t *out) {
        const uint8_t *p = *pp;
        uint64_t value   = 0;
        unsigned shift   = 0;

        while (p < end && shift < 64) {
                uint8_t byte = *p++;

                value |= ((uint64_t)(byte & 0x7fU)) << shift;
                if (!(byte & 0x80U)) {
                        *pp  = p;
                        *out = value;
                        return 1;
                }

                shift += 7;
        }

        return 0;
}

static int points_tags_read_zstd_section(const uint8_t **pp,
                                         const uint8_t *end,
                                         uint8_t **raw_out,
                                         size_t *raw_len_out,
                                         char *errstr,
                                         size_t errstr_size) {
        uint64_t raw_len_u64;
        uint64_t compressed_len_u64;
        const uint8_t *p = *pp;
        uint8_t *raw;
        size_t actual;

        if (!points_tags_read_uvarint_mem(&p, end, &raw_len_u64) ||
            !points_tags_read_uvarint_mem(&p, end, &compressed_len_u64)) {
                snprintf(errstr, errstr_size,
                         "failed to read section length varints");
                return 0;
        }

        if (raw_len_u64 > SIZE_MAX || compressed_len_u64 > SIZE_MAX) {
                snprintf(errstr, errstr_size,
                         "section length does not fit in size_t");
                return 0;
        }

        if ((size_t)(end - p) < (size_t)compressed_len_u64) {
                snprintf(errstr, errstr_size,
                         "truncated section: need %" PRIu64 ", have %zu",
                         compressed_len_u64, (size_t)(end - p));
                return 0;
        }

        raw = malloc(raw_len_u64 > 0 ? (size_t)raw_len_u64 : 1U);
        if (!raw) {
                snprintf(errstr, errstr_size,
                         "OOM allocating %" PRIu64 "-byte raw section",
                         raw_len_u64);
                return 0;
        }

        actual = ZSTD_decompress(raw, (size_t)raw_len_u64, p,
                                 (size_t)compressed_len_u64);
        if (ZSTD_isError(actual)) {
                snprintf(errstr, errstr_size, "zstd decompress failed: %s",
                         ZSTD_getErrorName(actual));
                free(raw);
                return 0;
        }

        if (actual != (size_t)raw_len_u64) {
                snprintf(errstr, errstr_size,
                         "zstd size mismatch: hint=%" PRIu64 ", actual=%zu",
                         raw_len_u64, actual);
                free(raw);
                return 0;
        }

        p += compressed_len_u64;
        *pp          = p;
        *raw_out     = raw;
        *raw_len_out = (size_t)raw_len_u64;
        return 1;
}

static int points_tags_skip_v3_header(const uint8_t **pp,
                                      const uint8_t *end,
                                      char *errstr,
                                      size_t errstr_size) {
        uint64_t num_header_fields;

        if (!points_tags_read_uvarint_mem(pp, end, &num_header_fields)) {
                snprintf(errstr, errstr_size,
                         "failed to read V3 header field count");
                return 0;
        }

        for (uint64_t i = 0; i < num_header_fields; i++) {
                uint64_t ignored;

                if (!points_tags_read_uvarint_mem(pp, end, &ignored)) {
                        snprintf(errstr, errstr_size,
                                 "failed to read V3 header field %" PRIu64, i);
                        return 0;
                }
        }

        return 1;
}

static int points_tags_skip_v4_header(const uint8_t **pp,
                                      const uint8_t *end,
                                      char *errstr,
                                      size_t errstr_size) {
        uint64_t presence;
        uint64_t extra_presence;

        if (!points_tags_read_uvarint_mem(pp, end, &presence)) {
                snprintf(errstr, errstr_size,
                         "failed to read V4 header presence bitmap");
                return 0;
        }

        for (uint64_t i = 0; i < 13; i++) {
                uint64_t ignored;

                if (!points_tags_read_uvarint_mem(pp, end, &ignored)) {
                        snprintf(errstr, errstr_size,
                                 "failed to read V4 required header field %" PRIu64,
                                 i);
                        return 0;
                }
        }

        if (presence & (1ULL << 13)) {
                uint64_t ignored;

                if (!points_tags_read_uvarint_mem(pp, end, &ignored)) {
                        snprintf(errstr, errstr_size,
                                 "failed to read V4 MessageIDHigh");
                        return 0;
                }
        }

        if (presence & (1ULL << 14)) {
                uint64_t ignored;

                if (!points_tags_read_uvarint_mem(pp, end, &ignored)) {
                        snprintf(errstr, errstr_size,
                                 "failed to read V4 MessageIDLow");
                        return 0;
                }
        }

        extra_presence = presence >> 15;
        for (uint64_t bit = 15; extra_presence != 0;
             bit++, extra_presence >>= 1) {
                if (!(extra_presence & 1ULL))
                        continue;

                uint64_t ignored;

                if (!points_tags_read_uvarint_mem(pp, end, &ignored)) {
                        snprintf(errstr, errstr_size,
                                 "failed to read V4 extra header field %" PRIu64,
                                 bit);
                        return 0;
                }
        }

        return 1;
}

static int points_tags_rewrite_logical_value(
    const uint8_t *value,
    size_t value_len,
    uint8_t **logical_value_out,
    size_t *logical_value_len_out,
    size_t *binary_len_out,
    size_t *strings_len_out,
    char *errstr,
    size_t errstr_size) {
        const uint8_t *cursor = value;
        const uint8_t *end    = value + value_len;
        uint32_t magic;
        size_t prefix_len;
        uint8_t *binary_raw  = NULL;
        uint8_t *strings_raw = NULL;
        size_t binary_len;
        size_t strings_len;
        uint8_t *logical_value;
        size_t logical_len;

        if (value_len < 4) {
                snprintf(errstr, errstr_size, "payload too short: %zu bytes",
                         value_len);
                return 0;
        }

        magic = ((uint32_t)value[0]) | ((uint32_t)value[1] << 8) |
                ((uint32_t)value[2] << 16) | ((uint32_t)value[3] << 24);
        if (magic != POINTS_TAGS_MAGIC_V3 && magic != POINTS_TAGS_MAGIC_V4) {
                snprintf(errstr, errstr_size,
                         "unexpected magic 0x%08" PRIx32, magic);
                return 0;
        }

        cursor += 4;

        if (magic == POINTS_TAGS_MAGIC_V3) {
                if (!points_tags_skip_v3_header(&cursor, end, errstr,
                                               errstr_size))
                        return 0;
        } else {
                if (!points_tags_skip_v4_header(&cursor, end, errstr,
                                               errstr_size))
                        return 0;
        }

        prefix_len = (size_t)(cursor - value);

        if (!points_tags_read_zstd_section(&cursor, end, &binary_raw,
                                           &binary_len, errstr, errstr_size))
                return 0;

        if (!points_tags_read_zstd_section(&cursor, end, &strings_raw,
                                           &strings_len, errstr, errstr_size)) {
                free(binary_raw);
                return 0;
        }

        if (cursor != end) {
                snprintf(errstr, errstr_size,
                         "unexpected trailing payload bytes: %zu",
                         (size_t)(end - cursor));
                free(strings_raw);
                free(binary_raw);
                return 0;
        }

        if (prefix_len > SIZE_MAX - binary_len ||
            prefix_len + binary_len > SIZE_MAX - strings_len) {
                snprintf(errstr, errstr_size, "logical payload length overflow");
                free(strings_raw);
                free(binary_raw);
                return 0;
        }

        logical_len   = prefix_len + binary_len + strings_len;
        logical_value = malloc(logical_len > 0 ? logical_len : 1U);
        if (!logical_value) {
                snprintf(errstr, errstr_size,
                         "OOM allocating %zu-byte logical payload",
                         logical_len);
                free(strings_raw);
                free(binary_raw);
                return 0;
        }

        memcpy(logical_value, value, prefix_len);
        memcpy(logical_value + prefix_len, binary_raw, binary_len);
        memcpy(logical_value + prefix_len + binary_len, strings_raw,
               strings_len);

        free(strings_raw);
        free(binary_raw);

        *logical_value_out     = logical_value;
        *logical_value_len_out = logical_len;
        *binary_len_out        = binary_len;
        *strings_len_out       = strings_len;
        return 1;
}

#endif
