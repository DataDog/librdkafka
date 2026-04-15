/*
 * librdkafka - Apache Kafka C library
 *
 * Shared helpers for the simple corpus file format used by payload dictionary
 * experiments.
 */

#ifndef TESTS_CORPUS_UTILS_H
#define TESTS_CORPUS_UTILS_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct corpus_file_s {
        uint8_t *data;
        size_t len;
} corpus_file_t;

typedef struct corpus_buf_s {
        const uint8_t *p;
        const uint8_t *end;
} corpus_buf_t;

typedef struct corpus_msg_s {
        const uint8_t *key;
        uint32_t key_len;
        const uint8_t *value;
        uint32_t value_len;
        uint32_t partition;
        int has_partition;
} corpus_msg_t;

typedef struct corpus_sample_s {
        const uint8_t *data;
        uint32_t len;
} corpus_sample_t;

typedef enum corpus_message_format_e {
        CORPUS_MESSAGE_FORMAT_COUNTED = 0,
        CORPUS_MESSAGE_FORMAT_PARTITION_DUMP = 1,
} corpus_message_format_t;

typedef struct corpus_reader_s {
        corpus_buf_t buf;
        corpus_message_format_t format;
        uint32_t remaining_count;
} corpus_reader_t;

typedef struct corpus_scan_result_s {
        uint32_t message_count;
        uint32_t partition_count;
} corpus_scan_result_t;

#define CORPUS_BATCH_MAGIC "RDKBAT01"
#define CORPUS_BATCH_MAGIC_SIZE 8U

static int corpus_read_u32_le_mem(corpus_buf_t *buf, uint32_t *out) {
        const uint8_t *p = buf->p;

        if ((size_t)(buf->end - p) < 4)
                return 0;

        *out = ((uint32_t)p[0]) | ((uint32_t)p[1] << 8) |
               ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);

        buf->p = p + 4;
        return 1;
}

static int corpus_read_one_message_mem(corpus_buf_t *buf, corpus_msg_t *msg) {
        uint32_t key_len;
        uint32_t value_len;

        if (!corpus_read_u32_le_mem(buf, &key_len))
                return 0;

        if (!corpus_read_u32_le_mem(buf, &value_len))
                return 0;

        if ((size_t)(buf->end - buf->p) < (size_t)key_len + (size_t)value_len)
                return 0;

        msg->key     = buf->p;
        msg->key_len = key_len;
        buf->p += key_len;

        msg->value     = buf->p;
        msg->value_len = value_len;
        buf->p += value_len;

        return 1;
}

static int
corpus_read_one_partition_dump_mem(corpus_buf_t *buf, corpus_msg_t *msg) {
        uint32_t value_len;
        uint32_t partition;

        if (buf->p == buf->end)
                return 0;

        if (!corpus_read_u32_le_mem(buf, &value_len))
                return -1;

        if (!corpus_read_u32_le_mem(buf, &partition))
                return -1;

        if ((size_t)(buf->end - buf->p) < (size_t)value_len)
                return -1;

        msg->key           = NULL;
        msg->key_len       = 0;
        msg->value         = buf->p;
        msg->value_len     = value_len;
        msg->partition     = partition;
        msg->has_partition = 1;
        buf->p += value_len;

        return 1;
}

static int corpus_message_format_parse(const char *s,
                                       corpus_message_format_t *out) {
        if (!strcmp(s, "message") || !strcmp(s, "counted")) {
                *out = CORPUS_MESSAGE_FORMAT_COUNTED;
                return 0;
        }

        if (!strcmp(s, "partition-dump") || !strcmp(s, "dump")) {
                *out = CORPUS_MESSAGE_FORMAT_PARTITION_DUMP;
                return 0;
        }

        return -1;
}

static const char *
corpus_message_format_name(corpus_message_format_t format) {
        switch (format)
        {
        case CORPUS_MESSAGE_FORMAT_COUNTED:
                return "message";
        case CORPUS_MESSAGE_FORMAT_PARTITION_DUMP:
                return "partition-dump";
        }

        return "unknown";
}

static int corpus_reader_init(corpus_reader_t *reader,
                              const corpus_file_t *file,
                              corpus_message_format_t format) {
        memset(reader, 0, sizeof(*reader));
        reader->buf.p   = file->data;
        reader->buf.end = file->data + file->len;
        reader->format  = format;

        if (format == CORPUS_MESSAGE_FORMAT_COUNTED)
                return corpus_read_u32_le_mem(&reader->buf,
                                              &reader->remaining_count);

        return 1;
}

static int corpus_reader_read_one(corpus_reader_t *reader, corpus_msg_t *msg) {
        memset(msg, 0, sizeof(*msg));

        if (reader->format == CORPUS_MESSAGE_FORMAT_COUNTED) {
                if (reader->remaining_count == 0)
                        return 0;

                if (!corpus_read_one_message_mem(&reader->buf, msg))
                        return -1;

                reader->remaining_count--;
                return 1;
        }

        return corpus_read_one_partition_dump_mem(&reader->buf, msg);
}

static int corpus_scan_message_file(const corpus_file_t *file,
                                    uint32_t msg_limit,
                                    corpus_scan_result_t *result) {
        corpus_buf_t buf;
        uint32_t count = 0;

        buf.p   = file->data;
        buf.end = file->data + file->len;

        if (!corpus_read_u32_le_mem(&buf, &count))
                return 0;

        if (msg_limit > 0 && msg_limit < count)
                count = msg_limit;

        result->message_count   = count;
        result->partition_count = count > 0 ? 1 : 0;
        return 1;
}

static int corpus_scan_partition_dump(const corpus_file_t *file,
                                      uint32_t msg_limit,
                                      corpus_scan_result_t *result) {
        corpus_buf_t buf;
        uint32_t count = 0;
        uint32_t max_partition = 0;

        buf.p   = file->data;
        buf.end = file->data + file->len;

        while (buf.p < buf.end && (msg_limit == 0 || count < msg_limit)) {
                corpus_msg_t msg;
                int r = corpus_read_one_partition_dump_mem(&buf, &msg);

                if (r <= 0)
                        return 0;

                if (msg.partition > max_partition)
                        max_partition = msg.partition;
                count++;
        }

        result->message_count   = count;
        result->partition_count = count > 0 ? (max_partition + 1) : 0;
        return buf.p == buf.end || (msg_limit > 0 && count == msg_limit);
}

static int corpus_scan(const corpus_file_t *file,
                       corpus_message_format_t format,
                       uint32_t msg_limit,
                       corpus_scan_result_t *result) {
        memset(result, 0, sizeof(*result));

        if (format == CORPUS_MESSAGE_FORMAT_COUNTED)
                return corpus_scan_message_file(file, msg_limit, result);

        return corpus_scan_partition_dump(file, msg_limit, result);
}

static int corpus_has_batch_samples(const corpus_file_t *file) {
        return file->len >= CORPUS_BATCH_MAGIC_SIZE &&
               !memcmp(file->data, CORPUS_BATCH_MAGIC, CORPUS_BATCH_MAGIC_SIZE);
}

static int
corpus_read_batch_header_mem(corpus_buf_t *buf, uint32_t *sample_count) {
        if ((size_t)(buf->end - buf->p) < CORPUS_BATCH_MAGIC_SIZE)
                return 0;

        if (memcmp(buf->p, CORPUS_BATCH_MAGIC, CORPUS_BATCH_MAGIC_SIZE) != 0)
                return 0;

        buf->p += CORPUS_BATCH_MAGIC_SIZE;
        return corpus_read_u32_le_mem(buf, sample_count);
}

static int
corpus_read_one_sample_mem(corpus_buf_t *buf, corpus_sample_t *sample) {
        uint32_t len;

        if (!corpus_read_u32_le_mem(buf, &len))
                return 0;

        if ((size_t)(buf->end - buf->p) < (size_t)len)
                return 0;

        sample->data = buf->p;
        sample->len  = len;
        buf->p += len;

        return 1;
}

static int corpus_file_load(const char *path, corpus_file_t *file) {
        FILE *fp;
        long sz;

        memset(file, 0, sizeof(*file));

        fp = fopen(path, "rb");
        if (!fp)
                return 0;

        if (fseek(fp, 0, SEEK_END) != 0) {
                fclose(fp);
                return 0;
        }

        sz = ftell(fp);
        if (sz < 0) {
                fclose(fp);
                return 0;
        }

        rewind(fp);

        file->len = (size_t)sz;
        if (file->len == 0) {
                fclose(fp);
                return 0;
        }

        file->data = malloc(file->len);
        if (!file->data) {
                fclose(fp);
                return 0;
        }

        if (fread(file->data, 1, file->len, fp) != file->len) {
                free(file->data);
                memset(file, 0, sizeof(*file));
                fclose(fp);
                return 0;
        }

        fclose(fp);
        return 1;
}

static void corpus_file_unload(corpus_file_t *file) {
        free(file->data);
        memset(file, 0, sizeof(*file));
}

#endif
