/*
 * librdkafka - Apache Kafka C library
 *
 * Manual real-cluster replay for points-tags replacement experiments.
 *
 * Replays points-tags corpora to a real cluster for disk-size comparisons.
 *
 * Two corpus shapes are relevant:
 *   1. Baseline wrapped values as produced by the current intake-v2 pipeline.
 *   2. Logical values rewritten as:
 *      [magic + encoded header + raw binary section + raw strings section]
 */

#include "test.h"
#include "corpus_utils.h"
#include "points_tags_logical_utils.h"

#if WITH_ZSTD
#include <zstd.h>

typedef enum replay_mode_t {
        REPLAY_MODE_BASELINE,
        REPLAY_MODE_PAYLOAD_DICT,
        REPLAY_MODE_OUTER_ZSTD_DICT,
} replay_mode_t;

typedef struct payload_dict_ctx_s {
        ZSTD_CCtx *cctx;
        ZSTD_CDict *cdict;
        int level;
        unsigned dict_id;
} payload_dict_ctx_t;

static void payload_dict_ctx_init(payload_dict_ctx_t *ctx,
                                  const corpus_file_t *dict_file,
                                  int level) {
        memset(ctx, 0, sizeof(*ctx));
        ctx->level = level;
        ctx->dict_id =
            ZSTD_getDictID_fromDict(dict_file->data, dict_file->len);
        TEST_ASSERT(ctx->dict_id != 0,
                    "Expected a conformant dictionary with non-zero dictID");
        ctx->cctx = ZSTD_createCCtx();
        TEST_ASSERT(ctx->cctx, "Failed to create ZSTD_CCtx");
        ctx->cdict = ZSTD_createCDict(dict_file->data, dict_file->len, level);
        TEST_ASSERT(ctx->cdict, "Failed to create ZSTD_CDict");
}

static void payload_dict_ctx_destroy(payload_dict_ctx_t *ctx) {
        if (ctx->cdict)
                ZSTD_freeCDict(ctx->cdict);
        if (ctx->cctx)
                ZSTD_freeCCtx(ctx->cctx);
        memset(ctx, 0, sizeof(*ctx));
}

static rd_kafka_resp_err_t
enqueue_message(rd_kafka_t *rk,
                rd_kafka_topic_t *rkt,
                int32_t partition,
                const void *payload,
                size_t payload_len,
                int msgflags,
                const corpus_msg_t *msg,
                int *inflightp) {
        rd_kafka_resp_err_t err;

        do {
                err = rd_kafka_producev(
                    rk, RD_KAFKA_V_RKT(rkt), RD_KAFKA_V_PARTITION(partition),
                    RD_KAFKA_V_VALUE((void *)payload, payload_len),
                    RD_KAFKA_V_KEY((void *)msg->key, msg->key_len),
                    RD_KAFKA_V_MSGFLAGS(msgflags),
                    RD_KAFKA_V_OPAQUE(inflightp), RD_KAFKA_V_END);

                if (!err) {
                        (*inflightp)++;
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
                }

                if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL)
                        return err;

                rd_kafka_poll(rk, 50);
        } while (1);
}

static void do_test_points_tags_replay_real(void) {
        const char *corpus_path;
        const char *dict_path;
        const char *mode_str;
        const char *topic_suffix;
        const char *dict_level_str;
        const char *outer_level_str;
        const char *msg_limit_str;
        const char *corpus_format_str;
        replay_mode_t mode = REPLAY_MODE_BASELINE;
        corpus_message_format_t corpus_format =
            CORPUS_MESSAGE_FORMAT_COUNTED;
        corpus_file_t corpus_file;
        corpus_file_t dict_file;
        corpus_reader_t reader;
        corpus_scan_result_t scan;
        payload_dict_ctx_t dict_ctx;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        char *topic;
        uint32_t msg_count = 0;
        uint32_t msg_limit = 0;
        uint32_t sent      = 0;
        int inflight       = 0;
        int dict_level;

        if (test_needs_auth()) {
                TEST_SKIP("This manual replay test expects the local plaintext "
                          "cluster from tests/test.conf\n");
                return;
        }

        corpus_path = test_getenv("CORPUS_PATH", NULL);
        dict_path   = test_getenv("DICT_PATH", NULL);
        mode_str    = test_getenv("POINTS_TAGS_REPLAY_MODE", NULL);
        if (!corpus_path || !mode_str) {
                TEST_SKIP("Set CORPUS_PATH, DICT_PATH, and "
                          "POINTS_TAGS_REPLAY_MODE={baseline|payload-dict|outer-zstd-dict} "
                          "to run this manual replay\n");
                return;
        }

        if (!strcmp(mode_str, "baseline"))
                mode = REPLAY_MODE_BASELINE;
        else if (!strcmp(mode_str, "payload-dict"))
                mode = REPLAY_MODE_PAYLOAD_DICT;
        else if (!strcmp(mode_str, "outer-zstd-dict"))
                mode = REPLAY_MODE_OUTER_ZSTD_DICT;
        else
                TEST_FAIL("Unknown POINTS_TAGS_REPLAY_MODE=%s", mode_str);

        topic_suffix = test_getenv(
            "POINTS_TAGS_TOPIC_SUFFIX",
            mode == REPLAY_MODE_BASELINE         ? "0209-pt-baseline"
            : mode == REPLAY_MODE_PAYLOAD_DICT   ? "0209-pt-payload-dict"
                                                 : "0209-pt-outer-zstd-dict");
        dict_level_str  = test_getenv("DICT_LEVEL", "3");
        outer_level_str = test_getenv("OUTER_COMPRESSION_LEVEL",
                                      dict_level_str);
        corpus_format_str = test_getenv("CORPUS_FORMAT", "message");
        dict_level      = atoi(dict_level_str);
        msg_limit_str   = test_getenv("CORPUS_MSG_LIMIT", NULL);
        if (msg_limit_str)
                msg_limit = (uint32_t)strtoul(msg_limit_str, NULL, 10);
        TEST_ASSERT(corpus_message_format_parse(corpus_format_str,
                                                &corpus_format) == 0,
                    "Unknown CORPUS_FORMAT=%s", corpus_format_str);
        if (mode != REPLAY_MODE_BASELINE)
                TEST_ASSERT(dict_path, "DICT_PATH is required for replay mode %s",
                            mode_str);

        TEST_ASSERT(corpus_file_load(corpus_path, &corpus_file),
                    "Failed to load corpus from %s", corpus_path);
        if (mode != REPLAY_MODE_BASELINE)
                TEST_ASSERT(corpus_file_load(dict_path, &dict_file),
                            "Failed to load dictionary from %s", dict_path);

        if (mode == REPLAY_MODE_PAYLOAD_DICT)
                payload_dict_ctx_init(&dict_ctx, &dict_file, dict_level);
        else
                memset(&dict_ctx, 0, sizeof(dict_ctx));

        TEST_ASSERT(corpus_scan(&corpus_file, corpus_format, msg_limit, &scan),
                    "Failed to scan corpus %s", corpus_path);
        TEST_ASSERT(scan.partition_count > 0,
                    "Expected at least one partition in corpus");
        TEST_ASSERT(corpus_reader_init(&reader, &corpus_file, corpus_format),
                    "Failed to initialize corpus reader for %s", corpus_path);

        test_conf_init(&conf, NULL, 120);
        test_conf_set(conf, "linger.ms", "200");
        test_conf_set(conf, "max.in.flight", "1");
        test_conf_set(conf, "batch.num.messages", "10000");
        test_conf_set(conf, "batch.size", "2621440");
        test_conf_set(conf, "produce.engine", "v2");
        test_conf_set(conf, "message.max.bytes", "20971520");
        test_conf_set(conf, "queue.buffering.max.kbytes", "600000");
        test_conf_set(conf, "queue.buffering.max.messages", "600000");
        if (mode == REPLAY_MODE_BASELINE) {
                test_conf_set(conf, "compression.type", "none");
        } else if (mode == REPLAY_MODE_PAYLOAD_DICT) {
                test_conf_set(conf, "compression.type", "none");
        } else {
                test_conf_set(conf, "compression.type", "zstd");
                test_conf_set(conf, "compression.level", outer_level_str);
                test_conf_set(conf, "ut.compression.zstd.dict.path", dict_path);
        }
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        TEST_ASSERT(rk, "Failed to create producer");

        topic = rd_strdup(test_mk_topic_name(topic_suffix, 1));
        TEST_ASSERT(topic, "Failed to allocate topic name");
        TEST_SAY("Replay mode=%s corpus=%s corpus_format=%s dict=%s topic=%s "
                 "messages=%" PRIu32 " partitions=%" PRIu32 "\n",
                 mode_str, corpus_path,
                 corpus_message_format_name(corpus_format),
                 dict_path ? dict_path : "(none)", topic,
                 scan.message_count, scan.partition_count);

        test_admin_create_topic(rk, topic, (int)scan.partition_count, 1,
                                (const char *[]) {"max.message.bytes",
                                                  "20971520", NULL});
        test_wait_topic_exists(rk, topic, tmout_multip(10000));
        rkt = test_create_producer_topic(rk, topic, NULL);

        msg_count = scan.message_count;

        for (uint32_t i = 0; i < msg_count; i++) {
                corpus_msg_t msg;
                uint8_t *logical_value = NULL;
                size_t logical_value_len = 0;
                size_t binary_len        = 0;
                size_t strings_len       = 0;
                char errstr[256];
                rd_kafka_resp_err_t err;
                int r;

                r = corpus_reader_read_one(&reader, &msg);
                TEST_ASSERT(r == 1,
                            "Failed to read message %" PRIu32, i);

                if (mode == REPLAY_MODE_BASELINE) {
                        err = enqueue_message(rk, rkt, msg.partition, msg.value,
                                              msg.value_len,
                                              RD_KAFKA_MSG_F_COPY, &msg,
                                              &inflight);
                } else {
                        if (!points_tags_rewrite_logical_value(
                                msg.value, msg.value_len, &logical_value,
                                &logical_value_len, &binary_len, &strings_len,
                                errstr, sizeof(errstr))) {
                                TEST_FAIL(
                                    "Failed to rewrite message %" PRIu32 ": %s",
                                    i, errstr);
                        }
                }

                if (mode == REPLAY_MODE_PAYLOAD_DICT) {
                        void *compressed;
                        size_t compressed_cap;
                        size_t compressed_len;

                        compressed_cap = ZSTD_compressBound(logical_value_len);
                        compressed     = malloc(compressed_cap);
                        TEST_ASSERT(compressed, "OOM allocating %zu-byte "
                                                 "compression buffer",
                                    compressed_cap);

                        compressed_len =
                            ZSTD_compress_usingCDict(dict_ctx.cctx, compressed,
                                                     compressed_cap,
                                                     logical_value,
                                                     logical_value_len,
                                                     dict_ctx.cdict);
                        TEST_ASSERT(
                            !ZSTD_isError(compressed_len),
                            "ZSTD compression failed: %s",
                            ZSTD_getErrorName(compressed_len));

                        err = enqueue_message(rk, rkt, msg.partition,
                                              compressed,
                                              compressed_len,
                                              RD_KAFKA_MSG_F_FREE, &msg,
                                              &inflight);
                        free(logical_value);
                        if (err)
                                free(compressed);
                } else if (mode == REPLAY_MODE_OUTER_ZSTD_DICT) {
                        err = enqueue_message(rk, rkt, msg.partition,
                                              logical_value,
                                              logical_value_len,
                                              RD_KAFKA_MSG_F_FREE, &msg,
                                              &inflight);
                        if (err)
                                free(logical_value);
                }

                TEST_ASSERT(!err, "Failed to enqueue message %" PRIu32 ": %s",
                            i, rd_kafka_err2str(err));
                sent++;

                if ((i % 100) == 0)
                        rd_kafka_poll(rk, 0);

                if (mode != REPLAY_MODE_BASELINE) {
                        (void)binary_len;
                        (void)strings_len;
                }
        }

        TEST_CALL_ERR__(rd_kafka_flush(rk, tmout_multip(30000)));
        TEST_ASSERT(inflight == 0, "Expected all messages delivered, "
                                   "still have %d inflight",
                    inflight);

        TEST_SAY("Replay complete: topic=%s sent=%" PRIu32 "\n", topic, sent);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        rd_free(topic);
        if (mode == REPLAY_MODE_PAYLOAD_DICT)
                payload_dict_ctx_destroy(&dict_ctx);
        if (mode != REPLAY_MODE_BASELINE)
                corpus_file_unload(&dict_file);
        corpus_file_unload(&corpus_file);
}

int main_0209_points_tags_replay_real(int argc, char **argv) {
        (void)argc;
        (void)argv;
        do_test_points_tags_replay_real();
        return 0;
}

#else

#include "test.h"

int main_0209_points_tags_replay_real(int argc, char **argv) {
        (void)argc;
        (void)argv;
        TEST_SKIP("Test requires WITH_ZSTD support\n");
}

#endif
