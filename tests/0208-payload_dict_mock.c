/*
 * librdkafka - Apache Kafka C library
 *
 * Manual benchmark for comparing payload-dictionary compression with Kafka
 * outer compression, using either a mock broker or the local real cluster.
 */

#include "test.h"
#include "corpus_utils.h"
#include "../src/rdkafka_proto.h"

#if WITH_ZSTD
#include <inttypes.h>
#include <zstd.h>

typedef enum bench_mode_t {
        BENCH_MODE_PAYLOAD_DICT,
        BENCH_MODE_OUTER_ZSTD_DICT,
} bench_mode_t;

typedef struct payload_dict_ctx_s {
        ZSTD_CCtx *cctx;
        ZSTD_CDict *cdict;
        int level;
        unsigned dict_id;
} payload_dict_ctx_t;

typedef struct stats_ctx_s {
        mtx_t lock;
        int64_t tx_bytes;
        int64_t txmsg_bytes;
        int64_t txmsgs;
        int64_t produce_requests;
        double batchsize_avg;
        int64_t stats_ts_us;
} stats_ctx_t;

typedef struct run_result_s {
        uint32_t messages_attempted;
        uint32_t messages_sent;
        uint32_t messages_failed;
        int64_t tx_bytes;
        int64_t txmsg_bytes;
        int64_t txmsgs;
        int64_t produce_requests_stats;
        int64_t produce_requests_mock;
        double batchsize_avg;
        double elapsed_ms;
} run_result_t;

typedef rd_kafka_resp_err_t (*send_msg_cb_t)(rd_kafka_t *rk,
                                             rd_kafka_topic_t *rkt,
                                             int32_t partition,
                                             const corpus_msg_t *msg,
                                             int *inflightp,
                                             void *opaque);

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

static void stats_ctx_init(stats_ctx_t *ctx) {
        memset(ctx, 0, sizeof(*ctx));
        mtx_init(&ctx->lock, mtx_plain);
}

static void stats_ctx_destroy(stats_ctx_t *ctx) {
        mtx_destroy(&ctx->lock);
}

static void stats_ctx_snapshot(const stats_ctx_t *ctx, run_result_t *result) {
        mtx_lock((mtx_t *)&ctx->lock);
        result->tx_bytes               = ctx->tx_bytes;
        result->txmsg_bytes            = ctx->txmsg_bytes;
        result->txmsgs                 = ctx->txmsgs;
        result->produce_requests_stats = ctx->produce_requests;
        result->batchsize_avg          = ctx->batchsize_avg;
        mtx_unlock((mtx_t *)&ctx->lock);
}

static void stats_cb(rd_kafka_t *rk,
                     const rd_kafka_stats_t *stats,
                     void *opaque) {
        stats_ctx_t *ctx = (stats_ctx_t *)opaque;
        int64_t produce_reqs = 0;
        double batchsize_avg = 0.0;
        uint32_t i;

        (void)rk;

        if (!ctx)
                return;

        for (i = 0; i < stats->broker_cnt; i++) {
                const rd_kafka_broker_stats_t *broker = &stats->brokers[i];
                uint32_t j;

                for (j = 0; j < broker->req_cnt; j++) {
                        if (!strcmp(broker->reqs[j].name, "Produce"))
                                produce_reqs += broker->reqs[j].count;
                }
        }

        if (stats->topic_cnt > 0)
                batchsize_avg = stats->topics[0].batchsize.avg;

        mtx_lock(&ctx->lock);
        ctx->tx_bytes         = stats->tx_bytes;
        ctx->txmsg_bytes      = stats->txmsg_bytes;
        ctx->txmsgs           = stats->txmsgs;
        ctx->produce_requests = produce_reqs;
        ctx->batchsize_avg    = batchsize_avg;
        ctx->stats_ts_us      = stats->ts_us;
        mtx_unlock(&ctx->lock);
}

static void wait_for_final_stats(rd_kafka_t *rk, stats_ctx_t *ctx) {
        int i;
        int64_t stats_ts_us = 0;

        for (i = 0; i < 40; i++) {
                rd_kafka_poll(rk, 50);
                mtx_lock(&ctx->lock);
                stats_ts_us = ctx->stats_ts_us;
                mtx_unlock(&ctx->lock);
                if (stats_ts_us > 0)
                        return;
        }
}

static int64_t
count_mock_produce_requests(rd_kafka_mock_cluster_t *mcluster) {
        rd_kafka_mock_request_t **requests;
        size_t request_cnt = 0;
        int64_t produce_reqcnt = 0;
        size_t i;

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);
        for (i = 0; i < request_cnt; i++) {
                if (rd_kafka_mock_request_api_key(requests[i]) ==
                    RD_KAFKAP_Produce)
                        produce_reqcnt++;
        }

        rd_kafka_mock_request_destroy_array(requests, request_cnt);
        return produce_reqcnt;
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

static rd_kafka_resp_err_t
send_msg_raw_copy(rd_kafka_t *rk,
                  rd_kafka_topic_t *rkt,
                  int32_t partition,
                  const corpus_msg_t *msg,
                  int *inflightp,
                  void *opaque) {
        (void)opaque;
        return enqueue_message(rk, rkt, partition, msg->value, msg->value_len,
                               RD_KAFKA_MSG_F_COPY, msg, inflightp);
}

static rd_kafka_resp_err_t
send_msg_payload_dict(rd_kafka_t *rk,
                      rd_kafka_topic_t *rkt,
                      int32_t partition,
                      const corpus_msg_t *msg,
                      int *inflightp,
                      void *opaque) {
        payload_dict_ctx_t *ctx = (payload_dict_ctx_t *)opaque;
        void *compressed;
        size_t compressed_cap;
        size_t compressed_len;
        rd_kafka_resp_err_t err;

        compressed_cap = ZSTD_compressBound(msg->value_len);
        compressed     = malloc(compressed_cap);
        TEST_ASSERT(compressed, "OOM allocating %zu-byte compression buffer",
                    compressed_cap);

        compressed_len =
            ZSTD_compress_usingCDict(ctx->cctx, compressed, compressed_cap,
                                     msg->value, msg->value_len, ctx->cdict);
        TEST_ASSERT(!ZSTD_isError(compressed_len),
                    "ZSTD compression failed: %s",
                    ZSTD_getErrorName(compressed_len));

        err = enqueue_message(rk, rkt, partition, compressed, compressed_len,
                              RD_KAFKA_MSG_F_FREE, msg, inflightp);
        if (err)
                free(compressed);

        return err;
}

static run_result_t run_corpus(const char *run_name,
                               const char *topic,
                               const char *compression_type,
                               const char *compression_level,
                               const char *outer_zstd_dict_path,
                               rd_bool_t use_mock_cluster,
                               const corpus_file_t *corpus,
                               corpus_message_format_t corpus_format,
                               uint32_t msg_limit,
                               send_msg_cb_t send_msg,
                               void *send_ctx) {
        run_result_t result;
        stats_ctx_t stats_ctx;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_mock_cluster_t *mcluster;
        corpus_reader_t reader;
        corpus_scan_result_t scan;
        uint64_t start_ts;
        int inflight = 0;
        uint32_t i;
        const char *linger_ms;
        const char *max_in_flight;
        const char *batch_num_messages;
        const char *batch_size;
        const char *produce_engine;
        const char *message_max_bytes;
        const char *queue_max_kbytes;
        const char *queue_max_messages;
        const char *sticky_partitioning_linger_ms;

        memset(&result, 0, sizeof(result));
        stats_ctx_init(&stats_ctx);

        linger_ms = test_getenv("BENCH_LINGER_MS", "200");
        max_in_flight = test_getenv("BENCH_MAX_IN_FLIGHT", "1");
        batch_num_messages =
            test_getenv("BENCH_BATCH_NUM_MESSAGES", "10000");
        batch_size = test_getenv("BENCH_BATCH_SIZE", "2621440");
        produce_engine = test_getenv("BENCH_PRODUCE_ENGINE", "v2");
        message_max_bytes =
            test_getenv("BENCH_MESSAGE_MAX_BYTES", "20971520");
        queue_max_kbytes =
            test_getenv("BENCH_QUEUE_BUFFERING_MAX_KBYTES", "600000");
        queue_max_messages =
            test_getenv("BENCH_QUEUE_BUFFERING_MAX_MESSAGES", "600000");
        sticky_partitioning_linger_ms =
            test_getenv("BENCH_STICKY_PARTITIONING_LINGER_MS", NULL);

        test_conf_init(&conf, NULL, 30);
        if (use_mock_cluster)
                test_conf_set(conf, "test.mock.num.brokers", "1");
        test_conf_set(conf, "linger.ms", linger_ms);
        test_conf_set(conf, "max.in.flight", max_in_flight);
        test_conf_set(conf, "batch.num.messages", batch_num_messages);
        test_conf_set(conf, "batch.size", batch_size);
        test_conf_set(conf, "produce.engine", produce_engine);
        test_conf_set(conf, "message.max.bytes", message_max_bytes);
        test_conf_set(conf, "queue.buffering.max.kbytes", queue_max_kbytes);
        test_conf_set(conf, "queue.buffering.max.messages",
                      queue_max_messages);
        if (sticky_partitioning_linger_ms &&
            *sticky_partitioning_linger_ms)
                test_conf_set(conf, "sticky.partitioning.linger.ms",
                              sticky_partitioning_linger_ms);
        test_conf_set(conf, "statistics.interval.ms", "50");
        test_conf_set(conf, "compression.type", compression_type);
        if (compression_level && strcmp(compression_type, "none"))
                test_conf_set(conf, "compression.level", compression_level);
        if (outer_zstd_dict_path && !strcmp(compression_type, "zstd"))
                test_conf_set(conf, "ut.compression.zstd.dict.path",
                              outer_zstd_dict_path);
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);
        rd_kafka_conf_set_stats_cb(conf, NULL);
        rd_kafka_conf_set_stats_cb_typed(conf, stats_cb);
        rd_kafka_conf_set_opaque(conf, &stats_ctx);

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        TEST_ASSERT(rk, "Failed to create producer");

        TEST_ASSERT(corpus_scan(corpus, corpus_format, msg_limit, &scan),
                    "Failed to scan corpus for %s", run_name);
        TEST_ASSERT(scan.partition_count > 0,
                    "Expected at least one partition in corpus for %s",
                    run_name);
        TEST_ASSERT(corpus_reader_init(&reader, corpus, corpus_format),
                    "Failed to initialize corpus reader for %s", run_name);

        if (use_mock_cluster) {
                mcluster = rd_kafka_handle_mock_cluster(rk);
                TEST_ASSERT(mcluster, "Expected mock cluster");
                TEST_CALL_ERR__(rd_kafka_mock_topic_create(
                    mcluster, topic, (int)scan.partition_count, 1));
                rd_kafka_mock_start_request_tracking(mcluster);
        } else {
                mcluster = NULL;
                test_create_topic_wait_exists(rk, topic,
                                              (int)scan.partition_count, 1,
                                              tmout_multip(10000));
        }

        rkt = test_create_producer_topic(rk, topic, NULL);

        TEST_SAY("%s: topic=%s compression.type=%s compression.level=%s "
                 "outer.zstd.dict.path=%s corpus_format=%s messages=%" PRIu32
                 " partitions=%" PRIu32 "\n",
                 run_name, topic, compression_type,
                 compression_level ? compression_level : "<default>",
                 outer_zstd_dict_path ? outer_zstd_dict_path : "<none>",
                 corpus_message_format_name(corpus_format),
                 scan.message_count, scan.partition_count);

        start_ts = test_clock();

        for (i = 0; i < scan.message_count; i++) {
                corpus_msg_t msg;
                rd_kafka_resp_err_t err;
                int r;

                r = corpus_reader_read_one(&reader, &msg);
                TEST_ASSERT(r == 1,
                            "Failed to read message %" PRIu32, i);

                result.messages_attempted++;
                err = send_msg(rk, rkt, msg.partition, &msg, &inflight,
                               send_ctx);
                if (err) {
                        result.messages_failed++;
                        TEST_FAIL("Failed to enqueue message %" PRIu32 ": %s",
                                  i, rd_kafka_err2str(err));
                } else {
                        result.messages_sent++;
                }

                if ((i % 100) == 0)
                        rd_kafka_poll(rk, 0);
        }

        TEST_CALL_ERR__(rd_kafka_flush(rk, tmout_multip(30000)));
        wait_for_final_stats(rk, &stats_ctx);

        result.elapsed_ms = (double)(test_clock() - start_ts) / 1000.0;
        result.produce_requests_mock =
            mcluster ? count_mock_produce_requests(mcluster) : -1;
        stats_ctx_snapshot(&stats_ctx, &result);

        TEST_ASSERT(inflight == 0, "Expected all messages to be delivered, "
                                   "still have %d inflight",
                    inflight);
        TEST_ASSERT(result.messages_failed == 0,
                    "Expected zero enqueue failures, got %" PRIu32,
                    result.messages_failed);
        TEST_ASSERT(result.txmsgs == (int64_t)result.messages_sent,
                    "Expected txmsgs=%" PRIu32 ", got %" PRId64,
                    result.messages_sent, result.txmsgs);

        TEST_SAY("%s: sent=%" PRIu32 " txmsg_bytes=%" PRId64
                 " tx_bytes=%" PRId64 " produce_reqs(stats=%" PRId64
                 ",mock=%" PRId64 ") batchsize_avg=%.2f elapsed_ms=%.2f\n",
                 run_name, result.messages_sent, result.txmsg_bytes,
                 result.tx_bytes, result.produce_requests_stats,
                 result.produce_requests_mock, result.batchsize_avg,
                 result.elapsed_ms);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        stats_ctx_destroy(&stats_ctx);

        return result;
}

static void compare_results(const run_result_t *baseline,
                            const run_result_t *candidate) {
        int64_t baseline_reqs =
            baseline->produce_requests_mock >= 0
                ? baseline->produce_requests_mock
                : baseline->produce_requests_stats;
        int64_t candidate_reqs =
            candidate->produce_requests_mock >= 0
                ? candidate->produce_requests_mock
                : candidate->produce_requests_stats;
        double tx_bytes_ratio =
            baseline->tx_bytes > 0
                ? ((double)candidate->tx_bytes / (double)baseline->tx_bytes)
                : 0.0;
        double txmsg_bytes_ratio =
            baseline->txmsg_bytes > 0
                ? ((double)candidate->txmsg_bytes /
                   (double)baseline->txmsg_bytes)
                : 0.0;

        TEST_SAY("Comparison: candidate/baseline tx_bytes=%.4f "
                 "txmsg_bytes=%.4f baseline_reqs=%" PRId64
                 " candidate_reqs=%" PRId64 "\n",
                 tx_bytes_ratio, txmsg_bytes_ratio, baseline_reqs,
                 candidate_reqs);
}

static void do_test_payload_dict_mock(void) {
        const char *shared_corpus_path;
        const char *baseline_corpus_path;
        const char *candidate_corpus_path;
        const char *dict_path;
        const char *dict_level_str;
        const char *msg_limit_str;
        const char *bench_mode_str;
        const char *shared_corpus_format_str;
        const char *baseline_corpus_format_str;
        const char *candidate_corpus_format_str;
        const char *baseline_compression_type;
        const char *baseline_compression_level;
        const char *candidate_outer_compression;
        const char *outer_compression_level;
        const char *use_real_cluster_str;
        const char *baseline_subtest_name;
        const char *candidate_subtest_name;
        rd_bool_t use_mock_cluster;
        rd_bool_t candidate_corpus_is_alias = rd_false;
        bench_mode_t bench_mode = BENCH_MODE_PAYLOAD_DICT;
        corpus_message_format_t shared_corpus_format =
            CORPUS_MESSAGE_FORMAT_COUNTED;
        corpus_message_format_t baseline_corpus_format =
            CORPUS_MESSAGE_FORMAT_COUNTED;
        corpus_message_format_t candidate_corpus_format =
            CORPUS_MESSAGE_FORMAT_COUNTED;
        int dict_level;
        unsigned dict_id;
        uint32_t msg_limit = 0;
        corpus_file_t baseline_corpus_file;
        corpus_file_t candidate_corpus_file;
        corpus_file_t dict_file;
        payload_dict_ctx_t dict_ctx;
        run_result_t baseline;
        run_result_t candidate;
        char *topic_baseline;
        char *topic_candidate;

        use_real_cluster_str = test_getenv("PAYLOAD_DICT_USE_REAL_CLUSTER", NULL);
        use_mock_cluster     = !use_real_cluster_str ||
                           !atoi(use_real_cluster_str);

        if (use_mock_cluster && test_needs_auth()) {
            TEST_SKIP("Mock cluster does not support SSL/SASL\n");
            return;
        }

        shared_corpus_path = test_getenv("CORPUS_PATH", NULL);
        shared_corpus_format_str = test_getenv("CORPUS_FORMAT", "message");
        baseline_corpus_path =
            test_getenv("BASELINE_CORPUS_PATH", shared_corpus_path);
        candidate_corpus_path =
            test_getenv("CANDIDATE_CORPUS_PATH", shared_corpus_path);
        baseline_corpus_format_str = test_getenv("BASELINE_CORPUS_FORMAT",
                                                 shared_corpus_format_str);
        candidate_corpus_format_str = test_getenv("CANDIDATE_CORPUS_FORMAT",
                                                  shared_corpus_format_str);
        dict_path = test_getenv("DICT_PATH", NULL);
        if (!baseline_corpus_path || !candidate_corpus_path || !dict_path) {
                TEST_SKIP("Set DICT_PATH and either CORPUS_PATH or both "
                          "BASELINE_CORPUS_PATH/CANDIDATE_CORPUS_PATH to run "
                          "this manual payload dictionary benchmark\n");
                return;
        }
        TEST_ASSERT(corpus_message_format_parse(shared_corpus_format_str,
                                                &shared_corpus_format) == 0,
                    "Unknown CORPUS_FORMAT=%s", shared_corpus_format_str);
        TEST_ASSERT(corpus_message_format_parse(baseline_corpus_format_str,
                                                &baseline_corpus_format) == 0,
                    "Unknown BASELINE_CORPUS_FORMAT=%s",
                    baseline_corpus_format_str);
        TEST_ASSERT(corpus_message_format_parse(candidate_corpus_format_str,
                                                &candidate_corpus_format) == 0,
                    "Unknown CANDIDATE_CORPUS_FORMAT=%s",
                    candidate_corpus_format_str);

        dict_level_str = test_getenv("DICT_LEVEL", "3");
        dict_level     = atoi(dict_level_str);
        bench_mode_str = test_getenv("DICT_BENCH_MODE", "payload");
        if (!strcmp(bench_mode_str, "payload"))
                bench_mode = BENCH_MODE_PAYLOAD_DICT;
        else if (!strcmp(bench_mode_str, "outer-zstd"))
                bench_mode = BENCH_MODE_OUTER_ZSTD_DICT;
        else
                TEST_FAIL("Unknown DICT_BENCH_MODE=%s, expected payload or "
                          "outer-zstd",
                          bench_mode_str);

        baseline_compression_type =
            test_getenv("BASELINE_COMPRESSION_TYPE", "zstd");
        candidate_outer_compression =
            test_getenv("PAYLOAD_DICT_OUTER_COMPRESSION", "none");
        outer_compression_level =
            test_getenv("OUTER_COMPRESSION_LEVEL", NULL);
        baseline_compression_level =
            test_getenv("BASELINE_COMPRESSION_LEVEL", outer_compression_level);

        msg_limit_str = test_getenv("CORPUS_MSG_LIMIT", NULL);
        if (msg_limit_str)
                msg_limit = (uint32_t)strtoul(msg_limit_str, NULL, 10);

        TEST_ASSERT(corpus_file_load(baseline_corpus_path, &baseline_corpus_file),
                    "Failed to load baseline corpus from %s",
                    baseline_corpus_path);
        if (!strcmp(candidate_corpus_path, baseline_corpus_path)) {
                candidate_corpus_file    = baseline_corpus_file;
                candidate_corpus_is_alias = rd_true;
        } else {
                TEST_ASSERT(
                    corpus_file_load(candidate_corpus_path,
                                     &candidate_corpus_file),
                    "Failed to load candidate corpus from %s",
                    candidate_corpus_path);
        }
        TEST_ASSERT(corpus_file_load(dict_path, &dict_file),
                    "Failed to load dictionary from %s", dict_path);

        dict_id = ZSTD_getDictID_fromDict(dict_file.data, dict_file.len);
        TEST_ASSERT(dict_id != 0,
                    "Expected a conformant dictionary with non-zero dictID");

        if (bench_mode == BENCH_MODE_PAYLOAD_DICT)
                payload_dict_ctx_init(&dict_ctx, &dict_file, dict_level);
        else
                memset(&dict_ctx, 0, sizeof(dict_ctx));

        baseline_subtest_name =
            use_mock_cluster ? "baseline (mock)" : "baseline (real)";
        candidate_subtest_name =
            bench_mode == BENCH_MODE_PAYLOAD_DICT
                ? (use_mock_cluster ? "payload dictionary candidate (mock)"
                                    : "payload dictionary candidate (real)")
                : (use_mock_cluster
                       ? "outer Kafka zstd dictionary candidate (mock)"
                       : "outer Kafka zstd dictionary candidate (real)");

        topic_baseline = rd_strdup(test_mk_topic_name("0208-baseline", 1));
        topic_candidate = rd_strdup(test_mk_topic_name(
            bench_mode == BENCH_MODE_PAYLOAD_DICT ? "0208-payload-dict"
                                                  : "0208-outer-zstd-dict",
            1));
        TEST_ASSERT(topic_baseline && topic_candidate,
                    "Failed to allocate topic names");

        SUB_TEST("%s", baseline_subtest_name);
        TEST_SAY("Using baseline corpus=%s candidate corpus=%s\n",
                 baseline_corpus_path, candidate_corpus_path);
        baseline = run_corpus("baseline", topic_baseline,
                              baseline_compression_type,
                              baseline_compression_level, NULL,
                              use_mock_cluster, &baseline_corpus_file,
                              baseline_corpus_format,
                              msg_limit, send_msg_raw_copy, NULL);
        SUB_TEST_PASS();

        SUB_TEST("%s", candidate_subtest_name);
        if (bench_mode == BENCH_MODE_PAYLOAD_DICT) {
                TEST_SAY("Using payload zstd dictionary dict_id=%u level=%d "
                         "outer=%s\n",
                         dict_ctx.dict_id, dict_ctx.level,
                         candidate_outer_compression);
                candidate = run_corpus("payload-dict", topic_candidate,
                                       candidate_outer_compression,
                                       outer_compression_level, NULL,
                                       use_mock_cluster, &candidate_corpus_file,
                                       candidate_corpus_format,
                                       msg_limit, send_msg_payload_dict,
                                       &dict_ctx);
        } else {
                TEST_SAY("Using outer zstd dictionary dict_id=%u "
                         "compression.level=%s path=%s\n",
                         dict_id,
                         outer_compression_level ? outer_compression_level
                                                 : "<default>",
                         dict_path);
                candidate = run_corpus("outer-zstd-dict", topic_candidate,
                                       "zstd", outer_compression_level,
                                       dict_path, use_mock_cluster,
                                       &candidate_corpus_file,
                                       candidate_corpus_format, msg_limit,
                                       send_msg_raw_copy, NULL);
        }
        SUB_TEST_PASS();

        compare_results(&baseline, &candidate);

        rd_free(topic_candidate);
        rd_free(topic_baseline);
        if (bench_mode == BENCH_MODE_PAYLOAD_DICT)
                payload_dict_ctx_destroy(&dict_ctx);
        corpus_file_unload(&dict_file);
        if (!candidate_corpus_is_alias)
                corpus_file_unload(&candidate_corpus_file);
        corpus_file_unload(&baseline_corpus_file);
}

int main_0208_payload_dict_mock(int argc, char **argv) {
        (void)argc;
        (void)argv;

        do_test_payload_dict_mock();
        return 0;
}

#else

#include "test.h"

int main_0208_payload_dict_mock(int argc, char **argv) {
        (void)argc;
        (void)argv;
        TEST_SKIP("Test requires WITH_ZSTD support\n");
}

#endif
