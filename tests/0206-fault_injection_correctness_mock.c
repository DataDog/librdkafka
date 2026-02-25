/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Datadog Inc.
 * All rights reserved
 *
 *
 */


/**
 * Multibatch correctness under fault injection (mock cluster).
 *
 * This test exists to ensure that multibatch is "correct".
 * That it, is fundamentally writes all messages as-given,
 * and in the order given on a per-partition level.
 *
 * This test runs v1, v1+multibatch and v2 with a forced broker outage
 * during produce, then validates byte-level correctness invariants.
 */

#include "test.h"
#include "rdkafka.h"
#include "rdcrc32.h"
#include "../src/rdkafka_mock.h"
#include <math.h>
#include <sys/stat.h>
#include <time.h>


typedef enum { PROFILE_V1, PROFILE_V1_MB, PROFILE_V2 } profile_t;

#define FAULT_BROKER_ID         1
#define FAULT_DOWN_AT_PCT       25
#define FAULT_UP_AT_PCT         75
// These are the possible types of mismatches we codify
/* enum mismatch_type { */
/*       MM_PARSE_KEY, */
/*       MM_TESTID_MISMATCH, */
/*       MM_PARTITION_MISMATCH, */
/*       MM_MSGID_RANGE, */
/*       MM_DUPLICATE, */
/*       MM_PAYLOAD_LEN, */
/*       MM_PAYLOAD_BYTES, */
/*       MM_ORDER */
/* }; */

static const char *bench_producer_profile_name(profile_t producer_profile) {
        switch (producer_profile) {
        case PROFILE_V1:
                return "v1";
        case PROFILE_V1_MB:
                return "v1+multibatch";
        case PROFILE_V2:
                return "v2";
        default:
                return "unknown";
        }
}

static const char *bench_producer_profile_slug(profile_t producer_profile) {
        switch (producer_profile) {
        case PROFILE_V1:
                return "v1";
        case PROFILE_V1_MB:
                return "v1_multibatch";
        case PROFILE_V2:
                return "v2";
        default:
                return "unknown";
        }
}

static int outage_error_is_fatal_cb(rd_kafka_t *rk,
                                    rd_kafka_resp_err_t err,
                                    const char *reason) {
        (void)rk;

        if (err == RD_KAFKA_RESP_ERR__TRANSPORT ||
            err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                TEST_SAY("Ignoring expected outage error: %s: %s\n",
                         rd_kafka_err2name(err), reason);
                return 0;
        }

        return 1;
}

typedef enum {
        MSGLOG_COMPACT = 0,
        MSGLOG_PREVIEW,
        MSGLOG_FULL,
        MSGLOG_MISMATCH
} message_log_mode_t;

typedef struct {
        /* Commong config */
        int partition_cnt;
        int msg_cnt;
        int message_size_min;
        int message_size_max;
        char *output_dir;
        int log_each_message;
        message_log_mode_t log_mode;
        size_t log_preview_bytes;

        // int *controlled_rates;
        // int controlled_rate_cnt;
        // int controlled_duration_sec;
} bench_config_t;

typedef enum {
        MM_NONE          = 0,
        MM_PARSE_KEY     = 1 << 0,
        MM_TESTID        = 1 << 1,
        MM_MSGID_RANGE   = 1 << 2,
        MM_PARTITION     = 1 << 3,
        MM_ORDER         = 1 << 4,
        MM_PAYLOAD_LEN   = 1 << 5,
        MM_PAYLOAD_BYTES = 1 << 6,
        MM_KEY_LEN       = 1 << 7,
        MM_KEY_BYTES     = 1 << 8,
        MM_HEADER_CNT    = 1 << 9,
        MM_HEADER_NAME   = 1 << 10,
        MM_HEADER_LEN    = 1 << 11,
        MM_HEADER_BYTES  = 1 << 12,
        MM_OFFSET_ORDER  = 1 << 13,
        MM_OFFSET_GAP    = 1 << 14,
        MM_OFFSET_BASE   = 1 << 15
} mm_flags_t;

typedef struct {
        char *name;
        size_t name_len;
        unsigned char *value;
        size_t value_len;
} msg_header_t;

typedef struct {
        int msgid; /* -1 if parse/range failed */
        int32_t partition;
        int64_t offset;
        mm_flags_t flags;

        size_t key_len;
        unsigned char *key; /* full received key bytes */
        int first_key_diff_idx;

        size_t payload_len;
        unsigned char *payload; /* full received payload bytes */
        int first_payload_diff_idx;
} seen_record_t;

typedef struct {
        int msgid;
        int32_t expected_partition;

        size_t seen_cnt;
        size_t seen_cap;
        seen_record_t *seen; /* every observation for this msgid */

        rd_bool_t missing; /* true if seen_cnt == 0 at finalize */

        size_t key_len;
        unsigned char *key;

        size_t payload_len;
        unsigned char *payload;

        size_t header_cnt;
        msg_header_t *headers;

        int dr_seen_cnt;                  // exactly 1 expected
        rd_kafka_resp_err_t dr_last_err;  // last dr_err
} msg_verify_t;

typedef struct {
        int expected_msgs;
        int partition_cnt;
        int log_each_message;
        message_log_mode_t log_mode;
        size_t log_preview_bytes;


        size_t msg_size_min;
        size_t msg_size_max;
        uint64_t size_seed;
        size_t
            *size_by_msgid; /* [expected_msgs] - pre-generated message sizes */

        msg_verify_t *msgs; /* [expected_msgs] */


        size_t orphan_cnt;
        size_t orphan_cap;
        seen_record_t *orphans; /* parse fail/wrong testid/out-of-range */

        int *
            last_msgid_per_partition;  // init -1, for per-partition order check
        int *expected_per_partition;   // expected consumed messages per partition
        int *seen_per_partition;       // observed consumed messages per partition
        int64_t *first_offset_per_partition; // first consumed offset per partition
        int64_t *last_offset_per_partition;  // last consumed offset per partition


        // aggregate counters
        int produce_errors;
        int dr_errors;
        int dr_success;
        int dr_orphan;
        int dr_total;
        int parse_key_errors;
        int testid_mismatches;
        int msgid_range_errors;
        int partition_mismatches;
        int order_violations;
        int key_len_mismatches;
        int key_byte_mismatches;
        int payload_len_mismatches;
        int payload_byte_mismatches;
        int header_count_mismatches;
        int header_name_mismatches;
        int header_len_mismatches;
        int header_byte_mismatches;
        int offset_order_violations;
        int offset_gap_violations;
        int offset_base_violations;
        int partition_count_mismatches;
        int duplicates;
        int missing;

        int64_t produce_request_count;
        int64_t total_request_count;
} verify_state_t;

static void mm_flags_to_str(mm_flags_t flags, char *dst, size_t dst_size);
static void dump_bytes_hex(const char *label,
                           const unsigned char *bytes,
                           size_t len);
static void dump_bytes_preview(const char *label,
                               const unsigned char *bytes,
                               size_t len,
                               size_t max_bytes);
static uint32_t message_crc32(const void *bytes, size_t len);
static rd_bool_t should_log_this_message(const verify_state_t *vs,
                                         mm_flags_t flags);
static void log_verify_payloads(const verify_state_t *vs,
                                const msg_verify_t *m,
                                const seen_record_t *rec,
                                rd_bool_t has_expected);

typedef struct {
        verify_state_t *vs;
        int msgid;
} produce_opaque_t;

typedef struct {
        mtx_t lock;
        int initialized;
        int64_t last_total_requests;
        int64_t last_produce_requests;
        int64_t last_stats_ts_us;
} produce_req_stats_ctx_t;

static produce_req_stats_ctx_t g_produce_req_stats = {0};

static void produce_req_stats_ctx_ensure_init(void) {
        if (!g_produce_req_stats.initialized) {
                mtx_init(&g_produce_req_stats.lock, mtx_plain);
                g_produce_req_stats.initialized = 1;
        }
}

static void produce_req_stats_ctx_reset(void) {
        produce_req_stats_ctx_ensure_init();
        mtx_lock(&g_produce_req_stats.lock);
        g_produce_req_stats.last_total_requests   = 0;
        g_produce_req_stats.last_produce_requests = 0;
        g_produce_req_stats.last_stats_ts_us      = 0;
        mtx_unlock(&g_produce_req_stats.lock);
}

static void
produce_req_stats_ctx_snapshot(int64_t *total_requests,
                               int64_t *produce_requests,
                               int64_t *stats_ts_us) {
        produce_req_stats_ctx_ensure_init();
        mtx_lock(&g_produce_req_stats.lock);
        if (total_requests)
                *total_requests = g_produce_req_stats.last_total_requests;
        if (produce_requests)
                *produce_requests = g_produce_req_stats.last_produce_requests;
        if (stats_ts_us)
                *stats_ts_us = g_produce_req_stats.last_stats_ts_us;
        mtx_unlock(&g_produce_req_stats.lock);
}

static void stats_cb_produce_requests(rd_kafka_t *rk,
                                      const rd_kafka_stats_t *stats,
                                      void *opaque) {
        produce_req_stats_ctx_t *ctx = (produce_req_stats_ctx_t *)opaque;
        int64_t total_requests = 0;
        int64_t produce_requests = 0;
        uint32_t i;

        (void)rk;

        if (!ctx)
                return;

        for (i = 0; i < stats->broker_cnt; i++) {
                const rd_kafka_broker_stats_t *broker = &stats->brokers[i];
                uint32_t j;

                total_requests += broker->tx;
                for (j = 0; j < broker->req_cnt; j++) {
                        if (!strcmp(broker->reqs[j].name, "Produce"))
                                produce_requests += broker->reqs[j].count;
                }
        }

        mtx_lock(&ctx->lock);
        ctx->last_total_requests   = total_requests;
        ctx->last_produce_requests = produce_requests;
        ctx->last_stats_ts_us      = stats->ts_us;
        mtx_unlock(&ctx->lock);
}

static void parse_config(bench_config_t *config) {
        const char *val;

        config->partition_cnt    = 64;
        config->message_size_min = 256;
        config->message_size_max = 10000;
        config->msg_cnt          = 10000;
        config->log_each_message = 0;
        config->log_mode         = MSGLOG_COMPACT;
        config->log_preview_bytes = 24;

        if ((val = test_getenv("MAX_PARTITIONS", NULL)))
                config->partition_cnt = atoi(val);
        if ((val = test_getenv("MSG_CNT", NULL)))
                config->msg_cnt = atoi(val);
        if ((val = test_getenv("LOG_EACH_MSG", NULL)))
                config->log_each_message = !!atoi(val);
        if ((val = test_getenv("LOG_PREVIEW_BYTES", NULL))) {
                int preview = atoi(val);
                if (preview > 0)
                        config->log_preview_bytes = (size_t)preview;
        }
        if ((val = test_getenv("LOG_EACH_MSG_MODE", NULL))) {
                if (!strcmp(val, "compact"))
                        config->log_mode = MSGLOG_COMPACT;
                else if (!strcmp(val, "preview"))
                        config->log_mode = MSGLOG_PREVIEW;
                else if (!strcmp(val, "full"))
                        config->log_mode = MSGLOG_FULL;
                else if (!strcmp(val, "mismatch"))
                        config->log_mode = MSGLOG_MISMATCH;
                else
                        TEST_FAIL("Invalid LOG_EACH_MSG_MODE '%s' (expected: "
                                  "compact|preview|full|mismatch)",
                                  val);
        }

        // TODO, allow more configuration later
}

// simple deterministic PRNG (full byte range capable)
static uint64_t splitmix64_next(uint64_t *s) {
        uint64_t z = (*s += 0x9e3779b97f4a7c15ULL);
        z          = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
        z          = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
        return z ^ (z >> 31);
}

// fill [0..len) with deterministic pseudo-random bytes 0x00..0xFF
static void fill_random_bytes(unsigned char *dst, size_t len, uint64_t seed) {
        uint64_t s = seed;
        size_t i   = 0;
        while (i < len) {
                uint64_t r = splitmix64_next(&s);
                for (int b = 0; b < 8 && i < len; b++, i++) {
                        dst[i] = (unsigned char)((r >> (8 * b)) & 0xff);
                }
        }
}

static void fill_random_bytes_stored(unsigned char *dst, size_t len) {
        for (size_t i = 0; i < len; i++)
                dst[i] = (unsigned char)(rand() & 0xff);
}

static rd_kafka_headers_t *
msg_headers_build_for_produce(const msg_verify_t *m) {
        rd_kafka_headers_t *hdrs = rd_kafka_headers_new(m->header_cnt);

        for (size_t i = 0; i < m->header_cnt; i++) {
                const msg_header_t *h = &m->headers[i];
                rd_kafka_resp_err_t err =
                    rd_kafka_header_add(hdrs, h->name, -1, h->value, h->value_len);
                TEST_ASSERT(!err,
                            "rd_kafka_header_add failed for name=%s len=%zu: %s",
                            h->name, h->value_len, rd_kafka_err2str(err));
        }

        return hdrs;
}

static void verify_message_headers(verify_state_t *vs,
                                   msg_verify_t *m,
                                   const rd_kafka_message_t *rkmsg,
                                   seen_record_t *rec) {
        rd_kafka_headers_t *hdrs = NULL;
        rd_kafka_resp_err_t err;
        size_t got_cnt;
        size_t cmp_cnt;

        err = rd_kafka_message_headers(rkmsg, &hdrs);
        if (err) {
                rec->flags |= MM_HEADER_CNT;
                vs->header_count_mismatches++;
                return;
        }

        got_cnt = rd_kafka_header_cnt(hdrs);
        if (got_cnt != m->header_cnt) {
                rec->flags |= MM_HEADER_CNT;
                vs->header_count_mismatches++;
        }

        cmp_cnt = got_cnt < m->header_cnt ? got_cnt : m->header_cnt;
        for (size_t i = 0; i < cmp_cnt; i++) {
                const char *got_name = NULL;
                const void *got_value = NULL;
                size_t got_value_len = 0;
                const msg_header_t *exp = &m->headers[i];

                err = rd_kafka_header_get_all(hdrs, i, &got_name, &got_value,
                                              &got_value_len);
                if (err) {
                        rec->flags |= MM_HEADER_CNT;
                        vs->header_count_mismatches++;
                        break;
                }

                if (!got_name || strcmp(got_name, exp->name)) {
                        rec->flags |= MM_HEADER_NAME;
                        vs->header_name_mismatches++;
                }

                if (got_value_len != exp->value_len) {
                        rec->flags |= MM_HEADER_LEN;
                        vs->header_len_mismatches++;
                } else if (got_value_len > 0 &&
                           (!got_value ||
                            memcmp(got_value, exp->value, exp->value_len) !=
                                0)) {
                        rec->flags |= MM_HEADER_BYTES;
                        vs->header_byte_mismatches++;
                }
        }
}

static verify_state_t *init_verification_state(bench_config_t *config,
                                               uint64_t testid) {
        verify_state_t *vs;
        vs = calloc(1, sizeof(verify_state_t));

        int msgs_to_produce = config->msg_cnt;
        vs->size_by_msgid   = malloc(sizeof(size_t) * msgs_to_produce);
        vs->msg_size_min    = config->message_size_min;
        vs->msg_size_max    = config->message_size_max;
        vs->expected_msgs   = msgs_to_produce;
        vs->log_each_message = config->log_each_message;
        vs->log_mode        = config->log_mode;
        vs->log_preview_bytes = config->log_preview_bytes;

        int partition_cnt = config->partition_cnt;
        vs->partition_cnt = partition_cnt;
        vs->msgs          = calloc(msgs_to_produce, sizeof(*vs->msgs));
        vs->last_msgid_per_partition =
            malloc(sizeof(int) * partition_cnt);  // IS this right?
        vs->expected_per_partition = calloc((size_t)partition_cnt, sizeof(int));
        vs->seen_per_partition     = calloc((size_t)partition_cnt, sizeof(int));
        vs->first_offset_per_partition =
            malloc(sizeof(*vs->first_offset_per_partition) * (size_t)partition_cnt);
        vs->last_offset_per_partition =
            malloc(sizeof(*vs->last_offset_per_partition) * (size_t)partition_cnt);
        TEST_ASSERT(vs->last_msgid_per_partition && vs->expected_per_partition &&
                        vs->seen_per_partition && vs->first_offset_per_partition &&
                        vs->last_offset_per_partition,
                    "OOM allocating per-partition verification arrays");
        for (int i = 0; i < partition_cnt; i++) {
                vs->last_msgid_per_partition[i] = -1;
                vs->first_offset_per_partition[i] = -1;
                vs->last_offset_per_partition[i]  = -1;
        }

        vs->size_seed    = (uint64_t)time(NULL);
        int msg_size_max = config->message_size_max;
        int msg_size_min = config->message_size_min;
        for (int msgid = 0; msgid < msgs_to_produce; msgid++) {
                msg_verify_t *m = &vs->msgs[msgid];
                int32_t part    = (int32_t)(msgid % partition_cnt);
                uint64_t r      = splitmix64_next(&vs->size_seed);
                size_t sz =
                    (size_t)msg_size_min +
                    (size_t)(r % (uint64_t)(msg_size_max - msg_size_min + 1));

                m->msgid              = msgid;
                m->expected_partition = part;
                vs->expected_per_partition[part]++;

                // ----------------
                // generate a key and store it
                char keybuf[128];
                int klen   = rd_snprintf(keybuf, sizeof(keybuf),
                                         "testid=%" PRIu64 ",partition=%" PRId32
                                         ",msg=%d\n",
                                         testid, part, msgid);
                m->key_len = klen;
                m->key     = malloc(m->key_len);
                memcpy(m->key, keybuf, m->key_len);

                // -------------
                // generate a payload and store it
                m->payload_len           = sz;
                m->payload               = malloc(sz);
                vs->size_by_msgid[msgid] = sz;

                // seed ties payload uniquely to this message identity
                // this fancy stuff is codex generated. TODO: explain how it
                // works
                uint64_t seed = ((uint64_t)msgid << 32) ^ (uint64_t)part ^
                                (testid * 0x9e3779b97f4a7c15ULL);
                fill_random_bytes(m->payload, sz, seed);

                m->header_cnt = 2 + (size_t)(rand() % 3); /* 2..4 headers */
                m->headers    = calloc(m->header_cnt, sizeof(*m->headers));
                TEST_ASSERT(m->headers, "OOM allocating headers");

                for (size_t h = 0; h < m->header_cnt; h++) {
                        msg_header_t *mh = &m->headers[h];
                        char namebuf[64];
                        int n = rd_snprintf(namebuf, sizeof(namebuf),
                                            "h%02zu_msg%05d", h, msgid);
                        size_t val_len = 8 + (size_t)(rand() % 57); /* 8..64 */

                        TEST_ASSERT(n > 0, "header name generation failed");
                        mh->name_len = (size_t)n;
                        mh->name     = malloc(mh->name_len + 1);
                        TEST_ASSERT(mh->name, "OOM allocating header name");
                        memcpy(mh->name, namebuf, mh->name_len + 1);

                        mh->value_len = val_len;
                        mh->value     = malloc(mh->value_len);
                        TEST_ASSERT(mh->value, "OOM allocating header value");
                        fill_random_bytes_stored(mh->value, mh->value_len);
                }
        }
        return vs;
}

static void
dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmsg, void *opaque) {
        produce_opaque_t *op = (produce_opaque_t *)rkmsg->_private;
        (void)rk;
        (void)opaque;

        if (!op) {
                return;  // shouldn't happen, LOG
        }

        verify_state_t *vs = op->vs;
        int msgid          = op->msgid;
        if (!vs || msgid < 0 || msgid >= vs->expected_msgs) {
                if (vs)
                        vs->dr_orphan++;
                free(op);
                return;
        }

        msg_verify_t *m = &vs->msgs[msgid];
        m->dr_seen_cnt++;
        m->dr_last_err = rkmsg->err;


        vs->dr_total++;
        if (rkmsg->err)
                op->vs->dr_errors++;
        else
                op->vs->dr_success++;

        free(op);
}

/**
 * Create producer with standard configuration
 */
static rd_kafka_t *create_producer(bench_config_t *config,
                                   const char *topic,
                                   const char *bootstrap_servers,
                                   profile_t producer_profile) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        const char *val;
        const char *v1_linger_ms;

        (void)topic;

        test_conf_init(&conf, NULL, 120);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);
        /* Use typed stats callback only - disable JSON callback set by
         * test_conf_init to avoid double rd_avg_rollover consumption */
        rd_kafka_conf_set_stats_cb(conf, NULL);
        produce_req_stats_ctx_ensure_init();
        rd_kafka_conf_set_opaque(conf, &g_produce_req_stats);
        rd_kafka_conf_set_stats_cb_typed(conf, stats_cb_produce_requests);

        /* Standard MultiBatch configuration */
        test_conf_set(conf, "statistics.interval.ms", "50");
        test_conf_set(conf, "queue.buffering.max.messages", "1000000");
        test_conf_set(conf, "queue.buffering.max.kbytes", "102400"); /* 100MB */
        test_conf_set(conf, "compression.type", "lz4");
        test_conf_set(conf, "message.max.bytes", "100000000"); /* 100MB */
        test_conf_set(conf, "message.timeout.ms", "180000");
        test_conf_set(conf, "batch.num.messages", "100000");
        if (producer_profile == PROFILE_V2) {
                test_conf_set(conf, "produce.engine", "v2");
                /* Broker-level batching configuration (new) */
                test_conf_set(conf, "broker.linger.ms", "500"); /* Default: 5ms */
                /* test_conf_set(conf, "produce.request.max.partitions",
                 * "10000"); */
        } else {
                test_conf_set(conf, "produce.engine", "v1");
                test_conf_set(conf, "multibatch",
                              producer_profile == PROFILE_V1_MB ? "true"
                                                                : "false");

                /* v1 path uses queue.buffering.max.ms for linger behavior. */
                v1_linger_ms =
                    test_getenv("BENCH_V1_LINGER_MS",
                                test_getenv("BROKER_LINGER_MS", "500"));
                test_conf_set(conf, "queue.buffering.max.ms", v1_linger_ms);
        }


        /* Allow broker.linger.ms override via environment */
        if ((val = test_getenv("BROKER_LINGER_MS", NULL)))
                test_conf_set(conf, "broker.linger.ms", val);

        /* Allow broker.batch.max.bytes override via environment
         * -1 = disabled (default), use broker.linger.ms only */
        test_conf_set(conf, "broker.batch.max.bytes", "10000000"); /* 10MB */
        if ((val = test_getenv("BROKER_BATCH_MAX_BYTES", NULL)))
                test_conf_set(conf, "broker.batch.max.bytes", val);

        /* Allow produce.request.max.partitions override via environment (v2
         * only) */
        if (producer_profile == PROFILE_V2 &&
            (val = test_getenv("MAX_PARTITIONS", NULL))) {
                test_conf_set(conf, "produce.request.max.partitions", val);
        }

        rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
        return rk;
}


static void append_seen(msg_verify_t *m, const seen_record_t *rec) {
        if (m->seen_cnt == m->seen_cap) {
                size_t new_cap = m->seen_cap ? m->seen_cap * 2 : 64;
                seen_record_t *tmp =
                    realloc(m->seen, new_cap * sizeof(*m->seen));
                TEST_ASSERT(tmp, "OOM growing seen array");
                m->seen     = tmp;
                m->seen_cap = new_cap;
        }
        m->seen[m->seen_cnt++] = *rec;  // shallow copy
}
static void append_orphan(verify_state_t *vs, seen_record_t *rec) {
        if (vs->orphan_cnt == vs->orphan_cap) {
                size_t new_cap = vs->orphan_cap ? vs->orphan_cap * 2 : 64;
                seen_record_t *tmp =
                    realloc(vs->orphans, new_cap * sizeof(*vs->orphans));
                TEST_ASSERT(tmp, "OOM growing orphan array");
                vs->orphans    = tmp;
                vs->orphan_cap = new_cap;
        }
        vs->orphans[vs->orphan_cnt++] = *rec;  // shallow copy of struct
}



static void run_produce_phase(bench_config_t *config,
                              verify_state_t *vs,
                              profile_t producer_profile,
                              rd_kafka_mock_cluster_t *mcluster,
                              const char *topic,
                              const char *bootstrap_servers,
                              int rate_msgs_sec) {
        int (*prev_is_fatal_cb)(rd_kafka_t *rk,
                                rd_kafka_resp_err_t err,
                                const char *reason);
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        int64_t start_ts, end_ts;
        int64_t start_total_requests   = 0;
        int64_t start_produce_requests = 0;
        int64_t start_stats_ts_us      = 0;
        int64_t end_total_requests     = 0;
        int64_t end_produce_requests   = 0;
        int64_t end_stats_ts_us        = 0;
        int msg_success = 0;
        int msg_fails   = 0;
        int target_msgs = vs->expected_msgs;
        int down_at_msg = (target_msgs * FAULT_DOWN_AT_PCT) / 100;
        int up_at_msg   = (target_msgs * FAULT_UP_AT_PCT) / 100;
        rd_bool_t broker_down = rd_false;
        rd_bool_t broker_up   = rd_false;

        TEST_SAY("\n========================================\n");
        TEST_SAY("PRODUCE PHASE\n");
        TEST_SAY("========================================\n");
        TEST_SAY("Producer profile: %s\n",
                 bench_producer_profile_name(producer_profile));
        TEST_SAY("Target messages: %d\n", target_msgs);
        TEST_SAY("Partitions: %d\n", vs->partition_cnt);
        TEST_SAY("Message size: %zu-%zu bytes\n", vs->msg_size_min,
                 vs->msg_size_max);
        TEST_SAY("========================================\n\n");

        prev_is_fatal_cb       = test_curr->is_fatal_cb;
        test_curr->is_fatal_cb = outage_error_is_fatal_cb;

        produce_req_stats_ctx_reset();
        rk  = create_producer(config, topic, bootstrap_servers, producer_profile);
        rkt = test_create_producer_topic(rk, topic, "acks", "-1", NULL);

        for (int i = 0; i < 3; i++)
                rd_kafka_poll(rk, 50);
        produce_req_stats_ctx_snapshot(&start_total_requests,
                                       &start_produce_requests,
                                       &start_stats_ts_us);

        start_ts = test_clock();
        int64_t interval_us =
            rate_msgs_sec > 0 ? (1000000 / rate_msgs_sec) : 0;
        int64_t next_send_ts = start_ts;

        for (int i = 0; i < vs->expected_msgs; i++) {
                if (!broker_down && i >= down_at_msg) {
                        rd_kafka_resp_err_t ferr =
                            rd_kafka_mock_broker_set_down(mcluster,
                                                          FAULT_BROKER_ID);
                        TEST_ASSERT(!ferr, "Failed to bring broker %d down: %s",
                                    FAULT_BROKER_ID, rd_kafka_err2str(ferr));
                        broker_down = rd_true;
                        TEST_SAY("Fault injection: broker %d DOWN at msg %d\n",
                                 FAULT_BROKER_ID, i);
                }

                if (broker_down && !broker_up && i >= up_at_msg) {
                        rd_kafka_resp_err_t ferr =
                            rd_kafka_mock_broker_set_up(mcluster, FAULT_BROKER_ID);
                        TEST_ASSERT(!ferr, "Failed to bring broker %d up: %s",
                                    FAULT_BROKER_ID, rd_kafka_err2str(ferr));
                        broker_up = rd_true;
                        TEST_SAY("Fault injection: broker %d UP at msg %d\n",
                                 FAULT_BROKER_ID, i);
                }

                // Rate limiting: wait until next time to send message
                int64_t now = test_clock();
                if (now < next_send_ts) {
                        int64_t sleep_us = next_send_ts - now;
                        if (sleep_us > 0 && sleep_us < 1000000) {
                                usleep((useconds_t)sleep_us);
                        }
                }



                msg_verify_t *m      = &vs->msgs[i];
                produce_opaque_t *op = malloc(sizeof(*op));
                rd_kafka_headers_t *hdrs;
                rd_kafka_resp_err_t err_send;
                op->vs               = vs;
                op->msgid            = m->msgid;

                hdrs = msg_headers_build_for_produce(m);
                err_send =
                    rd_kafka_producev(rk, RD_KAFKA_V_RKT(rkt),
                                      RD_KAFKA_V_PARTITION(m->expected_partition),
                                      RD_KAFKA_V_VALUE(m->payload, m->payload_len),
                                      RD_KAFKA_V_KEY(m->key, m->key_len),
                                      RD_KAFKA_V_HEADERS(hdrs),
                                      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                      RD_KAFKA_V_OPAQUE(op), RD_KAFKA_V_END);
                if (err_send) {
                        rd_kafka_headers_destroy(hdrs);
                        free(op);
                        msg_fails++;
                } else {
                        msg_success++;
                }

                if (config->log_each_message) {
                        uint32_t payload_crc =
                            message_crc32(m->payload, m->payload_len);
                        TEST_SAY(
                            "PRODUCE msgid=%d partition=%" PRId32
                            " key_len=%zu payload_len=%zu headers=%zu "
                            "payload_crc=%08x "
                            "status=%s\n",
                            m->msgid, m->expected_partition, m->key_len,
                            m->payload_len, m->header_cnt,
                            (unsigned int)payload_crc,
                            err_send ? "enqueue_failed" : "enqueued");
                }

                if (i % 100 == 0) {
                        rd_kafka_poll(rk, 0);
                }

                if (interval_us > 0)
                        next_send_ts += interval_us;
        }

        if (broker_down && !broker_up) {
                rd_kafka_resp_err_t ferr =
                    rd_kafka_mock_broker_set_up(mcluster, FAULT_BROKER_ID);
                TEST_ASSERT(!ferr, "Failed to bring broker %d up at finalize: %s",
                            FAULT_BROKER_ID, rd_kafka_err2str(ferr));
                broker_up = rd_true;
                TEST_SAY("Fault injection: broker %d UP at finalize\n",
                         FAULT_BROKER_ID);
        }

        TEST_ASSERT(broker_down, "Fault injection never brought broker down");
        TEST_ASSERT(broker_up, "Fault injection never brought broker back up");

        TEST_SAY("Waiting for delivery confirmations....\n");
        int remains = rd_kafka_flush(rk, 60000);
        end_ts = test_clock();

        /* Get final stats */
        for (int i = 0; i < 5; i++)
                rd_kafka_poll(rk, 100);

        {
                int64_t stats_deadline = test_clock() + tmout_multip(3000) * 1000;
                while (test_clock() < stats_deadline) {
                        rd_kafka_poll(rk, 100);
                        produce_req_stats_ctx_snapshot(
                            &end_total_requests, &end_produce_requests,
                            &end_stats_ts_us);
                        if (end_stats_ts_us > start_stats_ts_us)
                                break;
                }
        }

        vs->produce_errors += msg_fails;
        if (remains > 0)
                vs->produce_errors += remains;

        vs->total_request_count = end_total_requests - start_total_requests;
        if (vs->total_request_count < 0)
                vs->total_request_count = 0;

        vs->produce_request_count = end_produce_requests - start_produce_requests;
        if (vs->produce_request_count < 0)
                vs->produce_request_count = 0;

        TEST_ASSERT(msg_success == 0 || vs->produce_request_count > 0,
                    "No Produce requests observed via stats callback, "
                    "start=%" PRId64 " end=%" PRId64 " start_ts=%" PRId64
                    " end_ts=%" PRId64,
                    start_produce_requests, end_produce_requests,
                    start_stats_ts_us, end_stats_ts_us);

        TEST_SAY(
            "Produce phase summary (%s): success=%d failed=%d dr_total=%d "
            "flush_remains=%d duration=%.2fs produce_reqs=%" PRId64
            " total_reqs=%" PRId64 " msgs/produce_req=%.2f\n",
            bench_producer_profile_name(producer_profile), msg_success, msg_fails,
            vs->dr_total, remains, (double)(end_ts - start_ts) / 1000000.0,
            vs->produce_request_count, vs->total_request_count,
            vs->produce_request_count
                ? (double)msg_success / (double)vs->produce_request_count
                : 0.0);

        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);
        test_curr->is_fatal_cb = prev_is_fatal_cb;
}

static int first_diff(const unsigned char *expected,
                      const unsigned char *got,
                      size_t expected_len,
                      size_t got_len) {
        size_t min_len = expected_len < got_len ? expected_len : got_len;
        for (size_t i = 0; i < min_len; i++) {
                if (expected[i] != got[i])
                        return (int)i;
        }
        return -1;
}

static rd_bool_t parse_key_bytes(const void *key,
                                 size_t key_len,
                                 uint64_t *testidp,
                                 int32_t *partitionp,
                                 int *msgidp) {
        char buf[256];
        uint64_t testid;
        int partition;
        int msgid;
        int n = 0;
        size_t parse_len;

        if (!key || !key_len || key_len >= sizeof(buf) || !testidp ||
            !partitionp || !msgidp)
                return rd_false;

        memcpy(buf, key, key_len);
        buf[key_len] = '\0';
        parse_len    = key_len;

        /* Generated keys include '\n', accept both with and without it. */
        if (parse_len > 0 && buf[parse_len - 1] == '\n') {
                buf[parse_len - 1] = '\0';
                parse_len--;
        }

        if (sscanf(buf, "testid=%" SCNu64 ",partition=%d,msg=%d%n", &testid,
                   &partition, &msgid, &n) != 3)
                return rd_false;

        if ((size_t)n != parse_len)
                return rd_false;

        *testidp    = testid;
        *partitionp = (int32_t)partition;
        *msgidp     = msgid;
        return rd_true;
}

static rd_bool_t verify_consumed_message(verify_state_t *vs,
                                         uint64_t expected_testid,
                                         const rd_kafka_message_t *rkmsg) {

        // initialize state
        seen_record_t rec          = {0};
        rec.msgid                  = -1;
        rec.partition              = rkmsg->partition;
        rec.offset                 = rkmsg->offset;
        rec.flags                  = MM_NONE;
        rec.first_key_diff_idx     = -1;
        rec.first_payload_diff_idx = -1;

        // Always capture full received bytes for debugging
        rec.key_len = rkmsg->key_len;
        rec.key     = malloc(rec.key_len);
        memcpy(rec.key, rkmsg->key, rkmsg->key_len);
        rec.payload_len = rkmsg->len;
        rec.payload     = malloc(rec.payload_len);
        memcpy(rec.payload, rkmsg->payload, rkmsg->len);

        // Parse key: "testid=<u64>,partition=<i32>,msg=<int>\n"
        uint64_t in_testid;
        int32_t in_part;
        int in_msgid;
        if (!parse_key_bytes(rkmsg->key, rkmsg->key_len, &in_testid, &in_part,
                             &in_msgid)) {
                rec.flags |= MM_PARSE_KEY;
                if (should_log_this_message(vs, rec.flags)) {
                        uint32_t got_crc =
                            message_crc32(rec.payload, rec.payload_len);
                        char flags[160];
                        mm_flags_to_str(rec.flags, flags, sizeof(flags));
                        TEST_SAY(
                            "VERIFY msgid=<parse-failed> "
                            "got(part=%" PRId32 ",offset=%" PRId64
                            ",key=%zu,payload=%zu,payload_crc=%08x) "
                            "result=MISMATCH flags=%s\n",
                            rkmsg->partition, rkmsg->offset, rkmsg->key_len,
                            rkmsg->len, (unsigned int)got_crc, flags);
                        log_verify_payloads(vs, NULL, &rec, rd_false);
                }
                append_orphan(vs, &rec);
                vs->parse_key_errors++;
                return rd_false;
        }

        rec.msgid = in_msgid;
        if (in_testid != expected_testid)
                rec.flags |= MM_TESTID;
        if (in_msgid < 0 || in_msgid >= vs->expected_msgs)
                rec.flags |= MM_MSGID_RANGE;

        // Range/testid failures can't be indexed safely
        if (rec.flags & (MM_TESTID | MM_MSGID_RANGE)) {
                if (should_log_this_message(vs, rec.flags)) {
                        uint32_t got_crc =
                            message_crc32(rec.payload, rec.payload_len);
                        char flags[160];
                        mm_flags_to_str(rec.flags, flags, sizeof(flags));
                        TEST_SAY(
                            "VERIFY msgid=%d expected_testid=%" PRIu64
                            " got(testid=%" PRIu64 ",part=%" PRId32
                            ",offset=%" PRId64 ",key=%zu,payload=%zu"
                            ",payload_crc=%08x) "
                            "result=MISMATCH flags=%s\n",
                            in_msgid, expected_testid, in_testid,
                            rkmsg->partition, rkmsg->offset, rkmsg->key_len,
                            rkmsg->len, (unsigned int)got_crc, flags);
                        log_verify_payloads(vs, NULL, &rec, rd_false);
                }
                append_orphan(vs, &rec);
                if (rec.flags & MM_TESTID)
                        vs->testid_mismatches++;
                if (rec.flags & MM_MSGID_RANGE)
                        vs->msgid_range_errors++;
                return rd_false;
        }

        msg_verify_t *m = &vs->msgs[in_msgid];

        // Partition must match expected partition
        if (rkmsg->partition != m->expected_partition) {
                rec.flags |= MM_PARTITION;
                vs->partition_mismatches++;
        }

        if (rkmsg->partition >= 0 && rkmsg->partition < vs->partition_cnt) {
                int32_t p = rkmsg->partition;
                int64_t prev_off = vs->last_offset_per_partition[p];
                vs->seen_per_partition[p]++;

                if (vs->first_offset_per_partition[p] == -1) {
                        vs->first_offset_per_partition[p] = rkmsg->offset;
                        if (rkmsg->offset != 0) {
                                rec.flags |= MM_OFFSET_BASE;
                                vs->offset_base_violations++;
                        }
                } else {
                        if (rkmsg->offset <= prev_off) {
                                rec.flags |= MM_OFFSET_ORDER;
                                vs->offset_order_violations++;
                        } else if (rkmsg->offset != prev_off + 1) {
                                rec.flags |= MM_OFFSET_GAP;
                                vs->offset_gap_violations++;
                        }
                }

                if (rkmsg->offset > vs->last_offset_per_partition[p])
                        vs->last_offset_per_partition[p] = rkmsg->offset;
        }

        // Now do a per-partition order check
        if (!(rec.flags & MM_MSGID_RANGE)) {
                if (rkmsg->partition >= 0 && rkmsg->partition < vs->partition_cnt) {
                        int prev = vs->last_msgid_per_partition[rkmsg->partition];
                        if (prev != -1 && in_msgid <= prev) {
                                rec.flags |= MM_ORDER;
                                vs->order_violations++;
                        } else {
                                vs->last_msgid_per_partition[rkmsg->partition] =
                                    in_msgid;
                        }
                }
        }

        // Exact key compare
        if (rkmsg->key_len != m->key_len) {
                rec.flags |= MM_KEY_LEN;
                vs->key_len_mismatches++;
        } else if (memcmp(rkmsg->key, m->key, m->key_len) != 0) {
                rec.flags |= MM_KEY_BYTES;
                vs->key_byte_mismatches++;
                rec.first_key_diff_idx =
                    first_diff((const unsigned char *)m->key,
                               (const unsigned char *)rkmsg->key, m->key_len,
                               rkmsg->key_len);
        }

        // Exact payload compare
        if ((size_t)rkmsg->len != m->payload_len) {
                rec.flags |= MM_PAYLOAD_LEN;
                vs->payload_len_mismatches++;
        } else if (memcmp(rkmsg->payload, m->payload, m->payload_len) != 0) {
                rec.flags |= MM_PAYLOAD_BYTES;
                rec.first_payload_diff_idx =
                    first_diff((const unsigned char *)m->payload,
                               (const unsigned char *)rkmsg->payload,
                               m->payload_len, rkmsg->len);
        }

        verify_message_headers(vs, m, rkmsg, &rec);

        if (should_log_this_message(vs, rec.flags)) {
                uint32_t expected_payload_crc =
                    message_crc32(m->payload, m->payload_len);
                uint32_t got_payload_crc =
                    message_crc32(rec.payload, rec.payload_len);
                char flags[160];
                mm_flags_to_str(rec.flags, flags, sizeof(flags));
                TEST_SAY(
                    "VERIFY msgid=%d expected(part=%" PRId32
                    ",key=%zu,payload=%zu,payload_crc=%08x) got(part=%" PRId32
                    ",offset=%" PRId64 ",key=%zu,payload=%zu,payload_crc=%08x) "
                    "result=%s flags=%s key_diff=%d payload_diff=%d headers=%zu\n",
                    in_msgid, m->expected_partition, m->key_len, m->payload_len,
                    (unsigned int)expected_payload_crc, rkmsg->partition,
                    rkmsg->offset, rkmsg->key_len, rkmsg->len,
                    (unsigned int)got_payload_crc,
                    rec.flags == MM_NONE ? "OK" : "MISMATCH", flags,
                    rec.first_key_diff_idx, rec.first_payload_diff_idx,
                    m->header_cnt);
                log_verify_payloads(vs, m, &rec, rd_true);
        }

        rd_bool_t first_seen = (m->seen_cnt == 0);
        append_seen(m, &rec);  // grows m->seen[], copies rec ownership
        return first_seen;
}

static void destroy_seen_record(seen_record_t *rec) {
        if (!rec)
                return;
        free(rec->key);
        free(rec->payload);
        rec->key         = NULL;
        rec->payload     = NULL;
        rec->key_len     = 0;
        rec->payload_len = 0;
}

static void destroy_verify_state(verify_state_t *vs) {
        if (!vs)
                return;

        if (vs->msgs) {
                for (int i = 0; i < vs->expected_msgs; i++) {
                        msg_verify_t *m = &vs->msgs[i];

                        free(m->key);
                        free(m->payload);
                        for (size_t h = 0; h < m->header_cnt; h++) {
                                free(m->headers[h].name);
                                free(m->headers[h].value);
                                m->headers[h].name      = NULL;
                                m->headers[h].value     = NULL;
                                m->headers[h].name_len  = 0;
                                m->headers[h].value_len = 0;
                        }
                        free(m->headers);
                        m->headers    = NULL;
                        m->header_cnt = 0;

                        for (size_t j = 0; j < m->seen_cnt; j++)
                                destroy_seen_record(&m->seen[j]);

                        free(m->seen);
                        m->seen     = NULL;
                        m->seen_cnt = 0;
                        m->seen_cap = 0;
                }
        }

        for (size_t i = 0; i < vs->orphan_cnt; i++)
                destroy_seen_record(&vs->orphans[i]);
        vs->orphan_cnt = 0;
        vs->orphan_cap = 0;

        free(vs->orphans);
        free(vs->msgs);
        free(vs->size_by_msgid);
        free(vs->last_msgid_per_partition);
        free(vs->expected_per_partition);
        free(vs->seen_per_partition);
        free(vs->first_offset_per_partition);
        free(vs->last_offset_per_partition);
        free(vs);
}

static rd_kafka_t *create_verify_consumer_0206(const char *topic,
                                               const char *group_id,
                                               int part_cnt,
                                               const char *bootstrap_servers) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        test_conf_init(&conf, NULL, 120);
        test_conf_set(conf, "bootstrap.servers", bootstrap_servers);

        // Force deterministic behavior
        test_conf_set(conf, "group.id", group_id);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "enable.auto.offset.store", "false");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.partition.eof", "true");

        rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

        // Use the consumer API poll path
        rd_kafka_poll_set_consumer(rk);

        // Assign exact partitions
        rd_kafka_topic_partition_list_t *parts =
            rd_kafka_topic_partition_list_new(part_cnt);
        for (int p = 0; p < part_cnt; p++) {
                rd_kafka_topic_partition_t *tp =
                    rd_kafka_topic_partition_list_add(parts, topic, p);
                tp->offset = RD_KAFKA_OFFSET_BEGINNING;
        }

        test_consumer_assign("0206.assign", rk, parts);
        rd_kafka_topic_partition_list_destroy(parts);
        return rk;
}

static void mm_flags_to_str(mm_flags_t flags, char *dst, size_t dst_size) {
        size_t of  = 0;
        int first = 1;

        if (!dst || dst_size == 0)
                return;

        dst[0] = '\0';

        if (flags == MM_NONE) {
                rd_snprintf(dst, dst_size, "none");
                return;
        }

#define APPEND_FLAG(_f, _name)                                                 \
        do {                                                                   \
                if ((flags & (_f)) != 0 && of < dst_size) {                    \
                        int n = rd_snprintf(dst + of, dst_size - of, "%s%s",   \
                                            first ? "" : "|", (_name));         \
                        if (n > 0)                                              \
                                of += (size_t)n;                                \
                        first = 0;                                              \
                }                                                              \
        } while (0)

        APPEND_FLAG(MM_PARSE_KEY, "PARSE_KEY");
        APPEND_FLAG(MM_TESTID, "TESTID");
        APPEND_FLAG(MM_MSGID_RANGE, "MSGID_RANGE");
        APPEND_FLAG(MM_PARTITION, "PARTITION");
        APPEND_FLAG(MM_ORDER, "ORDER");
        APPEND_FLAG(MM_PAYLOAD_LEN, "PAYLOAD_LEN");
        APPEND_FLAG(MM_PAYLOAD_BYTES, "PAYLOAD_BYTES");
        APPEND_FLAG(MM_KEY_LEN, "KEY_LEN");
        APPEND_FLAG(MM_KEY_BYTES, "KEY_BYTES");
        APPEND_FLAG(MM_HEADER_CNT, "HEADER_CNT");
        APPEND_FLAG(MM_HEADER_NAME, "HEADER_NAME");
        APPEND_FLAG(MM_HEADER_LEN, "HEADER_LEN");
        APPEND_FLAG(MM_HEADER_BYTES, "HEADER_BYTES");
        APPEND_FLAG(MM_OFFSET_ORDER, "OFFSET_ORDER");
        APPEND_FLAG(MM_OFFSET_GAP, "OFFSET_GAP");
        APPEND_FLAG(MM_OFFSET_BASE, "OFFSET_BASE");

#undef APPEND_FLAG
}

static void dump_bytes_hex(const char *label,
                           const unsigned char *bytes,
                           size_t len) {
        TEST_SAY("    %s (%zu bytes)\n", label, len);

        if (len == 0)
                return;

        if (!bytes) {
                TEST_SAY("      <NULL>\n");
                return;
        }

        for (size_t i = 0; i < len; i++) {
                if ((i % 16) == 0)
                        TEST_SAY("      %06zu: ", i);

                TEST_SAY("%02x", (unsigned int)bytes[i]);

                if ((i % 16) == 15 || i == len - 1)
                        TEST_SAY("\n");
                else
                        TEST_SAY(" ");
        }
}

static void dump_bytes_preview(const char *label,
                               const unsigned char *bytes,
                               size_t len,
                               size_t max_bytes) {
        size_t show = len < max_bytes ? len : max_bytes;

        TEST_SAY("    %s (%zu bytes, showing %zu)\n", label, len, show);

        if (len == 0)
                return;

        if (!bytes) {
                TEST_SAY("      <NULL>\n");
                return;
        }

        TEST_SAY("      hex: ");
        for (size_t i = 0; i < show; i++) {
                TEST_SAY("%02x", (unsigned int)bytes[i]);
                if (i + 1 < show)
                        TEST_SAY(" ");
        }
        if (show < len)
                TEST_SAY(" ...");
        TEST_SAY("\n");

        TEST_SAY("      asc: ");
        for (size_t i = 0; i < show; i++) {
                unsigned char c = bytes[i];
                TEST_SAY("%c", (c >= 32 && c <= 126) ? (char)c : '.');
        }
        if (show < len)
                TEST_SAY("...");
        TEST_SAY("\n");
}

static uint32_t message_crc32(const void *bytes, size_t len) {
        if (!bytes || len == 0)
                return 0;

        return rd_crc32((const char *)bytes, len);
}

static rd_bool_t should_log_this_message(const verify_state_t *vs,
                                         mm_flags_t flags) {
        if (!vs->log_each_message)
                return rd_false;

        if (vs->log_mode == MSGLOG_MISMATCH)
                return flags != MM_NONE;

        return rd_true;
}

static void log_verify_payloads(const verify_state_t *vs,
                                const msg_verify_t *m,
                                const seen_record_t *rec,
                                rd_bool_t has_expected) {
        if (!should_log_this_message(vs, rec->flags))
                return;

        switch (vs->log_mode) {
        case MSGLOG_COMPACT:
                return;

        case MSGLOG_PREVIEW:
                if (has_expected) {
                        dump_bytes_preview("expected_key", m->key, m->key_len,
                                           vs->log_preview_bytes);
                        dump_bytes_preview("expected_payload", m->payload,
                                           m->payload_len,
                                           vs->log_preview_bytes);
                } else {
                        TEST_SAY("    expected_key: <unavailable>\n");
                        TEST_SAY("    expected_payload: <unavailable>\n");
                }
                dump_bytes_preview("got_key", rec->key, rec->key_len,
                                   vs->log_preview_bytes);
                dump_bytes_preview("got_payload", rec->payload, rec->payload_len,
                                   vs->log_preview_bytes);
                return;

        case MSGLOG_FULL:
        case MSGLOG_MISMATCH:
                if (has_expected) {
                        dump_bytes_hex("expected_key", m->key, m->key_len);
                        dump_bytes_hex("expected_payload", m->payload,
                                       m->payload_len);
                } else {
                        TEST_SAY("    expected_key: <unavailable>\n");
                        TEST_SAY("    expected_payload: <unavailable>\n");
                }
                dump_bytes_hex("got_key", rec->key, rec->key_len);
                dump_bytes_hex("got_payload", rec->payload, rec->payload_len);
                return;
        }
}

static void dump_seen_record(const char *prefix, const seen_record_t *rec) {
        char flags[160];

        mm_flags_to_str(rec->flags, flags, sizeof(flags));
        TEST_SAY(
            "  %smsgid=%d partition=%" PRId32 " offset=%" PRId64
            " flags=%s key_diff=%d payload_diff=%d\n",
            prefix, rec->msgid, rec->partition, rec->offset, flags,
            rec->first_key_diff_idx, rec->first_payload_diff_idx);
        dump_bytes_hex("seen.key", rec->key, rec->key_len);
        dump_bytes_hex("seen.payload", rec->payload, rec->payload_len);
}

static void dump_full_mismatch_report(verify_state_t *vs) {
        TEST_SAY("\n========== 0206 FULL MISMATCH REPORT ==========\n");
        TEST_SAY("expected_msgs=%d partition_cnt=%d\n", vs->expected_msgs,
                 vs->partition_cnt);
        TEST_SAY("missing=%d duplicates=%d\n", vs->missing, vs->duplicates);
        TEST_SAY("parse_key_errors=%d testid_mismatches=%d msgid_range_errors=%d\n",
                 vs->parse_key_errors, vs->testid_mismatches,
                 vs->msgid_range_errors);
        TEST_SAY("partition_mismatches=%d order_violations=%d\n",
                 vs->partition_mismatches, vs->order_violations);
        TEST_SAY("key_len_mismatches=%d key_byte_mismatches=%d\n",
                 vs->key_len_mismatches, vs->key_byte_mismatches);
        TEST_SAY("payload_len_mismatches=%d payload_byte_mismatches=%d\n",
                 vs->payload_len_mismatches, vs->payload_byte_mismatches);
        TEST_SAY(
            "header_count_mismatches=%d header_name_mismatches=%d "
            "header_len_mismatches=%d header_byte_mismatches=%d\n",
            vs->header_count_mismatches, vs->header_name_mismatches,
            vs->header_len_mismatches, vs->header_byte_mismatches);
        TEST_SAY(
            "offset_order_violations=%d offset_gap_violations=%d "
            "offset_base_violations=%d partition_count_mismatches=%d\n",
            vs->offset_order_violations, vs->offset_gap_violations,
            vs->offset_base_violations, vs->partition_count_mismatches);
        TEST_SAY("dr_total=%d dr_success=%d dr_errors=%d dr_orphan=%d\n",
                 vs->dr_total, vs->dr_success, vs->dr_errors, vs->dr_orphan);

        TEST_SAY("\n-- Orphans (%zu) --\n", vs->orphan_cnt);
        for (size_t i = 0; i < vs->orphan_cnt; i++) {
                TEST_SAY(" orphan[%zu]\n", i);
                dump_seen_record("orphan.", &vs->orphans[i]);
        }

        TEST_SAY("\n-- Per-message issues --\n");
        for (int msgid = 0; msgid < vs->expected_msgs; msgid++) {
                msg_verify_t *m = &vs->msgs[msgid];
                rd_bool_t has_issue = m->missing || m->seen_cnt != 1 ||
                                      m->dr_seen_cnt != 1 ||
                                      m->dr_last_err != RD_KAFKA_RESP_ERR_NO_ERROR;

                if (!has_issue) {
                        for (size_t j = 0; j < m->seen_cnt; j++) {
                                if (m->seen[j].flags != MM_NONE) {
                                        has_issue = rd_true;
                                        break;
                                }
                        }
                }

                if (!has_issue)
                        continue;

                TEST_SAY(
                    " msg[%d] expected_partition=%" PRId32
                    " missing=%d seen_cnt=%zu dr_seen_cnt=%d dr_last_err=%s\n",
                    msgid, m->expected_partition, (int)m->missing, m->seen_cnt,
                    m->dr_seen_cnt, rd_kafka_err2str(m->dr_last_err));
                dump_bytes_hex("expected.key", m->key, m->key_len);
                dump_bytes_hex("expected.payload", m->payload, m->payload_len);

                for (size_t j = 0; j < m->seen_cnt; j++) {
                        TEST_SAY("  seen[%zu]\n", j);
                        dump_seen_record("", &m->seen[j]);
                }
        }

        TEST_SAY("\n-- Per-partition counts and offsets --\n");
        for (int p = 0; p < vs->partition_cnt; p++) {
                if (vs->seen_per_partition[p] != vs->expected_per_partition[p] ||
                    (vs->seen_per_partition[p] > 0 &&
                     vs->first_offset_per_partition[p] != 0)) {
                        TEST_SAY(
                            " partition=%d expected=%d seen=%d first_offset=%" PRId64
                            " last_offset=%" PRId64 "\n",
                            p, vs->expected_per_partition[p],
                            vs->seen_per_partition[p],
                            vs->first_offset_per_partition[p],
                            vs->last_offset_per_partition[p]);
                }
        }

        TEST_SAY("========== END 0206 FULL MISMATCH REPORT ==========\n");
}

static void run_consume_and_verify_phase(bench_config_t *config,
                                         verify_state_t *vs,
                                         uint64_t testid,
                                         const char *topic,
                                         const char *bootstrap_servers,
                                         profile_t producer_profile) {
        char group_id[128];
        rd_snprintf(group_id, sizeof(group_id), "0206-correctness-%s-%" PRIu64,
                    bench_producer_profile_slug(producer_profile), testid);
        rd_kafka_t *rk =
            create_verify_consumer_0206(topic, group_id, config->partition_cnt,
                                        bootstrap_servers);

        // deterministic assignment, no group rebalance noise

        int unique_seen  = 0;
        int64_t deadline = test_clock() + tmout_multip(120000) * 1000;

        while (test_clock() < deadline && unique_seen < vs->expected_msgs) {
                rd_kafka_message_t *rkmsg = rd_kafka_consumer_poll(rk, 1000);
                if (!rkmsg)
                        continue;

                if (rkmsg->err) {
                        if (rkmsg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                                // hard fail on unexpected consume errors
                                TEST_FAIL("consume error: %s",
                                          rd_kafka_message_errstr(rkmsg));
                        }
                        rd_kafka_message_destroy(rkmsg);
                        continue;
                }

                if (verify_consumed_message(vs, testid, rkmsg))
                        unique_seen++;

                rd_kafka_message_destroy(rkmsg);
        }

        if (unique_seen != vs->expected_msgs)
                TEST_FAIL("timeout: saw %d/%d unique messages", unique_seen,
                          vs->expected_msgs);

        // Finalize duplicates/missing and aggregate full mismatch counts
        for (int msgid = 0; msgid < vs->expected_msgs; msgid++) {
                msg_verify_t *m = &vs->msgs[msgid];
                if (m->seen_cnt == 0) {
                        m->missing = rd_true;
                        vs->missing++;
                } else if (m->seen_cnt > 1) {
                        vs->duplicates += (int)(m->seen_cnt - 1);
                }
        }

        for (int p = 0; p < vs->partition_cnt; p++) {
                if (vs->seen_per_partition[p] != vs->expected_per_partition[p])
                        vs->partition_count_mismatches++;
        }

        // Correctness gate
        if (vs->missing || vs->duplicates || vs->parse_key_errors ||
            vs->testid_mismatches || vs->msgid_range_errors ||
            vs->partition_mismatches || vs->order_violations ||
            vs->key_len_mismatches || vs->key_byte_mismatches ||
            vs->payload_len_mismatches || vs->payload_byte_mismatches ||
            vs->header_count_mismatches || vs->header_name_mismatches ||
            vs->header_len_mismatches || vs->header_byte_mismatches ||
            vs->offset_order_violations || vs->offset_gap_violations ||
            vs->offset_base_violations || vs->partition_count_mismatches) {
                dump_full_mismatch_report(vs);  // print it all
                TEST_FAIL("0206 correctness failed");
        }

        test_consumer_close(rk);
        rd_kafka_destroy(rk);
}

static void report_verification_state(verify_state_t *vs) {
        int64_t seen_total = 0;
        int dr_missing = 0;
        int dr_duplicates = 0;
        int dr_nonzero = 0;
        int flagged_seen_records = 0;

        for (int i = 0; i < vs->expected_msgs; i++) {
                msg_verify_t *m = &vs->msgs[i];
                seen_total += (int64_t)m->seen_cnt;

                if (m->dr_seen_cnt == 0)
                        dr_missing++;
                else if (m->dr_seen_cnt > 1)
                        dr_duplicates += (m->dr_seen_cnt - 1);

                if (m->dr_seen_cnt > 0 &&
                    m->dr_last_err != RD_KAFKA_RESP_ERR_NO_ERROR)
                        dr_nonzero++;

                for (size_t j = 0; j < m->seen_cnt; j++) {
                        if (m->seen[j].flags != MM_NONE)
                                flagged_seen_records++;
                }
        }

        TEST_SAY("\n========== 0206 VERIFICATION SUMMARY ==========\n");
        TEST_SAY("expected_msgs=%d seen_total=%" PRId64 " missing=%d duplicates=%d\n",
                 vs->expected_msgs, seen_total, vs->missing, vs->duplicates);
        TEST_SAY("parse_key=%d testid=%d msgid_range=%d\n", vs->parse_key_errors,
                 vs->testid_mismatches, vs->msgid_range_errors);
        TEST_SAY("partition=%d order=%d key_len=%d key_bytes=%d payload_len=%d payload_bytes=%d\n",
                 vs->partition_mismatches, vs->order_violations,
                 vs->key_len_mismatches, vs->key_byte_mismatches,
                 vs->payload_len_mismatches, vs->payload_byte_mismatches);
        TEST_SAY(
            "header_cnt=%d header_name=%d header_len=%d header_bytes=%d\n",
            vs->header_count_mismatches, vs->header_name_mismatches,
            vs->header_len_mismatches, vs->header_byte_mismatches);
        TEST_SAY(
            "offset_order=%d offset_gap=%d offset_base=%d "
            "partition_count=%d\n",
            vs->offset_order_violations, vs->offset_gap_violations,
            vs->offset_base_violations, vs->partition_count_mismatches);
        TEST_SAY("dr_total=%d dr_success=%d dr_errors=%d dr_orphan=%d dr_missing=%d dr_duplicates=%d dr_nonzero=%d\n",
                 vs->dr_total, vs->dr_success, vs->dr_errors, vs->dr_orphan,
                 dr_missing, dr_duplicates, dr_nonzero);
        TEST_SAY("requests: produce=%" PRId64 " total=%" PRId64 "\n",
                 vs->produce_request_count, vs->total_request_count);
        TEST_SAY("orphans=%zu flagged_seen_records=%d\n", vs->orphan_cnt,
                 flagged_seen_records);
        TEST_SAY("\n========== END 0206 VERIFICATION SUMMARY ==========\n");
}

static void run_correctness_phase(bench_config_t *config,
                                  profile_t producer_profile) {
        uint64_t testid = test_id_generate();
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstrap_servers;

        char suffix[64];
        rd_snprintf(suffix, sizeof(suffix), "0206_%s",
                    bench_producer_profile_slug(producer_profile));

        const char *topic = test_mk_topic_name(suffix, 1);
        mcluster = test_mock_cluster_new(3, &bootstrap_servers);
        TEST_ASSERT(mcluster, "Failed to create mock cluster");

        TEST_ASSERT(!rd_kafka_mock_topic_create(mcluster, topic,
                                                config->partition_cnt, 1),
                    "Failed to create mock topic %s", topic);
        for (int p = 0; p < config->partition_cnt; p++) {
                TEST_ASSERT(!rd_kafka_mock_partition_set_leader(
                                mcluster, topic, p, FAULT_BROKER_ID),
                            "Failed to set leader=%d for %s [%" PRId32 "]",
                            FAULT_BROKER_ID, topic, p);
        }

        verify_state_t *verification_state =
            init_verification_state(config, testid);
        run_produce_phase(config, verification_state, producer_profile, mcluster,
                          topic, bootstrap_servers, 1000);
        run_consume_and_verify_phase(config, verification_state, testid, topic,
                                     bootstrap_servers, producer_profile);
        report_verification_state(verification_state);
        destroy_verify_state(verification_state);
        test_mock_cluster_destroy(mcluster);
}

int main_0206_fault_injection_correctness_mock(int argc, char **argv) {
        bench_config_t config;

        TEST_SKIP_MOCK_CLUSTER(0);
        parse_config(&config);

        run_correctness_phase(&config, PROFILE_V1);
        run_correctness_phase(&config, PROFILE_V1_MB);
        run_correctness_phase(&config, PROFILE_V2);

        return 0;
}
