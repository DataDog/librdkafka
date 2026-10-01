/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2018-2022, Magnus Edenhill
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

#include "rdkafka_int.h"
#include "rdkafka_zstd.h"

#if WITH_ZSTD_STATIC
/* Enable advanced/unstable API for initCStream_srcSize */
#define ZSTD_STATIC_LINKING_ONLY
#endif

#include <zstd.h>
#include <zstd_errors.h>


void rd_kafka_zstd_pool_init(rd_kafka_zstd_pool_t *pool, int max) {
        rd_assert(max > 0);
        mtx_init(&pool->lock, mtx_plain);
        pool->max      = max;
        pool->cctx_cnt = 0;
        pool->dctx_cnt = 0;
        pool->cctx     = rd_calloc(max, sizeof(*pool->cctx));
        pool->dctx     = rd_calloc(max, sizeof(*pool->dctx));
}


void rd_kafka_zstd_pool_destroy(rd_kafka_zstd_pool_t *pool) {
        if (!pool->max)
                return;

        while (pool->cctx_cnt > 0)
                ZSTD_freeCStream(pool->cctx[--pool->cctx_cnt]);
        while (pool->dctx_cnt > 0)
                ZSTD_freeDCtx(pool->dctx[--pool->dctx_cnt]);
        rd_free(pool->cctx);
        rd_free(pool->dctx);
        mtx_destroy(&pool->lock);
        pool->max = 0;
}


/**
 * @returns an idle compression context from \p pool, or a new one if the
 *          pool is empty or disabled, or NULL on allocation failure.
 */
static ZSTD_CStream *rd_kafka_zstd_cctx_get(rd_kafka_zstd_pool_t *pool) {
        ZSTD_CStream *cctx = NULL;

        if (pool->max) {
                mtx_lock(&pool->lock);
                if (pool->cctx_cnt > 0)
                        cctx = pool->cctx[--pool->cctx_cnt];
                mtx_unlock(&pool->lock);
        }

        return cctx ? cctx : ZSTD_createCStream();
}

/**
 * @brief Return \p cctx to \p pool, or free it if the pool is full or
 *        disabled, or if \p reusable is false (the context may be in an
 *        undefined state after a failure).
 */
static void rd_kafka_zstd_cctx_put(rd_kafka_zstd_pool_t *pool,
                                   ZSTD_CStream *cctx,
                                   rd_bool_t reusable) {
        if (reusable && pool->max) {
                mtx_lock(&pool->lock);
                if (pool->cctx_cnt < pool->max) {
                        pool->cctx[pool->cctx_cnt++] = cctx;
                        cctx                         = NULL;
                }
                mtx_unlock(&pool->lock);
        }

        if (cctx)
                ZSTD_freeCStream(cctx);
}

/** @brief Decompression context counterpart of rd_kafka_zstd_cctx_get(). */
static ZSTD_DCtx *rd_kafka_zstd_dctx_get(rd_kafka_zstd_pool_t *pool) {
        ZSTD_DCtx *dctx = NULL;

        if (pool->max) {
                mtx_lock(&pool->lock);
                if (pool->dctx_cnt > 0)
                        dctx = pool->dctx[--pool->dctx_cnt];
                mtx_unlock(&pool->lock);
        }

        return dctx ? dctx : ZSTD_createDCtx();
}

/** @brief Decompression context counterpart of rd_kafka_zstd_cctx_put(). */
static void rd_kafka_zstd_dctx_put(rd_kafka_zstd_pool_t *pool,
                                   ZSTD_DCtx *dctx,
                                   rd_bool_t reusable) {
        if (reusable && pool->max) {
                mtx_lock(&pool->lock);
                if (pool->dctx_cnt < pool->max) {
                        pool->dctx[pool->dctx_cnt++] = dctx;
                        dctx                         = NULL;
                }
                mtx_unlock(&pool->lock);
        }

        if (dctx)
                ZSTD_freeDCtx(dctx);
}

rd_kafka_resp_err_t rd_kafka_zstd_decompress(rd_kafka_broker_t *rkb,
                                             char *inbuf,
                                             size_t inlen,
                                             void **outbuf,
                                             size_t *outlenp) {
        unsigned long long out_bufsize = ZSTD_getFrameContentSize(inbuf, inlen);
        rd_kafka_zstd_pool_t *pool = &rkb->rkb_rk->rk_zstd_pool;
        ZSTD_DCtx *dctx;
        rd_bool_t reusable = rd_true;
        rd_kafka_resp_err_t err;

        switch (out_bufsize) {
        case ZSTD_CONTENTSIZE_UNKNOWN:
                /* Decompressed size cannot be determined, make a guess */
                out_bufsize = inlen * 2;
                break;
        case ZSTD_CONTENTSIZE_ERROR:
                /* Error calculating frame content size */
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to begin ZSTD decompression "
                           "(out buffer is %llu bytes): %s",
                           out_bufsize, "Error in determining frame size");
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        default:
                break;
        }

        dctx = rd_kafka_zstd_dctx_get(pool);
        if (!dctx) {
                rd_rkb_dbg(rkb, MSG, "ZSTD",
                           "Unable to create ZSTD decompression context");
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        /* Increase output buffer until it can fit the entire result,
         * capped by message.max.bytes */
        while (out_bufsize <=
               (unsigned long long)rkb->rkb_rk->rk_conf.recv_max_msg_size) {
                size_t ret;
                char *decompressed;

                decompressed = rd_malloc((size_t)out_bufsize);
                if (!decompressed) {
                        rd_rkb_dbg(rkb, MSG, "ZSTD",
                                   "Unable to allocate output buffer "
                                   "(%llu bytes for %" PRIusz
                                   " compressed bytes): %s",
                                   out_bufsize, inlen, rd_strerror(errno));
                        err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                        goto done;
                }


                ret = ZSTD_decompressDCtx(dctx, decompressed,
                                          (size_t)out_bufsize, inbuf, inlen);
                if (!ZSTD_isError(ret)) {
                        *outlenp = ret;
                        *outbuf  = decompressed;
                        err      = RD_KAFKA_RESP_ERR_NO_ERROR;
                        goto done;
                }

                rd_free(decompressed);

                /* Check if the destination size is too small */
                if (ZSTD_getErrorCode(ret) == ZSTD_error_dstSize_tooSmall) {

                        /* Grow quadratically */
                        out_bufsize += RD_MAX(out_bufsize * 2, 4000);

                        rd_atomic64_add(&rkb->rkb_c.zbuf_grow, 1);

                } else {
                        /* Fail on any other error */
                        rd_rkb_dbg(rkb, MSG, "ZSTD",
                                   "Unable to begin ZSTD decompression "
                                   "(out buffer is %llu bytes): %s",
                                   out_bufsize, ZSTD_getErrorName(ret));
                        reusable = rd_false;
                        err      = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
        }

        rd_rkb_dbg(rkb, MSG, "ZSTD",
                   "Unable to decompress ZSTD "
                   "(input buffer %" PRIusz
                   ", output buffer %llu): "
                   "output would exceed message.max.bytes (%d)",
                   inlen, out_bufsize, rkb->rkb_rk->rk_conf.max_msg_size);

        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;

done:
        rd_kafka_zstd_dctx_put(pool, dctx, reusable);

        return err;
}


rd_kafka_resp_err_t rd_kafka_zstd_compress(rd_kafka_broker_t *rkb,
                                           int comp_level,
                                           rd_slice_t *slice,
                                           void **outbuf,
                                           size_t *outlenp) {
        rd_kafka_zstd_pool_t *pool = &rkb->rkb_rk->rk_zstd_pool;
        ZSTD_CStream *cctx         = NULL;
        size_t r;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t len              = rd_slice_remains(slice);
        ZSTD_outBuffer out;
        ZSTD_inBuffer in;

        *outbuf  = NULL;
        out.pos  = 0;
        out.size = ZSTD_compressBound(len);
        out.dst  = rd_malloc(out.size);
        if (!out.dst) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate output buffer "
                           "(%" PRIusz " bytes): %s",
                           out.size, rd_strerror(errno));
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }


        cctx = rd_kafka_zstd_cctx_get(pool);
        if (!cctx) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to create ZSTD compression context");
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }

#if defined(WITH_ZSTD_STATIC) &&                                               \
    ZSTD_VERSION_NUMBER >= (1 * 100 * 100 + 2 * 100 + 1) /* v1.2.1 */
        r = ZSTD_initCStream_srcSize(cctx, comp_level, len);
#else
        r = ZSTD_initCStream(cctx, comp_level);
#if ZSTD_VERSION_NUMBER >= (1 * 100 * 100 + 4 * 100) /* v1.4.0 */
        /* Include the uncompressed batch size in dynamically linked zstd
         * frames.  Besides allowing consumers to allocate the exact output
         * size, zstd uses this value when selecting compression parameters. */
        if (!ZSTD_isError(r))
                r = ZSTD_CCtx_setPledgedSrcSize(cctx, len);
#else
        /* zstd < 1.4.0 has no stable public pledged-size setter for
         * dynamically linked builds.  The resulting frame omits the
         * decompressed size, which may make consumer decompression costlier. */
#endif
#endif
        if (ZSTD_isError(r)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to begin ZSTD compression "
                           "(out buffer is %" PRIusz " bytes): %s",
                           out.size, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        while ((in.size = rd_slice_reader(slice, &in.src))) {
                in.pos = 0;
                r      = ZSTD_compressStream(cctx, &out, &in);
                if (unlikely(ZSTD_isError(r))) {
                        rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                                   "ZSTD compression failed "
                                   "(at of %" PRIusz
                                   " bytes, with "
                                   "%" PRIusz
                                   " bytes remaining in out buffer): "
                                   "%s",
                                   in.size, out.size - out.pos,
                                   ZSTD_getErrorName(r));
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }

                /* No space left in output buffer,
                 * but input isn't fully consumed */
                if (in.pos < in.size) {
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
        }

        if (rd_slice_remains(slice) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to finalize ZSTD compression "
                           "of %" PRIusz " bytes: %s",
                           len, "Unexpected trailing data");
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        r = ZSTD_endStream(cctx, &out);
        if (unlikely(ZSTD_isError(r) || r > 0)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to finalize ZSTD compression "
                           "of %" PRIusz " bytes: %s",
                           len, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        *outbuf  = out.dst;
        *outlenp = out.pos;

done:
        if (cctx)
                rd_kafka_zstd_cctx_put(pool, cctx, !err);

        if (err)
                rd_free(out.dst);

        return err;
}


/**
 * @name Unit tests
 * @{
 */

#include "rdunittest.h"

/** @brief Fake client and broker, as seen by (de)compression. */
typedef struct ut_zstd_client_s {
        rd_kafka_t *rk;
        rd_kafka_broker_t *rkb;
} ut_zstd_client_t;

static ut_zstd_client_t ut_zstd_client_new(int pool_max) {
        ut_zstd_client_t c;

        c.rk                              = rd_calloc(1, sizeof(*c.rk));
        c.rk->rk_conf.recv_max_msg_size   = 100000000;
        c.rkb                             = rd_calloc(1, sizeof(*c.rkb));
        c.rkb->rkb_rk                     = c.rk;
        if (pool_max)
                rd_kafka_zstd_pool_init(&c.rk->rk_zstd_pool, pool_max);
        return c;
}

static void ut_zstd_client_destroy(ut_zstd_client_t *c) {
        rd_kafka_zstd_pool_destroy(&c->rk->rk_zstd_pool);
        rd_free(c->rkb);
        rd_free(c->rk);
}

/**
 * @brief Compress \p len bytes of \p seed -derived data, spread over
 *        several buffer segments, decompress, and compare.
 * @returns 0 on success.
 */
static int ut_zstd_roundtrip(rd_kafka_broker_t *rkb, size_t len, int seed) {
        char *payload = rd_malloc(len);
        rd_buf_t b;
        rd_slice_t slice;
        void *comp, *decomp;
        size_t comp_len, decomp_len;
        size_t i, off;
        int fails = 0;

        for (i = 0; i < len; i++)
                payload[i] = (char)("abcdefgh"[(i / 7 + seed) % 8] + (i % 3));

        rd_buf_init(&b, 4, 0);
        for (off = 0; off < len; off += 4096)
                rd_buf_push(&b, payload + off, RD_MIN(4096, len - off), NULL);
        rd_slice_init_full(&slice, &b);

        if (rd_kafka_zstd_compress(rkb, 3, &slice, &comp, &comp_len)) {
                fails++;
                goto done;
        }
        if (rd_kafka_zstd_decompress(rkb, comp, comp_len, &decomp,
                                     &decomp_len)) {
                fails++;
        } else {
                if (decomp_len != len || memcmp(decomp, payload, len))
                        fails++;
                rd_free(decomp);
        }
        rd_free(comp);

done:
        rd_buf_destroy(&b);
        rd_free(payload);
        return fails;
}

/** @brief Idle contexts are capped at max, reused LIFO, and dropped when
 *         not reusable. */
static int ut_zstd_pool_bounds(void) {
        rd_kafka_zstd_pool_t pool = RD_ZERO_INIT;
        ZSTD_CStream *c[3];
        ZSTD_DCtx *d;
        int i;

        rd_kafka_zstd_pool_init(&pool, 2);

        for (i = 0; i < 3; i++) {
                c[i] = rd_kafka_zstd_cctx_get(&pool);
                RD_UT_ASSERT(c[i], "cctx %d not created", i);
        }
        for (i = 0; i < 3; i++)
                rd_kafka_zstd_cctx_put(&pool, c[i], rd_true);
        RD_UT_ASSERT(pool.cctx_cnt == 2, "expected 2 idle cctx, got %d",
                     pool.cctx_cnt);

        RD_UT_ASSERT(rd_kafka_zstd_cctx_get(&pool) == c[1],
                     "expected most recently returned cctx");
        rd_kafka_zstd_cctx_put(&pool, c[1], rd_false);
        RD_UT_ASSERT(pool.cctx_cnt == 1,
                     "non-reusable cctx must not be pooled, got %d",
                     pool.cctx_cnt);

        d = rd_kafka_zstd_dctx_get(&pool);
        rd_kafka_zstd_dctx_put(&pool, d, rd_true);
        RD_UT_ASSERT(pool.dctx_cnt == 1, "expected 1 idle dctx, got %d",
                     pool.dctx_cnt);
        RD_UT_ASSERT(rd_kafka_zstd_dctx_get(&pool) == d,
                     "expected pooled dctx");
        rd_kafka_zstd_dctx_put(&pool, d, rd_true);

        rd_kafka_zstd_pool_destroy(&pool);
        RD_UT_ASSERT(pool.cctx_cnt == 0 && pool.dctx_cnt == 0 && !pool.max,
                     "pool not emptied");

        RD_UT_PASS();
}

/** @brief Round-trips with reuse enabled reuse the same contexts, and with
 *         reuse disabled never touch the pool. */
static int ut_zstd_reuse(void) {
        ut_zstd_client_t on  = ut_zstd_client_new(1);
        ut_zstd_client_t off = ut_zstd_client_new(0);
        rd_kafka_zstd_pool_t *pool = &on.rk->rk_zstd_pool;
        void *cctx, *dctx;
        int i;

        RD_UT_ASSERT(!ut_zstd_roundtrip(on.rkb, 1 << 20, 0),
                     "roundtrip failed");
        RD_UT_ASSERT(pool->cctx_cnt == 1 && pool->dctx_cnt == 1,
                     "expected 1 idle cctx and dctx, got %d and %d",
                     pool->cctx_cnt, pool->dctx_cnt);
        cctx = pool->cctx[0];
        dctx = pool->dctx[0];

        /* Smaller and larger batches on the same contexts. */
        for (i = 1; i <= 4; i++)
                RD_UT_ASSERT(!ut_zstd_roundtrip(on.rkb, (size_t)i * 70001, i),
                             "roundtrip %d failed", i);
        RD_UT_ASSERT(pool->cctx[0] == cctx && pool->dctx[0] == dctx,
                     "contexts were not reused");

        RD_UT_ASSERT(!ut_zstd_roundtrip(off.rkb, 1 << 16, 0),
                     "roundtrip without reuse failed");
        RD_UT_ASSERT(!off.rk->rk_zstd_pool.cctx_cnt &&
                         !off.rk->rk_zstd_pool.dctx_cnt,
                     "disabled pool must stay empty");

        ut_zstd_client_destroy(&on);
        ut_zstd_client_destroy(&off);
        RD_UT_PASS();
}

/** @brief A truncated frame fails decompression and evicts its context. */
static int ut_zstd_evict_on_error(void) {
        ut_zstd_client_t c = ut_zstd_client_new(1);
        char payload[8192];
        rd_buf_t b;
        rd_slice_t slice;
        void *comp, *out;
        size_t comp_len, out_len, i;

        for (i = 0; i < sizeof(payload); i++)
                payload[i] = (char)(i * 7 % 251);
        rd_buf_init(&b, 1, 0);
        rd_buf_push(&b, payload, sizeof(payload), NULL);
        rd_slice_init_full(&slice, &b);
        RD_UT_ASSERT(!rd_kafka_zstd_compress(c.rkb, 3, &slice, &comp,
                                             &comp_len),
                     "compression failed");

        RD_UT_ASSERT(!rd_kafka_zstd_decompress(c.rkb, comp, comp_len, &out,
                                               &out_len),
                     "decompression failed");
        rd_free(out);
        RD_UT_ASSERT(c.rk->rk_zstd_pool.dctx_cnt == 1, "expected idle dctx");

        RD_UT_ASSERT(rd_kafka_zstd_decompress(c.rkb, comp, comp_len / 2, &out,
                                              &out_len) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "truncated frame must fail");
        RD_UT_ASSERT(c.rk->rk_zstd_pool.dctx_cnt == 0,
                     "failed dctx must not be returned to the pool");

        rd_free(comp);
        rd_buf_destroy(&b);
        ut_zstd_client_destroy(&c);
        RD_UT_PASS();
}

typedef struct ut_zstd_thread_arg_s {
        rd_kafka_broker_t *rkb;
        int seed;
        int fails;
} ut_zstd_thread_arg_t;

static int ut_zstd_thread_main(void *p) {
        ut_zstd_thread_arg_t *arg = p;
        int i;

        for (i = 0; i < 50; i++)
                arg->fails += ut_zstd_roundtrip(
                    arg->rkb, 4096 + (size_t)((arg->seed * 31 + i) % 16) * 8192,
                    arg->seed + i);
        return 0;
}

/** @brief More threads than pooled contexts share one pool. */
static int ut_zstd_concurrent(void) {
        ut_zstd_client_t c = ut_zstd_client_new(2);
        ut_zstd_thread_arg_t args[8];
        thrd_t thrds[8];
        int i, fails = 0;

        for (i = 0; i < 8; i++) {
                args[i].rkb   = c.rkb;
                args[i].seed  = i;
                args[i].fails = 0;
                RD_UT_ASSERT(thrd_create(&thrds[i], ut_zstd_thread_main,
                                         &args[i]) == thrd_success,
                             "thrd_create failed");
        }
        for (i = 0; i < 8; i++) {
                thrd_join(thrds[i], NULL);
                fails += args[i].fails;
        }

        RD_UT_ASSERT(!fails, "%d concurrent roundtrips failed", fails);
        RD_UT_ASSERT(c.rk->rk_zstd_pool.cctx_cnt <= 2 &&
                         c.rk->rk_zstd_pool.dctx_cnt <= 2,
                     "pool exceeded its cap: %d cctx, %d dctx",
                     c.rk->rk_zstd_pool.cctx_cnt, c.rk->rk_zstd_pool.dctx_cnt);

        ut_zstd_client_destroy(&c);
        RD_UT_PASS();
}

int unittest_zstd(void) {
        int fails = 0;

        fails += ut_zstd_pool_bounds();
        fails += ut_zstd_reuse();
        fails += ut_zstd_evict_on_error();
        fails += ut_zstd_concurrent();

        return fails;
}

/**@}*/
