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

rd_kafka_resp_err_t rd_kafka_zstd_decompress(rd_kafka_broker_t *rkb,
                                             char *inbuf,
                                             size_t inlen,
                                             void **outbuf,
                                             size_t *outlenp) {
        unsigned long long out_bufsize = ZSTD_getFrameContentSize(inbuf, inlen);
        ZSTD_DCtx *dctx;
        rd_bool_t use_pooled_dctx = rkb->rkb_rk->rk_conf.zstd_ctx_reuse &&
                                    thrd_is_current(rkb->rkb_thread);
        rd_bool_t evict_dctx      = rd_false;
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

        if (use_pooled_dctx) {
                dctx = (ZSTD_DCtx *)rkb->rkb_zstd_dctx;
                if (!dctx) {
                        dctx = ZSTD_createDCtx();
                        if (dctx)
                                rkb->rkb_zstd_dctx = dctx;
                }
        } else {
                dctx = ZSTD_createDCtx();
        }
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
                        evict_dctx = rd_true;
                        err        = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
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
        if (use_pooled_dctx) {
                if (evict_dctx) {
                        ZSTD_freeDCtx(dctx);
                        rkb->rkb_zstd_dctx = NULL;
                }
        } else {
                ZSTD_freeDCtx(dctx);
        }

        return err;
}


void rd_kafka_zstd_broker_term(rd_kafka_broker_t *rkb) {
        if (rkb->rkb_zstd_cctx) {
                ZSTD_freeCStream((ZSTD_CStream *)rkb->rkb_zstd_cctx);
                rkb->rkb_zstd_cctx = NULL;
        }
        if (rkb->rkb_zstd_dctx) {
                ZSTD_freeDCtx((ZSTD_DCtx *)rkb->rkb_zstd_dctx);
                rkb->rkb_zstd_dctx = NULL;
        }
}


rd_kafka_resp_err_t rd_kafka_zstd_compress(rd_kafka_broker_t *rkb,
                                           int comp_level,
                                           rd_slice_t *slice,
                                           void **outbuf,
                                           size_t *outlenp) {
        ZSTD_CStream *cctx;
        size_t r;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t len              = rd_slice_remains(slice);
        rd_bool_t use_pooled_cctx = rkb->rkb_rk->rk_conf.zstd_ctx_reuse &&
                                    thrd_is_current(rkb->rkb_thread);
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


        /* With compression.zstd.context.reuse, reuse the broker's cached
         * ZSTD_CStream when called from the broker I/O thread (the hot
         * producer path).  Off-thread callers (e.g. telemetry) create and
         * free their own context. */
        if (use_pooled_cctx) {
                cctx = (ZSTD_CStream *)rkb->rkb_zstd_cctx;
                if (!cctx) {
                        cctx = ZSTD_createCStream();
                        if (cctx)
                                rkb->rkb_zstd_cctx = cctx;
                }
        } else {
                cctx = ZSTD_createCStream();
        }
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
        if (cctx && use_pooled_cctx) {
                /* Evict the cached context on any error — it may be in
                 * an undefined state after a compression failure. */
                if (err) {
                        ZSTD_freeCStream(cctx);
                        rkb->rkb_zstd_cctx = NULL;
                }
        } else if (cctx) {
                ZSTD_freeCStream(cctx);
        }

        if (err)
                rd_free(out.dst);

        return err;
}
