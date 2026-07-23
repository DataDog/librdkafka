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
                        return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                }


                ret = ZSTD_decompress(decompressed, (size_t)out_bufsize, inbuf,
                                      inlen);
                if (!ZSTD_isError(ret)) {
                        *outlenp = ret;
                        *outbuf  = decompressed;
                        return RD_KAFKA_RESP_ERR_NO_ERROR;
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
                        return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                }
        }

        rd_rkb_dbg(rkb, MSG, "ZSTD",
                   "Unable to decompress ZSTD "
                   "(input buffer %" PRIusz
                   ", output buffer %llu): "
                   "output would exceed message.max.bytes (%d)",
                   inlen, out_bufsize, rkb->rkb_rk->rk_conf.max_msg_size);

        return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
}


/**
 * @brief Retrieve or lazily create the broker's pooled ZSTD compression
 *        context.  Called only from the broker I/O thread — no locking.
 */
static ZSTD_CStream *rd_kafka_broker_get_zstd_cctx(rd_kafka_broker_t *rkb,
                                                    int comp_level) {
        ZSTD_CStream *cctx = (ZSTD_CStream *)rkb->rkb_zstd_cctx;

        if (likely(cctx != NULL)) {
                /* Reset session state only — keeps internal workspace buffers
                 * (hash tables, window, etc.) allocated from the previous
                 * call, which is the entire point of pooling. */
                ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);

                /* Re-set compression level only if it changed */
                if (unlikely(comp_level != rkb->rkb_zstd_comp_level)) {
                        ZSTD_CCtx_setParameter(cctx,
                                               ZSTD_c_compressionLevel,
                                               comp_level);
                        rkb->rkb_zstd_comp_level = comp_level;
                }
                return cctx;
        }

        /* First call: create the context */
        cctx = ZSTD_createCStream();
        if (!cctx)
                return NULL;

        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, comp_level);
        rkb->rkb_zstd_cctx       = cctx;
        rkb->rkb_zstd_comp_level = comp_level;
        return cctx;
}


/**
 * @brief Free the broker's pooled ZSTD compression context.
 *        Call from broker destroy path.
 */
void rd_kafka_zstd_broker_term(rd_kafka_broker_t *rkb) {
        if (rkb->rkb_zstd_cctx) {
                ZSTD_freeCStream((ZSTD_CStream *)rkb->rkb_zstd_cctx);
                rkb->rkb_zstd_cctx = NULL;
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

        cctx = rd_kafka_broker_get_zstd_cctx(rkb, comp_level);
        if (!cctx) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to create ZSTD compression context");
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }

#if defined(WITH_ZSTD_STATIC) &&                                               \
    ZSTD_VERSION_NUMBER >= (1 * 100 * 100 + 2 * 100 + 1) /* v1.2.1 */
        /* Hint the source size for better frame headers */
        ZSTD_CCtx_setPledgedSrcSize(cctx, len);
#endif

        while ((in.size = rd_slice_reader(slice, &in.src))) {
                in.pos = 0;
                r      = ZSTD_compressStream2(cctx, &out, &in, ZSTD_e_continue);
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

        /* Flush and close frame (ZSTD_e_end equivalent of old endStream) */
        in.src  = NULL;
        in.size = 0;
        in.pos  = 0;
        r = ZSTD_compressStream2(cctx, &out, &in, ZSTD_e_end);
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
        /* Context is NOT freed — it's pooled on the broker for reuse */

        if (err)
                rd_free(out.dst);

        return err;
}
