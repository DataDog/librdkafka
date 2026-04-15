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
#include <stdio.h>

#define RD_KAFKA_ZSTD_SAMPLE_MAGIC "RDKBAT01"
#define RD_KAFKA_ZSTD_SAMPLE_MAGIC_SIZE 8U

static void rd_kafka_zstd_write_u32_le(uint8_t out[4], uint32_t value) {
        out[0] = (uint8_t)(value & 0xff);
        out[1] = (uint8_t)((value >> 8) & 0xff);
        out[2] = (uint8_t)((value >> 16) & 0xff);
        out[3] = (uint8_t)((value >> 24) & 0xff);
}

static void rd_kafka_zstd_sample_dump_disable(rd_kafka_broker_t *rkb,
                                              const char *sample_dump_path) {
        rd_kafka_t *rk = rkb->rkb_rk;

        if (rk->rk_ut.zstd_sample_dump_fp) {
                fclose(rk->rk_ut.zstd_sample_dump_fp);
                rk->rk_ut.zstd_sample_dump_fp = NULL;
        }

        rk->rk_ut.zstd_sample_dump_failed = rd_true;

        rd_rkb_dbg(rkb, MSG, "ZSTDDUMP",
                   "Disabling zstd sample dump for %s after write failure",
                   sample_dump_path);
}

static rd_bool_t rd_kafka_zstd_sample_dump_open(rd_kafka_broker_t *rkb,
                                                const char *sample_dump_path) {
        rd_kafka_t *rk = rkb->rkb_rk;
        uint8_t count_buf[4] = {0};

        if (rk->rk_ut.zstd_sample_dump_fp)
                return rd_true;

        rk->rk_ut.zstd_sample_dump_fp = fopen(sample_dump_path, "w+b");
        if (!rk->rk_ut.zstd_sample_dump_fp) {
                rd_rkb_dbg(rkb, MSG, "ZSTDDUMP",
                           "Failed to open zstd batch sample dump %s: %s",
                           sample_dump_path, rd_strerror(errno));
                rk->rk_ut.zstd_sample_dump_failed = rd_true;
                return rd_false;
        }

        if (fwrite(RD_KAFKA_ZSTD_SAMPLE_MAGIC, 1,
                   RD_KAFKA_ZSTD_SAMPLE_MAGIC_SIZE,
                   rk->rk_ut.zstd_sample_dump_fp) !=
                RD_KAFKA_ZSTD_SAMPLE_MAGIC_SIZE ||
            fwrite(count_buf, 1, sizeof(count_buf),
                   rk->rk_ut.zstd_sample_dump_fp) != sizeof(count_buf) ||
            fflush(rk->rk_ut.zstd_sample_dump_fp) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDDUMP",
                           "Failed to initialize zstd batch sample dump %s: %s",
                           sample_dump_path, rd_strerror(errno));
                rd_kafka_zstd_sample_dump_disable(rkb, sample_dump_path);
                return rd_false;
        }

        rk->rk_ut.zstd_sample_dump_count = 0;
        return rd_true;
}

static void rd_kafka_zstd_sample_dump_batch(rd_kafka_broker_t *rkb,
                                            const char *sample_dump_path,
                                            const void *sample,
                                            size_t sample_len) {
        rd_kafka_t *rk = rkb->rkb_rk;
        uint8_t len_buf[4];
        uint8_t count_buf[4];

        if (!sample_dump_path || !*sample_dump_path)
                return;

        if (unlikely(sample_len > UINT32_MAX)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDDUMP",
                           "Skipping zstd batch sample dump for %s: sample "
                           "size %" PRIusz " exceeds corpus limit",
                           sample_dump_path, sample_len);
                return;
        }

        mtx_lock(&rk->rk_ut.lock);

        if (rk->rk_ut.zstd_sample_dump_failed)
                goto out;

        if (!rd_kafka_zstd_sample_dump_open(rkb, sample_dump_path))
                goto out;

        rd_kafka_zstd_write_u32_le(len_buf, (uint32_t)sample_len);

        if (fseek(rk->rk_ut.zstd_sample_dump_fp, 0, SEEK_END) != 0 ||
            fwrite(len_buf, 1, sizeof(len_buf),
                   rk->rk_ut.zstd_sample_dump_fp) != sizeof(len_buf) ||
            fwrite(sample, 1, sample_len, rk->rk_ut.zstd_sample_dump_fp) !=
                sample_len) {
                rd_rkb_dbg(rkb, MSG, "ZSTDDUMP",
                           "Failed to append zstd batch sample to %s: %s",
                           sample_dump_path, rd_strerror(errno));
                rd_kafka_zstd_sample_dump_disable(rkb, sample_dump_path);
                goto out;
        }

        rk->rk_ut.zstd_sample_dump_count++;
        rd_kafka_zstd_write_u32_le(count_buf, rk->rk_ut.zstd_sample_dump_count);

        if (fseek(rk->rk_ut.zstd_sample_dump_fp,
                  (long)RD_KAFKA_ZSTD_SAMPLE_MAGIC_SIZE, SEEK_SET) != 0 ||
            fwrite(count_buf, 1, sizeof(count_buf),
                   rk->rk_ut.zstd_sample_dump_fp) != sizeof(count_buf) ||
            fflush(rk->rk_ut.zstd_sample_dump_fp) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDDUMP",
                           "Failed to finalize zstd batch sample dump %s: %s",
                           sample_dump_path, rd_strerror(errno));
                rd_kafka_zstd_sample_dump_disable(rkb, sample_dump_path);
        }

out:
        mtx_unlock(&rk->rk_ut.lock);
}

static rd_kafka_resp_err_t rd_kafka_zstd_copy_slice(rd_kafka_broker_t *rkb,
                                                    rd_slice_t *slice,
                                                    size_t len,
                                                    char **bufp) {
        char *buf;
        size_t copied;

        *bufp = NULL;

        buf = rd_malloc(len ? len : 1);
        if (!buf) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate input buffer "
                           "(%" PRIusz " bytes) for zstd compression",
                           len);
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        copied = rd_slice_read(slice, buf, len);
        if (copied != len || rd_slice_remains(slice) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to read input slice for zstd compression: "
                           "expected %" PRIusz ", copied %" PRIusz,
                           len, copied);
                rd_free(buf);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        *bufp = buf;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_kafka_resp_err_t
rd_kafka_zstd_compress_buffer(rd_kafka_broker_t *rkb,
                              int comp_level,
                              const void *input,
                              size_t len,
                              void **outbuf,
                              size_t *outlenp) {
        ZSTD_CStream *cctx;
        size_t r;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        ZSTD_outBuffer out = {0};
        ZSTD_inBuffer in;

        *outbuf  = NULL;
        out.pos  = 0;
        out.size = ZSTD_compressBound(len);
        out.dst  = rd_malloc(out.size ? out.size : 1);
        if (!out.dst) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate output buffer "
                           "(%" PRIusz " bytes): %s",
                           out.size, rd_strerror(errno));
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        cctx = ZSTD_createCStream();
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
#endif
        if (ZSTD_isError(r)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to begin ZSTD compression "
                           "(out buffer is %" PRIusz " bytes): %s",
                           out.size, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        in.src  = input;
        in.size = len;
        in.pos  = 0;
        r       = ZSTD_compressStream(cctx, &out, &in);
        if (unlikely(ZSTD_isError(r))) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "ZSTD compression failed "
                           "(at of %" PRIusz
                           " bytes, with "
                           "%" PRIusz
                           " bytes remaining in out buffer): "
                           "%s",
                           in.size, out.size - out.pos, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        if (in.pos < in.size) {
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
                ZSTD_freeCStream(cctx);

        if (err)
                rd_free(out.dst);

        return err;
}

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


static rd_kafka_resp_err_t
rd_kafka_zstd_read_file(rd_kafka_broker_t *rkb,
                        const char *path,
                        void **bufp,
                        size_t *lenp) {
        FILE *fp;
        long fsize;
        void *buf;
        size_t nread;

        *bufp = NULL;
        *lenp = 0;

        fp = fopen(path, "rb");
        if (!fp) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to open zstd dictionary %s: %s", path,
                           rd_strerror(errno));
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        if (fseek(fp, 0, SEEK_END) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to seek zstd dictionary %s: %s", path,
                           rd_strerror(errno));
                fclose(fp);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        fsize = ftell(fp);
        if (fsize < 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to size zstd dictionary %s: %s", path,
                           rd_strerror(errno));
                fclose(fp);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        if (fseek(fp, 0, SEEK_SET) != 0) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to rewind zstd dictionary %s: %s", path,
                           rd_strerror(errno));
                fclose(fp);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        buf = rd_malloc((size_t)fsize);
        if (!buf) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate %" PRIusz
                           " bytes for zstd dictionary %s",
                           (size_t)fsize, path);
                fclose(fp);
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        nread = fread(buf, 1, (size_t)fsize, fp);
        fclose(fp);

        if (nread != (size_t)fsize) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Failed to read zstd dictionary %s: "
                           "expected %" PRIusz ", got %" PRIusz,
                           path, (size_t)fsize, nread);
                rd_free(buf);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        *bufp = buf;
        *lenp = (size_t)fsize;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_kafka_resp_err_t
rd_kafka_zstd_compress_with_dict(rd_kafka_broker_t *rkb,
                                 int comp_level,
                                 rd_slice_t *slice,
                                 const char *dict_path,
                                 const char *sample_dump_path,
                                 void **outbuf,
                                 size_t *outlenp) {
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        size_t len              = rd_slice_remains(slice);
        void *dict_buf          = NULL;
        size_t dict_len         = 0;
        char *input             = NULL;
        ZSTD_CCtx *cctx         = NULL;
        ZSTD_CDict *cdict       = NULL;
        size_t r;
        struct {
                void *dst;
                size_t size;
                size_t pos;
        } out = {.dst = NULL, .size = 0, .pos = 0};

        *outbuf = NULL;

        err = rd_kafka_zstd_read_file(rkb, dict_path, &dict_buf, &dict_len);
        if (err)
                goto done;

        err = rd_kafka_zstd_copy_slice(rkb, slice, len, &input);
        if (err)
                goto done;

        rd_kafka_zstd_sample_dump_batch(rkb, sample_dump_path, input, len);

        out.size = ZSTD_compressBound(len);
        out.dst  = rd_malloc(out.size ? out.size : 1);
        if (!out.dst) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate output buffer "
                           "(%" PRIusz " bytes): %s",
                           out.size, rd_strerror(errno));
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }

        cctx = ZSTD_createCCtx();
        if (!cctx) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to create ZSTD compression context");
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }

        cdict = ZSTD_createCDict(dict_buf, dict_len, comp_level);
        if (!cdict) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to create ZSTD compression dictionary "
                           "from %s",
                           dict_path);
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        r = ZSTD_compress_usingCDict(cctx, out.dst, out.size, input, len,
                                     cdict);
        if (ZSTD_isError(r)) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "ZSTD dictionary compression failed for %s: %s",
                           dict_path, ZSTD_getErrorName(r));
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }

        *outbuf  = out.dst;
        *outlenp = r;
        out.dst  = NULL;

done:
        if (cdict)
                ZSTD_freeCDict(cdict);
        if (cctx)
                ZSTD_freeCCtx(cctx);
        if (out.dst)
                rd_free(out.dst);
        if (input)
                rd_free(input);
        if (dict_buf)
                rd_free(dict_buf);

        return err;
}

rd_kafka_resp_err_t rd_kafka_zstd_compress(rd_kafka_broker_t *rkb,
                                           int comp_level,
                                           rd_slice_t *slice,
                                           const char *dict_path,
                                           const char *sample_dump_path,
                                           void **outbuf,
                                           size_t *outlenp) {
        ZSTD_CStream *cctx = NULL;
        size_t r;
        size_t len              = rd_slice_remains(slice);
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        char *input             = NULL;
        ZSTD_outBuffer out = {0};
        ZSTD_inBuffer in;

        if (dict_path && *dict_path)
                return rd_kafka_zstd_compress_with_dict(
                    rkb, comp_level, slice, dict_path, sample_dump_path,
                    outbuf, outlenp);

        if (sample_dump_path && *sample_dump_path) {
                err = rd_kafka_zstd_copy_slice(rkb, slice, len, &input);
                if (err)
                        goto done;

                rd_kafka_zstd_sample_dump_batch(rkb, sample_dump_path, input,
                                               len);
                err = rd_kafka_zstd_compress_buffer(rkb, comp_level, input, len,
                                                    outbuf, outlenp);
                goto done;
        }

        *outbuf  = NULL;
        out.pos  = 0;
        out.size = ZSTD_compressBound(len);
        out.dst  = rd_malloc(out.size ? out.size : 1);
        if (!out.dst) {
                rd_rkb_dbg(rkb, MSG, "ZSTDCOMPR",
                           "Unable to allocate output buffer "
                           "(%" PRIusz " bytes): %s",
                           out.size, rd_strerror(errno));
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }

        cctx = ZSTD_createCStream();
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
                ZSTD_freeCStream(cctx);

        if (err && out.dst)
                rd_free(out.dst);
        if (input)
                rd_free(input);

        return err;
}
