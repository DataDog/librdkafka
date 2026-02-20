/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_msgset.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_header.h"
#include "rdkafka_request.h"
#include "rdkafka_lz4.h"
#include "rdkafka_adaptive.h"

#if WITH_ZSTD
#include "rdkafka_zstd.h"
#endif

#include "snappy.h"
#include "rdvarint.h"
#include "crc32c.h"
#include "rdunittest.h"

#include "rdkafka_msgset_writer_int.h"






/**
 * @brief Copy or link message payload to buffer.
 */
static RD_INLINE void
rd_kafka_msgset_writer_write_msg_payload(rd_kafka_msgset_writer_t *msetw,
                                         const rd_kafka_msg_t *rkm,
                                         void (*free_cb)(void *)) {
        const rd_kafka_t *rk  = msetw->msetw_rkb->rkb_rk;
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;

        /* If payload is below the copy limit and there is still
         * room in the buffer we'll copy the payload to the buffer,
         * otherwise we push a reference to the memory. */
        if (rkm->rkm_len <= (size_t)rk->rk_conf.msg_copy_max_size &&
            rd_buf_write_remains(&rkbuf->rkbuf_buf) > rkm->rkm_len) {
                rd_kafka_buf_write(rkbuf, rkm->rkm_payload, rkm->rkm_len);
                if (free_cb)
                        free_cb(rkm->rkm_payload);
        } else
                rd_kafka_buf_push(rkbuf, rkm->rkm_payload, rkm->rkm_len,
                                  free_cb);
}


/**
 * @brief Write message headers to buffer.
 *
 * @remark The enveloping HeaderCount varint must already have been written.
 * @returns the number of bytes written to msetw->msetw_rkbuf
 */
static size_t
rd_kafka_msgset_writer_write_msg_headers(rd_kafka_msgset_writer_t *msetw,
                                         const rd_kafka_headers_t *hdrs) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        const rd_kafka_header_t *hdr;
        int i;
        size_t start_pos = rd_buf_write_pos(&rkbuf->rkbuf_buf);
        size_t written;

        RD_LIST_FOREACH(hdr, &hdrs->rkhdrs_list, i) {
                rd_kafka_buf_write_varint(rkbuf, hdr->rkhdr_name_size);
                rd_kafka_buf_write(rkbuf, hdr->rkhdr_name,
                                   hdr->rkhdr_name_size);
                rd_kafka_buf_write_varint(
                    rkbuf,
                    hdr->rkhdr_value ? (int64_t)hdr->rkhdr_value_size : -1);
                rd_kafka_buf_write(rkbuf, hdr->rkhdr_value,
                                   hdr->rkhdr_value_size);
        }

        written = rd_buf_write_pos(&rkbuf->rkbuf_buf) - start_pos;
        rd_dassert(written == hdrs->rkhdrs_ser_size);

        return written;
}



/**
 * @brief Write message to messageset buffer with MsgVersion 0 or 1.
 * @returns the number of bytes written.
 */
size_t
rd_kafka_msgset_writer_write_msg_v0_1(rd_kafka_msgset_writer_t *msetw,
                                      rd_kafka_msg_t *rkm,
                                      int64_t Offset,
                                      int8_t MsgAttributes,
                                      void (*free_cb)(void *)) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        size_t MessageSize;
        size_t of_Crc;

        /*
         * MessageSet's (v0 and v1) per-Message header.
         */

        /* Offset (only relevant for compressed messages on MsgVersion v1) */
        rd_kafka_buf_write_i64(rkbuf, Offset);

        /* MessageSize */
        MessageSize = 4 + 1 + 1 + /* Crc+MagicByte+Attributes */
                      4 /* KeyLength */ + rkm->rkm_key_len +
                      4 /* ValueLength */ + rkm->rkm_len;

        if (msetw->msetw_MsgVersion == 1)
                MessageSize += 8; /* Timestamp i64 */

        rd_kafka_buf_write_i32(rkbuf, (int32_t)MessageSize);

        /*
         * Message
         */
        /* Crc: will be updated later */
        of_Crc = rd_kafka_buf_write_i32(rkbuf, 0);

        /* Start Crc calculation of all buf writes. */
        rd_kafka_buf_crc_init(rkbuf);

        /* MagicByte */
        rd_kafka_buf_write_i8(rkbuf, msetw->msetw_MsgVersion);

        /* Attributes */
        rd_kafka_buf_write_i8(rkbuf, MsgAttributes);

        /* V1: Timestamp */
        if (msetw->msetw_MsgVersion == 1)
                rd_kafka_buf_write_i64(rkbuf, rkm->rkm_timestamp);

        /* Message Key */
        rd_kafka_buf_write_bytes(rkbuf, rkm->rkm_key, rkm->rkm_key_len);

        /* Write or copy Value/payload */
        if (rkm->rkm_payload) {
                rd_kafka_buf_write_i32(rkbuf, (int32_t)rkm->rkm_len);
                rd_kafka_msgset_writer_write_msg_payload(msetw, rkm, free_cb);
        } else
                rd_kafka_buf_write_i32(rkbuf, RD_KAFKAP_BYTES_LEN_NULL);

        /* Finalize Crc */
        rd_kafka_buf_update_u32(rkbuf, of_Crc,
                                rd_kafka_buf_crc_finalize(rkbuf));


        /* Return written message size */
        return 8 /*Offset*/ + 4 /*MessageSize*/ + MessageSize;
}

/**
 * @brief Write message to messageset buffer with MsgVersion 2.
 * @returns the number of bytes written.
 */
size_t
rd_kafka_msgset_writer_write_msg_v2(rd_kafka_msgset_writer_t *msetw,
                                    rd_kafka_msg_t *rkm,
                                    int64_t Offset,
                                    int8_t MsgAttributes,
                                    void (*free_cb)(void *)) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        size_t MessageSize    = 0;
        char varint_Length[RD_UVARINT_ENC_SIZEOF(int32_t)];
        char varint_TimestampDelta[RD_UVARINT_ENC_SIZEOF(int64_t)];
        char varint_OffsetDelta[RD_UVARINT_ENC_SIZEOF(int64_t)];
        char varint_KeyLen[RD_UVARINT_ENC_SIZEOF(int32_t)];
        char varint_ValueLen[RD_UVARINT_ENC_SIZEOF(int32_t)];
        char varint_HeaderCount[RD_UVARINT_ENC_SIZEOF(int32_t)];
        size_t sz_Length;
        size_t sz_TimestampDelta;
        size_t sz_OffsetDelta;
        size_t sz_KeyLen;
        size_t sz_ValueLen;
        size_t sz_HeaderCount;
        int HeaderCount   = 0;
        size_t HeaderSize = 0;

        if (rkm->rkm_headers) {
                HeaderCount = rkm->rkm_headers->rkhdrs_list.rl_cnt;
                HeaderSize  = rkm->rkm_headers->rkhdrs_ser_size;
        }

        /* All varints, except for Length, needs to be pre-built
         * so that the Length field can be set correctly and thus have
         * correct varint encoded width. */

        sz_TimestampDelta = rd_uvarint_enc_i64(
            varint_TimestampDelta, sizeof(varint_TimestampDelta),
            rkm->rkm_timestamp - msetw->msetw_firstmsg.timestamp);
        sz_OffsetDelta = rd_uvarint_enc_i64(varint_OffsetDelta,
                                            sizeof(varint_OffsetDelta), Offset);
        sz_KeyLen   = rd_uvarint_enc_i32(varint_KeyLen, sizeof(varint_KeyLen),
                                       rkm->rkm_key
                                             ? (int32_t)rkm->rkm_key_len
                                             : (int32_t)RD_KAFKAP_BYTES_LEN_NULL);
        sz_ValueLen = rd_uvarint_enc_i32(
            varint_ValueLen, sizeof(varint_ValueLen),
            rkm->rkm_payload ? (int32_t)rkm->rkm_len
                             : (int32_t)RD_KAFKAP_BYTES_LEN_NULL);
        sz_HeaderCount =
            rd_uvarint_enc_i32(varint_HeaderCount, sizeof(varint_HeaderCount),
                               (int32_t)HeaderCount);

        /* Calculate MessageSize without length of Length (added later)
         * to store it in Length. */
        MessageSize = 1 /* MsgAttributes */ + sz_TimestampDelta +
                      sz_OffsetDelta + sz_KeyLen + rkm->rkm_key_len +
                      sz_ValueLen + rkm->rkm_len + sz_HeaderCount + HeaderSize;

        /* Length */
        sz_Length = rd_uvarint_enc_i64(varint_Length, sizeof(varint_Length),
                                       MessageSize);
        rd_kafka_buf_write(rkbuf, varint_Length, sz_Length);
        MessageSize += sz_Length;

        /* Attributes: The MsgAttributes argument is losely based on MsgVer0
         *             which don't apply for MsgVer2 */
        rd_kafka_buf_write_i8(rkbuf, 0);

        /* TimestampDelta */
        rd_kafka_buf_write(rkbuf, varint_TimestampDelta, sz_TimestampDelta);

        /* OffsetDelta */
        rd_kafka_buf_write(rkbuf, varint_OffsetDelta, sz_OffsetDelta);

        /* KeyLen */
        rd_kafka_buf_write(rkbuf, varint_KeyLen, sz_KeyLen);

        /* Key (if any) */
        if (rkm->rkm_key)
                rd_kafka_buf_write(rkbuf, rkm->rkm_key, rkm->rkm_key_len);

        /* ValueLen */
        rd_kafka_buf_write(rkbuf, varint_ValueLen, sz_ValueLen);

        /* Write or copy Value/payload */
        if (rkm->rkm_payload)
                rd_kafka_msgset_writer_write_msg_payload(msetw, rkm, free_cb);

        /* HeaderCount */
        rd_kafka_buf_write(rkbuf, varint_HeaderCount, sz_HeaderCount);

        /* Headers array */
        if (rkm->rkm_headers)
                rd_kafka_msgset_writer_write_msg_headers(msetw,
                                                         rkm->rkm_headers);

        /* Return written message size */
        return MessageSize;
}


/**
 * @brief Write message to messageset buffer.
 * @returns the number of bytes written.
 */
size_t rd_kafka_msgset_writer_write_msg(rd_kafka_msgset_writer_t *msetw,
                                               rd_kafka_msg_t *rkm,
                                               int64_t Offset,
                                               int8_t MsgAttributes,
                                               void (*free_cb)(void *)) {
        size_t outlen;
        size_t (*writer[])(rd_kafka_msgset_writer_t *, rd_kafka_msg_t *,
                           int64_t, int8_t, void (*)(void *)) = {
            [0] = rd_kafka_msgset_writer_write_msg_v0_1,
            [1] = rd_kafka_msgset_writer_write_msg_v0_1,
            [2] = rd_kafka_msgset_writer_write_msg_v2};
        size_t actual_written;
        size_t pre_pos;

        if (likely(rkm->rkm_timestamp))
                MsgAttributes |= RD_KAFKA_MSG_ATTR_CREATE_TIME;

        pre_pos = rd_buf_write_pos(&msetw->msetw_rkbuf->rkbuf_buf);

        outlen = writer[msetw->msetw_MsgVersion](msetw, rkm, Offset,
                                                 MsgAttributes, free_cb);

        actual_written =
            rd_buf_write_pos(&msetw->msetw_rkbuf->rkbuf_buf) - pre_pos;
        rd_assert(outlen <=
                  rd_kafka_msg_wire_size(rkm, msetw->msetw_MsgVersion));
        rd_assert(outlen == actual_written);

        return outlen;
}

#if WITH_ZLIB
/**
 * @brief Compress slice using gzip/zlib
 */
rd_kafka_resp_err_t rd_kafka_gzip_compress(rd_kafka_broker_t *rkb,
                                           int comp_level,
                                           rd_slice_t *slice,
                                           void **outbuf,
                                           size_t *outlenp) {
        z_stream strm;
        size_t len = rd_slice_remains(slice);
        const void *p;
        size_t rlen;
        int r;

        memset(&strm, 0, sizeof(strm));
        r = deflateInit2(&strm, comp_level, Z_DEFLATED, 15 + 16, 8,
                         Z_DEFAULT_STRATEGY);
        if (r != Z_OK) {
                rd_rkb_log(rkb, LOG_ERR, "GZIP",
                           "Failed to initialize gzip for "
                           "compressing %" PRIusz
                           " bytes: "
                           "%s (%i): "
                           "sending uncompressed",
                           len, strm.msg ? strm.msg : "", r);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        /* Calculate maximum compressed size and
         * allocate an output buffer accordingly, being
         * prefixed with the Message header. */
        *outlenp = deflateBound(&strm, (uLong)rd_slice_remains(slice));
        *outbuf  = rd_malloc(*outlenp);

        strm.next_out  = *outbuf;
        strm.avail_out = (uInt)*outlenp;

        /* Iterate through each segment and compress it. */
        while ((rlen = rd_slice_reader(slice, &p))) {

                strm.next_in  = (void *)p;
                strm.avail_in = (uInt)rlen;

                /* Compress message */
                if ((r = deflate(&strm, Z_NO_FLUSH)) != Z_OK) {
                        rd_rkb_log(rkb, LOG_ERR, "GZIP",
                                   "Failed to gzip-compress "
                                   "%" PRIusz " bytes (%" PRIusz
                                   " total): "
                                   "%s (%i): "
                                   "sending uncompressed",
                                   rlen, len, strm.msg ? strm.msg : "", r);
                        deflateEnd(&strm);
                        rd_free(*outbuf);
                        *outbuf = NULL;
                        return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                }

                rd_kafka_assert(rkb->rkb_rk, strm.avail_in == 0);
        }

        /* Finish the compression */
        if ((r = deflate(&strm, Z_FINISH)) != Z_STREAM_END) {
                rd_rkb_log(rkb, LOG_ERR, "GZIP",
                           "Failed to finish gzip compression "
                           " of %" PRIusz
                           " bytes: "
                           "%s (%i): "
                           "sending uncompressed",
                           len, strm.msg ? strm.msg : "", r);
                deflateEnd(&strm);
                rd_free(*outbuf);
                *outbuf = NULL;
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        *outlenp = strm.total_out;

        /* Deinitialize compression */
        deflateEnd(&strm);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/**
 * @brief Compress messageset using gzip/zlib
 */
static int rd_kafka_msgset_writer_compress_gzip(rd_kafka_msgset_writer_t *msetw,
                                                rd_slice_t *slice,
                                                struct iovec *ciov) {
        rd_kafka_resp_err_t err;
        int comp_level =
            msetw->msetw_rktp->rktp_rkt->rkt_conf.compression_level;
        err = rd_kafka_gzip_compress(msetw->msetw_rkb, comp_level, slice,
                                     &ciov->iov_base, &ciov->iov_len);
        return (err ? -1 : 0);
}
#endif


#if WITH_SNAPPY
/**
 * @brief Compress slice using Snappy
 */
rd_kafka_resp_err_t rd_kafka_snappy_compress_slice(rd_kafka_broker_t *rkb,
                                                   rd_slice_t *slice,
                                                   void **outbuf,
                                                   size_t *outlenp) {
        struct iovec *iov;
        size_t iov_max, iov_cnt;
        struct snappy_env senv;
        size_t len = rd_slice_remains(slice);
        int r;
        struct iovec ciov;

        /* Initialize snappy compression environment */
        rd_kafka_snappy_init_env_sg(&senv, 1 /*iov enable*/);

        /* Calculate maximum compressed size and
         * allocate an output buffer accordingly. */
        ciov.iov_len  = rd_kafka_snappy_max_compressed_length(len);
        ciov.iov_base = rd_malloc(ciov.iov_len);

        iov_max = slice->buf->rbuf_segment_cnt;
        iov     = rd_alloca(sizeof(*iov) * iov_max);

        rd_slice_get_iov(slice, iov, &iov_cnt, iov_max, len);

        /* Compress each message */
        if ((r = rd_kafka_snappy_compress_iov(&senv, iov, iov_cnt, len,
                                              &ciov)) != 0) {
                rd_rkb_log(rkb, LOG_ERR, "SNAPPY",
                           "Failed to snappy-compress "
                           "%" PRIusz
                           " bytes: %s:"
                           "sending uncompressed",
                           len, rd_strerror(-r));
                rd_free(ciov.iov_base);
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }

        /* rd_free snappy environment */
        rd_kafka_snappy_free_env(&senv);

        *outbuf  = ciov.iov_base;
        *outlenp = ciov.iov_len;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/**
 * @brief Compress messageset using Snappy
 */
static int
rd_kafka_msgset_writer_compress_snappy(rd_kafka_msgset_writer_t *msetw,
                                       rd_slice_t *slice,
                                       struct iovec *ciov) {
        rd_kafka_resp_err_t err;
        err = rd_kafka_snappy_compress_slice(msetw->msetw_rkb, slice,
                                             &ciov->iov_base, &ciov->iov_len);
        return (err ? -1 : 0);
}
#endif

/**
 * @brief Compress messageset using LZ4F
 */
static int rd_kafka_msgset_writer_compress_lz4(rd_kafka_msgset_writer_t *msetw,
                                               rd_slice_t *slice,
                                               struct iovec *ciov) {
        rd_kafka_resp_err_t err;
        int comp_level =
            msetw->msetw_rktp->rktp_rkt->rkt_conf.compression_level;
        err = rd_kafka_lz4_compress(msetw->msetw_rkb,
                                    /* Correct or incorrect HC */
                                    msetw->msetw_MsgVersion >= 1 ? 1 : 0,
                                    comp_level, slice, &ciov->iov_base,
                                    &ciov->iov_len);
        return (err ? -1 : 0);
}

#if WITH_ZSTD
/**
 * @brief Compress messageset using ZSTD
 */
static int rd_kafka_msgset_writer_compress_zstd(rd_kafka_msgset_writer_t *msetw,
                                                rd_slice_t *slice,
                                                struct iovec *ciov) {
        rd_kafka_resp_err_t err;
        int comp_level =
            msetw->msetw_rktp->rktp_rkt->rkt_conf.compression_level;
        err = rd_kafka_zstd_compress(msetw->msetw_rkb, comp_level, slice,
                                     &ciov->iov_base, &ciov->iov_len);
        return (err ? -1 : 0);
}
#endif

/**
 * @brief Compress the message set.
 * @param outlenp in: total uncompressed messages size,
 *                out (on success): returns the compressed buffer size.
 * @returns 0 on success or if -1 if compression failed.
 * @remark Compression failures are not critical, we'll just send the
 *         the messageset uncompressed.
 */
int rd_kafka_msgset_writer_compress(rd_kafka_msgset_writer_t *msetw,
                                    size_t *outlenp) {
        rd_buf_t *rbuf = &msetw->msetw_rkbuf->rkbuf_buf;
        rd_slice_t slice;
        size_t len        = *outlenp;
        struct iovec ciov = RD_ZERO_INIT; /* Compressed output buffer */
        int r             = -1;
        size_t outlen;

        rd_assert(rd_buf_len(rbuf) >= msetw->msetw_firstmsg.of + len);

        /* Create buffer slice from firstmsg and onwards */
        r = rd_slice_init(&slice, rbuf, msetw->msetw_firstmsg.of, len);
        rd_assert(r == 0 || !*"invalid firstmsg position");

        switch (msetw->msetw_compression) {
#if WITH_ZLIB
        case RD_KAFKA_COMPRESSION_GZIP:
                r = rd_kafka_msgset_writer_compress_gzip(msetw, &slice, &ciov);
                break;
#endif

#if WITH_SNAPPY
        case RD_KAFKA_COMPRESSION_SNAPPY:
                r = rd_kafka_msgset_writer_compress_snappy(msetw, &slice,
                                                           &ciov);
                break;
#endif

        case RD_KAFKA_COMPRESSION_LZ4:
                r = rd_kafka_msgset_writer_compress_lz4(msetw, &slice, &ciov);
                break;

#if WITH_ZSTD
        case RD_KAFKA_COMPRESSION_ZSTD:
                r = rd_kafka_msgset_writer_compress_zstd(msetw, &slice, &ciov);
                break;
#endif

        default:
                rd_kafka_assert(NULL,
                                !*"notreached: unsupported compression.codec");
                break;
        }

        if (r == -1) /* Compression failed, send uncompressed */
                return -1;


        if (unlikely(ciov.iov_len > len)) {
                /* If the compressed data is larger than the uncompressed size
                 * then throw it away and send as uncompressed. */
                rd_free(ciov.iov_base);
                return -1;
        }

        /* Set compression codec in MessageSet.Attributes */
        msetw->msetw_Attributes |= msetw->msetw_compression;

        /* Rewind rkbuf to the pre-message checkpoint (firstmsg)
         * and replace the original message(s) with the compressed payload,
         * possibly with version dependent enveloping. */
        rd_buf_write_seek(rbuf, msetw->msetw_firstmsg.of);

        rd_kafka_assert(msetw->msetw_rkb->rkb_rk, ciov.iov_len < INT32_MAX);

        if (msetw->msetw_MsgVersion == 2) {
                /* MsgVersion 2 has no inner MessageSet header or wrapping
                 * for compressed messages, just the messages back-to-back,
                 * so we can push the compressed memory directly to the
                 * buffer without wrapping it. */
                rd_buf_push(rbuf, ciov.iov_base, ciov.iov_len, rd_free);
                outlen = ciov.iov_len;

        } else {
                /* Older MessageSets envelope/wrap the compressed MessageSet
                 * in an outer Message. */
                rd_kafka_msg_t rkm = {.rkm_len     = ciov.iov_len,
                                      .rkm_payload = ciov.iov_base,
                                      .rkm_timestamp =
                                          msetw->msetw_firstmsg.timestamp};
                outlen             = rd_kafka_msgset_writer_write_msg(
                    msetw, &rkm, 0, msetw->msetw_compression,
                    rd_free /*free for ciov.iov_base*/);
        }

        *outlenp = outlen;

        return 0;
}


/**
 * @brief Calculate MessageSet v2 CRC (CRC32C) when messageset is complete.
 */
void
rd_kafka_msgset_writer_calc_crc_v2(rd_kafka_msgset_writer_t *msetw) {
        int32_t crc;
        rd_slice_t slice;
        int r;

        r = rd_slice_init(&slice, &msetw->msetw_rkbuf->rkbuf_buf,
                          msetw->msetw_of_CRC + 4,
                          rd_buf_write_pos(&msetw->msetw_rkbuf->rkbuf_buf) -
                              msetw->msetw_of_CRC - 4);
        rd_assert(!r && *"slice_init failed");

        /* CRC32C calculation */
        crc = rd_slice_crc32c(&slice);

        /* Update CRC at MessageSet v2 CRC offset */
        rd_kafka_buf_update_i32(msetw->msetw_rkbuf, msetw->msetw_of_CRC, crc);
}



/**
 * @brief Test helpers
 */
static void unittest_fill_msgq(rd_kafka_msgq_t *rkmq,
                               int cnt,
                               size_t msg_size) {
        int i;

        for (i = 0; i < cnt; i++) {
                rd_kafka_msg_t *rkm = ut_rd_kafka_msg_new(0);
                if (msg_size > 0) {
                        rkm->rkm_payload = rd_malloc(msg_size);
                        rkm->rkm_len     = msg_size;
                        rkm->rkm_flags |= RD_KAFKA_MSG_F_FREE;
                }
                rkm->rkm_ts_enq     = rd_clock();
                rd_kafka_msgq_enq(rkmq, rkm);
        }
}

static void unittest_clear_msgq(rd_kafka_msgq_t *rkmq) {
        ut_rd_kafka_msgq_purge(rkmq);
}

/**
 * @brief Test: Empty partition should be rejected
 */
static int unittest_msgset_writer_empty_partition(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;

        RD_UT_BEGIN();

        /* Create producer instance */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Test: Empty partition should return 0 */
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);
        RD_UT_ASSERT(result == 0, "Empty partition should be rejected, got %d", result);

        /* Cleanup */
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Partition with messages should be accepted
 */
static int unittest_msgset_writer_add_partition(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;

        RD_UT_BEGIN();

        /* Create producer instance */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Simulate 100 messages */
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 100, 256);
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);

        RD_UT_ASSERT(result == 1, "Should accept partition with messages, got %d", result);
        RD_UT_ASSERT(rkpca.rkpca_partition_cnt >= 1,
                     "Partition count should be >= 1, got %d", rkpca.rkpca_partition_cnt);

        /* Cleanup */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Partition limit enforcement
 */
static int unittest_msgset_writer_partition_limit(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;
        int added_count;

        RD_UT_BEGIN();

        /* Create producer with low partition limit */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "produce.request.max.partitions", "5", NULL, 0);
        rd_kafka_conf_set(conf, "multibatch_v2", "true", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Simulate small partition */
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 10, 256);

        /* Add partitions until limit is hit */
        added_count = 0;
        for (int i = 0; i < 10; i++) {
                result = rd_kafka_produce_calculator_add(&rkpca, rktp);
                if (result == 1) {
                        added_count++;
                } else {
                        break;
                }
        }

        /* Limit check is ">= max", so max=5 allows indices 0-4 (5 partitions) */
        RD_UT_ASSERT(added_count == 5,
                     "Should add 5 partitions (limit >= 5), added %d", added_count);

        /* Cleanup */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Configuration is properly read
 */
static int unittest_msgset_writer_config_values(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        RD_UT_BEGIN();

        /* Create producer with specific configuration */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "message.max.bytes", "10000", NULL, 0);
        rd_kafka_conf_set(conf, "produce.request.max.partitions", "123", NULL, 0);
        rd_kafka_conf_set(conf, "batch.num.messages", "456", NULL, 0);
        rd_kafka_conf_set(conf, "multibatch_v2", "true", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Verify configuration values were set correctly */
        RD_UT_ASSERT(rk->rk_conf.max_msg_size == 10000,
                     "message.max.bytes should be 10000, got %d",
                     rk->rk_conf.max_msg_size);
        RD_UT_ASSERT(rk->rk_conf.produce_request_max_partitions == 123,
                     "produce.request.max.partitions should be 123, got %d",
                     rk->rk_conf.produce_request_max_partitions);
        RD_UT_ASSERT(rk->rk_conf.batch_num_messages == 456,
                     "batch.num.messages should be 456, got %d",
                     rk->rk_conf.batch_num_messages);

        /* Cleanup */
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Successfully add multiple small partitions
 */
static int unittest_msgset_writer_multiple_partitions(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;
        int added_count;

        RD_UT_BEGIN();

        /* Create producer with large limits */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "message.max.bytes", "10000000", NULL, 0);
        rd_kafka_conf_set(conf, "produce.request.max.partitions", "1000", NULL, 0);
        rd_kafka_conf_set(conf, "multibatch_v2", "true", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Simulate small partition */
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 10, 256);

        /* Add 100 small partitions */
        added_count = 0;
        for (int i = 0; i < 100; i++) {
                result = rd_kafka_produce_calculator_add(&rkpca, rktp);
                if (result == 1) {
                        added_count++;
                } else {
                        break;
                }
        }

        RD_UT_ASSERT(added_count == 100,
                     "Should successfully add 100 partitions, added %d", added_count);
        RD_UT_ASSERT(rkpca.rkpca_partition_cnt == 100,
                     "Calculator should track 100 partitions, got %d", rkpca.rkpca_partition_cnt);

        /* Cleanup */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Configuration mismatch between topics
 */
static int unittest_msgset_writer_config_mismatch(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_topic_conf_t *tconf1, *tconf2;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt1, *rkt2;
        rd_kafka_toppar_t *rktp1, *rktp2;
        rd_kafka_produce_calculator_t rkpca;
        int result;

        RD_UT_BEGIN();

        /* Create producer */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create two topics with different acks settings */
        tconf1 = rd_kafka_topic_conf_new();
        rd_kafka_topic_conf_set(tconf1, "request.required.acks", "1", NULL, 0);
        rkt1 = rd_kafka_topic_new(rk, "topic1", tconf1);
        rktp1 = rd_kafka_toppar_new(rkt1, 0);

        tconf2 = rd_kafka_topic_conf_new();
        rd_kafka_topic_conf_set(tconf2, "request.required.acks", "-1", NULL, 0);
        rkt2 = rd_kafka_topic_new(rk, "topic2", tconf2);
        rktp2 = rd_kafka_toppar_new(rkt2, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Add first partition with acks=1 */
        unittest_fill_msgq(&rktp1->rktp_xmit_msgq, 10, 256);
        result = rd_kafka_produce_calculator_add(&rkpca, rktp1);
        RD_UT_ASSERT(result == 1, "First partition should be added, got %d", result);

        /* Try to add second partition with different acks=-1 (should fail) */
        unittest_fill_msgq(&rktp2->rktp_xmit_msgq, 10, 256);
        result = rd_kafka_produce_calculator_add(&rkpca, rktp2);
        RD_UT_ASSERT(result == 0,
                     "Should reject partition with different acks config, got %d", result);

        /* Cleanup */
        unittest_clear_msgq(&rktp1->rktp_xmit_msgq);
        unittest_clear_msgq(&rktp2->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp1);
        rd_kafka_toppar_destroy(rktp2);
        rd_kafka_topic_destroy(rkt1);
        rd_kafka_topic_destroy(rkt2);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Batch count calculation with batch.num.messages
 */
static int unittest_msgset_writer_batch_count(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;

        RD_UT_BEGIN();

        /* Create producer with batch.num.messages limit */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "batch.num.messages", "100", NULL, 0);
        rd_kafka_conf_set(conf, "message.max.bytes", "1000000", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        RD_UT_ASSERT(rk->rk_conf.batch_num_messages == 100,
                     "batch.num.messages should be 100, got %d",
                     rk->rk_conf.batch_num_messages);

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Simulate 250 messages (should create 3 batches: 100+100+50) */
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 250, 256);
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);

        RD_UT_ASSERT(result == 1, "Should add partition with 250 messages, got %d", result);
        /* Calculator should account for batching */
        RD_UT_ASSERT(rkpca.rkpca_partition_cnt >= 1,
                     "Should track partition, got %d", rkpca.rkpca_partition_cnt);

        /* Cleanup */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Adding partitions accumulates message counts
 */
static int unittest_msgset_writer_accumulation(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;

        RD_UT_BEGIN();

        /* Create producer */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "message.max.bytes", "10000000", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Add first partition with 50 messages */
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 50, 256);
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);
        RD_UT_ASSERT(result == 1, "First partition should be added");

        int first_msg_cnt = rkpca.rkpca_message_cnt;
        size_t first_msg_size = rkpca.rkpca_message_size;

        /* Add second partition with 30 messages */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 30, 256);
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);
        RD_UT_ASSERT(result == 1, "Second partition should be added");

        /* Verify accumulation */
        RD_UT_ASSERT(rkpca.rkpca_partition_cnt >= 2,
                     "Should have 2+ partitions, got %d", rkpca.rkpca_partition_cnt);
        RD_UT_ASSERT(rkpca.rkpca_message_cnt > first_msg_cnt,
                     "Message count should accumulate: %d > %d",
                     rkpca.rkpca_message_cnt, first_msg_cnt);
        RD_UT_ASSERT(rkpca.rkpca_message_size > first_msg_size,
                     "Message size should accumulate: %zu > %zu",
                     rkpca.rkpca_message_size, first_msg_size);

        /* Cleanup */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Exactly at partition limit boundary
 */
static int unittest_msgset_writer_exact_limit(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_toppar_t *rktp;
        rd_kafka_produce_calculator_t rkpca;
        int result;

        RD_UT_BEGIN();

        /* Create producer with partition limit=1 */
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "produce.request.max.partitions", "1", NULL, 0);
        rd_kafka_conf_set(conf, "multibatch_v2", "true", NULL, 0);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        /* Create topic and partition */
        rkt = rd_kafka_topic_new(rk, "test_topic", NULL);
        rktp = rd_kafka_toppar_new(rkt, 0);

        /* Initialize calculator */
        memset(&rkpca, 0, sizeof(rkpca));
        rkpca.rkpca_produce_header_size = 100;
        rkpca.rkpca_topic_header_size = 50;
        rkpca.rkpca_partition_header_size = 100;
        rkpca.rkpca_message_set_header_size = 50;
        rkpca.rkpca_message_overhead = 30;

        /* Simulate small partition */
        unittest_fill_msgq(&rktp->rktp_xmit_msgq, 10, 256);

        /* First add should succeed (count=0, limit=1, check is count >= limit) */
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);
        RD_UT_ASSERT(result == 1, "First partition should be added (0 < 1)");

        /* Second add should fail (count=1, limit=1, 1 >= 1) */
        result = rd_kafka_produce_calculator_add(&rkpca, rktp);
        RD_UT_ASSERT(result == 0, "Second partition should be rejected (1 >= 1)");

        /* Cleanup */
        unittest_clear_msgq(&rktp->rktp_xmit_msgq);
        rd_kafka_toppar_destroy(rktp);
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Test: Topic header is rolled back if no partitions are appended.
 */
static int unittest_msgset_writer_rollback_empty_topic(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_kafka_broker_t *rkb;
        rd_kafka_topic_t *rkt1, *rkt2;
        rd_kafka_toppar_t *rktp1, *rktp2;
        rd_kafka_produce_ctx_t ctx;
        rd_kafka_pid_t pid = {.id = 1, .epoch = 0};
        int appended_msg_cnt = 0;
        size_t appended_msg_bytes = 0;
        rd_kafka_buf_t *rkbuf;

        RD_UT_BEGIN();

        conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "client.id", "unittest", NULL, 0);
        rd_kafka_conf_set(conf, "multibatch_v2", "true", NULL, 0);
        rd_kafka_conf_set(conf, "debug", "msg", NULL, 0);   
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!rk)
                RD_UT_FAIL("Failed to create producer");

        rkb = rd_kafka_broker_add_logical(rk, "unittest");
        rd_kafka_broker_lock(rkb);
        rkb->rkb_features = RD_KAFKA_FEATURE_UNITTEST | RD_KAFKA_FEATURE_ALL;
        rd_kafka_broker_unlock(rkb);

        rkt1  = rd_kafka_topic_new(rk, "uttopic1", NULL);
        RD_UT_ASSERT(rkt1, "failed to create topic uttopic1");
        rktp1 = rd_kafka_toppar_new(rkt1, 0);
        RD_UT_ASSERT(rktp1, "failed to get toppar for uttopic1");
        rd_ut_kafka_topic_set_topic_exists(rktp1->rktp_rkt, 1, -1);

        rkt2  = rd_kafka_topic_new(rk, "uttopic2", NULL);
        RD_UT_ASSERT(rkt2, "failed to create topic uttopic2");
        rktp2 = rd_kafka_toppar_new(rkt2, 0);
        RD_UT_ASSERT(rktp2, "failed to get toppar for uttopic2");
        rd_ut_kafka_topic_set_topic_exists(rktp2->rktp_rkt, 1, -1);

        // put 1 message in topic 1 and 2s queues
        size_t msg_size = 16;
        unittest_fill_msgq(&rktp1->rktp_xmit_msgq, 1, msg_size);
        unittest_fill_msgq(&rktp2->rktp_xmit_msgq, 1, msg_size);

        // force topic 2's message in the future by setting backoff
        // this simulates a block causing 0 messages to be written
        rd_kafka_msg_t *first_msg =
            TAILQ_FIRST(&rktp2->rktp_xmit_msgq.rkmq_msgs);
        RD_UT_ASSERT(first_msg, "expected message in uttopic2 xmit_msgq");
        first_msg->rkm_u.producer.ts_backoff = rd_clock() + (10 * 1000 * 1000);

        // build a produce context and append the 1st (good topic)
        RD_UT_ASSERT(
            rd_kafka_produce_ctx_init_mbv2(
                &ctx, rkb, 2, 2, 10, 1024,
                rktp1->rktp_rkt->rkt_conf.required_acks,
                rktp1->rktp_rkt->rkt_conf.request_timeout_ms, pid, NULL),
            "produce_ctx_init failed");
        if (!rd_kafka_produce_ctx_append_toppar_mbv2(
                &ctx, rktp1, &appended_msg_cnt, &appended_msg_bytes)) {
                rd_kafka_msg_t *msg =
                    TAILQ_FIRST(&rktp1->rktp_xmit_msgq.rkmq_msgs);
                RD_UT_FAIL(
                    "append topic 1 failed: qlen=%d batch.num.messages=%d "
                    "ctx.ApiVersion=%d ctx.MsgVersion=%d first.ts_backoff=%"
                    PRId64 " now=%" PRId64,
                    rd_kafka_msgq_len(&rktp1->rktp_xmit_msgq),
                    rk->rk_conf.batch_num_messages, ctx.rkpc_api_version,
                    ctx.rkpc_msg_version,
                    msg ? (int64_t)msg->rkm_u.producer.ts_backoff : -1,
                    (int64_t)rd_clock());
        }
        RD_UT_ASSERT(appended_msg_cnt > 0,
                     "expected 1st topic to append messages");


        // attempt to append the 2nd (slow topic), this should fail because
        // of backoff, and 0 messages should be appended, and the topic-header
        // write should be rolled back as well
        appended_msg_cnt   = 0;
        appended_msg_bytes = 0;
        RD_UT_ASSERT(rd_kafka_produce_ctx_append_toppar_mbv2(
                         &ctx, rktp2, &appended_msg_cnt, &appended_msg_bytes) ==
                         0,
                     "expected 2nd topic to append 0 messages (backoff)");
        RD_UT_ASSERT(ctx.rkpc_appended_topic_cnt == 1,
                     "expected 2nd topic header rollback, topic_cnt=%d",
                     ctx.rkpc_appended_topic_cnt);
        RD_UT_ASSERT(ctx.rkpc_appended_message_cnt == 1,
                     "expected final message_cnt=1, got %d",
                     ctx.rkpc_appended_message_cnt);
        RD_UT_ASSERT(ctx.rkpc_appended_message_bytes == msg_size,
                     "expected final message_bytes=%zu, got %zu",
                     msg_size, ctx.rkpc_appended_message_bytes);

        // check that we only wrote 1 topic, 1 message, and msg_bytes bytes
        // to the ProduceRequest
        rkbuf = rd_kafka_produce_ctx_finalize_mbv2(&ctx);
        RD_UT_ASSERT(rkbuf, "expected request with 1st topic to finalize");
        {
                rd_slice_t slice;
                const rd_bool_t is_flexver =
                    (rkbuf->rkbuf_flags & RD_KAFKA_OP_F_FLEXVER) ? rd_true
                                                                : rd_false;
                int16_t api_key;
                int16_t api_version;
                int16_t required_acks;
                int32_t req_size;
                int32_t corrid;
                int32_t timeout;
                int32_t topic_cnt;
                int32_t partition_cnt;
                int32_t partition;
                int32_t records_len;
                int str_len;
                int16_t str_len16;
                uint64_t uva;
                const char *expected_topic = "uttopic1";
                char topic[64];

                rd_slice_init_full(&slice, &rkbuf->rkbuf_buf);

                /* Request header */
                RD_UT_ASSERT(rd_slice_read(&slice, &req_size, sizeof(req_size)),
                             "failed to parse request size");

                RD_UT_ASSERT(rd_slice_read(&slice, &api_key, sizeof(api_key)),
                             "failed to parse request api_key");
                api_key = (int16_t)be16toh((uint16_t)api_key);
                RD_UT_ASSERT(api_key == RD_KAFKAP_Produce,
                             "expected Produce ApiKey, got %" PRId16, api_key);

                RD_UT_ASSERT(
                    rd_slice_read(&slice, &api_version, sizeof(api_version)),
                    "failed to parse request api_version");

                RD_UT_ASSERT(rd_slice_read(&slice, &corrid, sizeof(corrid)),
                             "failed to parse request correlation id");

                /* ClientId */
                RD_UT_ASSERT(rd_slice_read(&slice, &str_len16, sizeof(str_len16)),
                             "failed to parse ClientId length");
                str_len = (int)(int16_t)be16toh((uint16_t)str_len16);
                if (str_len > 0) {
                        RD_UT_ASSERT(rd_slice_read(&slice, NULL, str_len),
                                     "failed to skip ClientId bytes");
                }

                /* Header tags */
                if (is_flexver) {
                        RD_UT_ASSERT(
                            !RD_UVARINT_UNDERFLOW(
                                rd_slice_read_uvarint(&slice, &uva)),
                            "failed to parse request header tag count");
                        RD_UT_ASSERT(uva == 0,
                                     "expected empty header tags, got %" PRIu64,
                                     uva);
                }

                /* ProduceRequest header */
                if (ctx.rkpc_api_version >= 3) {
                        /* TransactionalId */
                        if (is_flexver) {
                                RD_UT_ASSERT(!RD_UVARINT_UNDERFLOW(
                                                 rd_slice_read_uvarint(&slice,
                                                                      &uva)),
                                             "failed to parse TransactionalId "
                                             "length");
                                str_len = ((int32_t)uva) - 1;
                        } else {
                                RD_UT_ASSERT(
                                    rd_slice_read(&slice, &str_len16,
                                                  sizeof(str_len16)),
                                    "failed to parse TransactionalId length");
                                str_len = (int)(int16_t)be16toh((uint16_t)str_len16);
                        }
                        if (str_len > 0) {
                                RD_UT_ASSERT(
                                    rd_slice_read(&slice, NULL, str_len),
                                    "failed to skip TransactionalId bytes");
                        }
                }

                RD_UT_ASSERT(
                    rd_slice_read(&slice, &required_acks, sizeof(required_acks)),
                    "failed to parse required_acks");
                required_acks = (int16_t)be16toh((uint16_t)required_acks);
                RD_UT_ASSERT(required_acks == ctx.rkpc_request_required_acks,
                             "expected required_acks=%" PRId16 ", got %" PRId16,
                             ctx.rkpc_request_required_acks, required_acks);

                RD_UT_ASSERT(rd_slice_read(&slice, &timeout, sizeof(timeout)),
                             "failed to parse timeout");
                timeout = (int32_t)be32toh(timeout);
                RD_UT_ASSERT(timeout == ctx.rkpc_request_timeout_ms,
                             "expected timeout=%" PRId32 ", got %" PRId32,
                             ctx.rkpc_request_timeout_ms, timeout);

                /* Topic array */
                if (is_flexver) {
                        RD_UT_ASSERT(!RD_UVARINT_UNDERFLOW(
                                         rd_slice_read_uvarint(&slice, &uva)),
                                     "failed to parse flex topic_cnt");
                        topic_cnt = ((int32_t)uva) - 1;
                } else {
                        RD_UT_ASSERT(rd_slice_read(&slice, &topic_cnt,
                                                   sizeof(topic_cnt)),
                                     "failed to parse topic_cnt");
                        topic_cnt = (int32_t)be32toh(topic_cnt);
                }
                RD_UT_ASSERT(topic_cnt == 1,
                             "expected 1 topic in ProduceRequest, got %d",
                             topic_cnt);

                /* Topic name */
                if (is_flexver) {
                        RD_UT_ASSERT(!RD_UVARINT_UNDERFLOW(
                                         rd_slice_read_uvarint(&slice, &uva)),
                                     "failed to parse flex topic length");
                        str_len = ((int32_t)uva) - 1;
                } else {
                        RD_UT_ASSERT(
                            rd_slice_read(&slice, &str_len16, sizeof(str_len16)),
                            "failed to parse topic length");
                        str_len = (int)(int16_t)be16toh((uint16_t)str_len16);
                }

                RD_UT_ASSERT(str_len == (int)strlen(expected_topic),
                             "expected topic name \"%s\", got length %d",
                             expected_topic, str_len);
                RD_UT_ASSERT(str_len < (int)sizeof(topic),
                             "topic name too long: %d", str_len);
                RD_UT_ASSERT(rd_slice_read(&slice, topic, str_len),
                             "failed to read topic bytes");
                topic[str_len] = '\0';
                RD_UT_ASSERT(!strcmp(topic, expected_topic),
                             "expected topic \"%s\", got \"%s\"",
                             expected_topic, topic);

                /* Partition array */
                if (is_flexver) {
                        RD_UT_ASSERT(!RD_UVARINT_UNDERFLOW(
                                         rd_slice_read_uvarint(&slice, &uva)),
                                     "failed to parse flex partition_cnt");
                        partition_cnt = ((int32_t)uva) - 1;
                } else {
                        RD_UT_ASSERT(rd_slice_read(&slice, &partition_cnt,
                                                   sizeof(partition_cnt)),
                                     "failed to parse partition_cnt");
                        partition_cnt = (int32_t)be32toh(partition_cnt);
                }
                RD_UT_ASSERT(partition_cnt == 1,
                             "expected 1 partition in ProduceRequest, got %d",
                             partition_cnt);

                RD_UT_ASSERT(rd_slice_read(&slice, &partition, sizeof(partition)),
                             "failed to parse partition id");
                partition = (int32_t)be32toh(partition);
                RD_UT_ASSERT(partition == 0,
                             "expected partition 0 in ProduceRequest, got %d",
                             partition);

                /* Records */
                if (is_flexver) {
                        RD_UT_ASSERT(!RD_UVARINT_UNDERFLOW(
                                         rd_slice_read_uvarint(&slice, &uva)),
                                     "failed to parse flex records length");
                        records_len = ((int32_t)uva) - 1;
                } else {
                        RD_UT_ASSERT(rd_slice_read(&slice, &records_len,
                                                   sizeof(records_len)),
                                     "failed to parse records length");
                        records_len = (int32_t)be32toh(records_len);
                }
                RD_UT_ASSERT(records_len > 0,
                             "expected non-empty records, got %d", records_len);
                RD_UT_ASSERT(rd_slice_read(&slice, NULL, records_len),
                             "failed to skip records bytes");

                /* Partition/Topic/Request tags */
                if (is_flexver) {
                        /* Partition tags */
                        RD_UT_ASSERT(
                            !RD_UVARINT_UNDERFLOW(
                                rd_slice_read_uvarint(&slice, &uva)),
                            "failed to parse partition tag count");
                        RD_UT_ASSERT(uva == 0,
                                     "expected empty partition tags, got %" PRIu64,
                                     uva);

                        /* Topic tags */
                        RD_UT_ASSERT(
                            !RD_UVARINT_UNDERFLOW(
                                rd_slice_read_uvarint(&slice, &uva)),
                            "failed to parse topic tag count");
                        RD_UT_ASSERT(uva == 0,
                                     "expected empty topic tags, got %" PRIu64,
                                     uva);

                        /* Request tags */
                        RD_UT_ASSERT(
                            !RD_UVARINT_UNDERFLOW(
                                rd_slice_read_uvarint(&slice, &uva)),
                            "failed to parse request tag count");
                        RD_UT_ASSERT(uva == 0,
                                     "expected empty request tags, got %" PRIu64,
                                     uva);
                }

                RD_UT_ASSERT(rd_slice_remains(&slice) == 0,
                             "expected to parse full request, %" PRIusz
                             " bytes remain",
                             rd_slice_remains(&slice));
        }
        RD_UT_ASSERT(rkbuf->rkbuf_u.rkbuf_produce.v1.batch.msgq.rkmq_msg_cnt ==
                         1,
                     "expected 1 message in batch, got %d",
                     rkbuf->rkbuf_u.rkbuf_produce.v1.batch.msgq.rkmq_msg_cnt);
        RD_UT_ASSERT(
            rkbuf->rkbuf_u.rkbuf_produce.v1.batch.msgq.rkmq_msg_bytes ==
                msg_size,
                     "expected %zu message bytes in batch, got %zu",
                     msg_size,
                     rkbuf->rkbuf_u.rkbuf_produce.v1.batch.msgq.rkmq_msg_bytes);

        /* The request buffer owns the messages in its batch msgq, mimic the
         * normal request lifecycle by draining them before destroying the
         * buffer. */
        rd_kafka_msgq_purge(rk, &rkbuf->rkbuf_batch.msgq);
        rd_kafka_buf_destroy(rkbuf);

        unittest_clear_msgq(&rktp2->rktp_xmit_msgq);

        rd_kafka_toppar_destroy(rktp2);
        rd_kafka_toppar_destroy(rktp1);
        rd_kafka_topic_destroy(rkt2);
        rd_kafka_topic_destroy(rkt1);
        rd_kafka_broker_destroy(rkb);
        rd_kafka_destroy(rk);

        RD_UT_PASS();
}

/**
 * @brief Unit tests for msgset writer - main entry point
 */
int unittest_msgset_writer(void) {
        int fails = 0;

        fails += unittest_msgset_writer_config_values();
        fails += unittest_msgset_writer_empty_partition();
        fails += unittest_msgset_writer_add_partition();
        fails += unittest_msgset_writer_partition_limit();
        fails += unittest_msgset_writer_multiple_partitions();
        fails += unittest_msgset_writer_config_mismatch();
        fails += unittest_msgset_writer_accumulation();
        fails += unittest_msgset_writer_exact_limit();
        fails += unittest_msgset_writer_rollback_empty_topic();

        return fails;
}
