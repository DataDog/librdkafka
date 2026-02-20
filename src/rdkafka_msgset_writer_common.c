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
static int rd_kafka_msgset_writer_compress(rd_kafka_msgset_writer_t *msetw,
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
static void
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
