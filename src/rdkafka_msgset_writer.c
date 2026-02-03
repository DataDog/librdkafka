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

/* KAFKA-3219 - max topic length is 249 chars
 * this corresponds to the max file name length of 255
 * with some room for adding an underscore and digits for
 * log number
 */
#define TOPIC_LENGTH_MAX 249


/** @brief The maxium ProduceRequestion ApiVersion supported by librdkafka */
static const int16_t rd_kafka_ProduceRequest_max_version = 10;


typedef struct rd_kafka_msgset_writer_s {
        rd_kafka_buf_t *msetw_rkbuf; /* Backing store buffer (refcounted)*/

        int16_t msetw_ApiVersion; /* ProduceRequest ApiVersion */
        int msetw_MsgVersion;     /* MsgVersion to construct */
        int msetw_features;       /* Protocol features to use */
        rd_kafka_compression_t msetw_compression; /**< Compression type */
        int msetw_msgcntmax;         /* Max number of messages to send
                                      * in a batch. */
        size_t msetw_messages_len;   /* Total size of Messages, with Message
                                      * framing but without
                                      * MessageSet header */
        size_t msetw_messages_kvlen; /* Total size of Message keys
                                      * and values */

        size_t msetw_MessageSetSize;    /* Current MessageSetSize value */
        size_t msetw_of_MessageSetSize; /* offset of MessageSetSize */
        size_t msetw_of_start;          /* offset of MessageSet */

        int msetw_relative_offsets; /* Bool: use relative offsets */

        /* For MessageSet v2 */
        int msetw_Attributes;       /* MessageSet Attributes */
        int64_t msetw_MaxTimestamp; /* Maximum timestamp in batch */
        size_t msetw_of_CRC;        /* offset of MessageSet.CRC */

        rd_kafka_msgbatch_t *msetw_batch; /**< Convenience pointer to
                                           *   rkbuf_u.Produce.batch */

        /* First message information */
        struct {
                size_t of; /* rkbuf's first message position */
                int64_t timestamp;
                uint64_t msgid; /**< First message's msgid for sequence calculation */
        } msetw_firstmsg;

        rd_kafka_pid_t msetw_pid;      /**< Idempotent producer's
                                        *   current Producer Id */
        uint64_t msetw_epoch_base_msgid; /**< Partition's epoch base msgid for
                                          *   sequence number calculation */
        rd_kafka_broker_t *msetw_rkb;  /* @warning Not a refcounted
                                        *          reference! */
        rd_kafka_toppar_t *msetw_rktp; /* @warning Not a refcounted
                                        *          reference! */
        rd_kafka_msgq_t *msetw_msgq;   /**< Input message queue */
} rd_kafka_msgset_writer_t;

/**
 * @brief Select produce request capabilities based on
 *       broker features.
 * @locality broker thread
 */
static RD_INLINE void
rd_kafka_produce_request_select_caps(rd_kafka_broker_t *rkb,
                                     int *api_version,
                                     int *msg_version,
                                     int *features) {
        int feature;
        int16_t min_ApiVersion = 0;

        *api_version = 0;
        *msg_version = 0;
        *features    = 0;

        if ((feature = rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER2)) {
                min_ApiVersion = 3;
                *msg_version   = 2;
                *features |= feature;
        } else if ((feature = rkb->rkb_features & RD_KAFKA_FEATURE_MSGVER1)) {
                min_ApiVersion = 2;
                *msg_version   = 1;
                *features |= feature;
        } else {
                if ((feature =
                         rkb->rkb_features & RD_KAFKA_FEATURE_THROTTLETIME)) {
                        min_ApiVersion = 1;
                        *features |= feature;
                } else {
                        min_ApiVersion = 0;
                }
                *msg_version = 0;
        }

        *api_version = rd_kafka_broker_ApiVersion_supported(
            rkb, RD_KAFKAP_Produce, min_ApiVersion,
            rd_kafka_ProduceRequest_max_version, NULL);


        rd_assert(*api_version >= min_ApiVersion);
}

static RD_INLINE size_t
rd_kafka_arraycnt_wire_size(size_t cnt, rd_bool_t flexver) {
        if (!flexver)
            return sizeof(int32_t);
        char buf[RD_UVARINT_ENC_SIZEOF(uint64_t)];
        /* CompactArray count is encoded as cnt + 1 */
        return rd_uvarint_enc_u64(buf, sizeof(buf), (uint64_t)cnt + 1);
}

static RD_INLINE size_t
rd_kafka_arraycnt_wire_size_max(rd_bool_t flexver) {
        if (!flexver)
            return sizeof(int32_t);
        return RD_UVARINT_ENC_SIZEOF(int32_t);
}

static RD_INLINE size_t
rd_kafka_kstr_wire_size_max(size_t max_len, rd_bool_t flexver) {
        if (!flexver)
            return RD_KAFKAP_STR_SIZE0(max_len);
        char buf[RD_UVARINT_ENC_SIZEOF(uint64_t)];
        size_t len_sz =
            rd_uvarint_enc_u64(buf, sizeof(buf), (uint64_t)max_len + 1);
        return len_sz + max_len;
}

static RD_INLINE size_t
rd_kafka_kstr_wire_size(const rd_kafkap_str_t *kstr, rd_bool_t flexver) {
    if (!flexver)
        return RD_KAFKAP_STR_SIZE(kstr);
    if (!kstr || RD_KAFKAP_STR_IS_NULL(kstr))
        return RD_UVARINT_ENC_SIZE_0();
    return rd_kafka_kstr_wire_size_max(RD_KAFKAP_STR_LEN(kstr), rd_true);
}

static RD_INLINE size_t
rd_kafka_msgq_bytes_prefix(const rd_kafka_msgq_t *rkmq, int cnt) {
        int i = 0;
        size_t bytes = 0;
        const rd_kafka_msg_t *rkm;

        RD_KAFKA_MSGQ_FOREACH(rkm, rkmq) {
                if (i++ >= cnt)
                        break;
                bytes += rkm->rkm_len + rkm->rkm_key_len;
        }

        return bytes;
}



/**
 * @brief Select message set features to use based on broker's
 *        feature compatibility and topic configuration.
 *
 * @returns -1 if a MsgVersion (or ApiVersion) could not be selected, else 0.
 * @locality broker thread
 */
static RD_INLINE int
rd_kafka_msgset_writer_select_caps(rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_broker_t *rkb       = msetw->msetw_rkb;
        rd_kafka_toppar_t *rktp      = msetw->msetw_rktp;
        const int16_t max_ApiVersion = rd_kafka_ProduceRequest_max_version;
        int16_t min_ApiVersion       = 0;
        int feature;
        /* Map compression types to required feature and ApiVersion */
        static const struct {
                int feature;
                int16_t ApiVersion;
        } compr_req[RD_KAFKA_COMPRESSION_NUM] = {
            [RD_KAFKA_COMPRESSION_LZ4] = {RD_KAFKA_FEATURE_LZ4, 0},
#if WITH_ZSTD
            [RD_KAFKA_COMPRESSION_ZSTD] = {RD_KAFKA_FEATURE_ZSTD, 7},
#endif
        };



        msetw->msetw_compression = rktp->rktp_rkt->rkt_conf.compression_codec;

        /*
         * Check that the configured compression type is supported
         * by both client and broker, else disable compression.
         */
        if (msetw->msetw_compression &&
            (msetw->msetw_ApiVersion <
                 compr_req[msetw->msetw_compression].ApiVersion ||
             (compr_req[msetw->msetw_compression].feature &&
              !(rkb->rkb_features &
                compr_req[msetw->msetw_compression].feature)))) {
                if (unlikely(
                        rd_interval(&rkb->rkb_suppress.unsupported_compression,
                                    /* at most once per day */
                                    (rd_ts_t)86400 * 1000 * 1000, 0) > 0))
                        rd_rkb_log(
                            rkb, LOG_NOTICE, "COMPRESSION",
                            "%.*s [%" PRId32
                            "]: "
                            "Broker does not support compression "
                            "type %s: not compressing batch",
                            RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                            rktp->rktp_partition,
                            rd_kafka_compression2str(msetw->msetw_compression));
                else
                        rd_rkb_dbg(
                            rkb, MSG, "PRODUCE",
                            "%.*s [%" PRId32
                            "]: "
                            "Broker does not support compression "
                            "type %s: not compressing batch",
                            RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                            rktp->rktp_partition,
                            rd_kafka_compression2str(msetw->msetw_compression));

                msetw->msetw_compression = RD_KAFKA_COMPRESSION_NONE;
        } else {
                /* Broker supports this compression type. */
                msetw->msetw_features |=
                    compr_req[msetw->msetw_compression].feature;
        }

        /* MsgVersion specific setup. */
        switch (msetw->msetw_MsgVersion) {
        case 2:
                msetw->msetw_relative_offsets = 1; /* OffsetDelta */
                break;
        case 1:
                if (msetw->msetw_compression != RD_KAFKA_COMPRESSION_NONE)
                        msetw->msetw_relative_offsets = 1;
                break;
        }


        if (msetw->msetw_ApiVersion == -1) {
                rd_kafka_msg_t *rkm;
                /* This will only happen if the broker reports none, or
                 * no matching ProduceRequest versions, which should never
                 * happen. */
                rd_rkb_log(rkb, LOG_ERR, "PRODUCE",
                           "%.*s [%" PRId32
                           "]: "
                           "No viable ProduceRequest ApiVersions (v%d..%d) "
                           "supported by broker: unable to produce",
                           RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                           rktp->rktp_partition, min_ApiVersion,
                           max_ApiVersion);

                /* Back off and retry in 5s */
                rkm = rd_kafka_msgq_first(msetw->msetw_msgq);
                rd_assert(rkm);
                rkm->rkm_u.producer.ts_backoff = rd_clock() + (5 * 1000 * 1000);
                return -1;
        }

        /* It should not be possible to get a lower version than requested,
         * otherwise the logic in this function is buggy. */
        // rd_assert(msetw->msetw_ApiVersion >= min_ApiVersion);

        return 0;
}



static void
rd_kafka_produce_request_get_header_sizes(rd_kafka_t *rk,
                                          int api_version,
                                          int msg_version,
                                          size_t *produce_hdr_size,
                                          size_t *topic_hdr_size,
                                          size_t *partition_hdr_size,
                                          size_t *msgset_hdr_size,
                                          size_t *msg_overhead) {
        *produce_hdr_size   = 0;
        *topic_hdr_size     = 0; // TODO(xvandish). Remove? alloc_buf callsite no longer uses
        *partition_hdr_size = 0;
        *msgset_hdr_size    = 0;
        *msg_overhead       = 0;

        /* Calculate worst-case buffer size, produce header size,
         * message size, etc, this isn't critical but avoids unnecessary
         * extra allocations. The buffer will grow as needed if we get
         * this wrong.
         *
         * ProduceRequest headers go in one iovec:
         *  ProduceRequest v0..2:
         *    RequiredAcks + Timeout +
         *    [Topic + [Partition + MessageSetSize + MessageSet]]
         *
         *  ProduceRequest v3:
         *    TransactionalId + RequiredAcks + Timeout +
         *    [Topic + [Partition + MessageSetSize + MessageSet]]
         */

        /*
         * ProduceRequest header sizes
         */
        switch (api_version) {
        case 10:
        case 9:
        case 8:
        case 7:
        case 6:
        case 5:
        case 4:
        case 3:
                /* Add TransactionalId */
                *produce_hdr_size += RD_KAFKAP_STR_SIZE(rk->rk_eos.transactional_id);
                /* FALLTHRU */
        case 0:
        case 1:
        case 2:
                *produce_hdr_size +=
                    /* RequiredAcks + Timeout + TopicCnt */
                    2 + 4 + 4;
                /* Topic name + PartitionArrayCnt are sized elsewhere. */
                *partition_hdr_size +=
                    /* Partition (MessageSetSize length handled separately) */
                    4;
                break;

        default:
        }

        /*
         * MsgVersion specific sizes:
         * - (Worst-case) Message overhead: message fields
         * - MessageSet header size
         */
        switch (msg_version) {
        case 0:
                /* MsgVer0 */
                *msg_overhead = RD_KAFKAP_MESSAGE_V0_OVERHEAD;
                break;
        case 1:
                /* MsgVer1 */
                *msg_overhead = RD_KAFKAP_MESSAGE_V1_OVERHEAD;
                break;

        case 2:
                /* MsgVer2 uses varints, we calculate for the worst-case. */
                *msg_overhead += RD_KAFKAP_MESSAGE_V2_MAX_OVERHEAD;

                /* MessageSet header fields */
                *msgset_hdr_size +=
                    8 /* BaseOffset */ + 4 /* Length */ +
                    4 /* PartitionLeaderEpoch */ + 1 /* Magic (MsgVersion) */ +
                    4 /* CRC (CRC32C) */ + 2 /* Attributes */ +
                    4 /* LastOffsetDelta */ + 8 /* BaseTimestamp */ +
                    8 /* MaxTimestamp */ + 8 /* ProducerId */ +
                    2 /* ProducerEpoch */ + 4 /* BaseSequence */ +
                    4 /* RecordCount */;
                break;

        default:
        }
}


/**
 * @brief Allocate buffer for produce request based on a previously set up
 * rkpc
 *
 * Allocate iovecs to hold all the headers and messages,
 * and allocate enough space to allow copies of small messages.
 * The allocated size is the minimum of message.max.bytes
 * or queued_bytes ormsgcntmax * msg_overhead
 */
static void rd_kafka_produce_request_alloc_buf(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_t *rk      = rkpc->rkpc_rkb->rkb_rk;
        size_t msg_overhead = 0;

        size_t produce_hdr_size   = 0;
        size_t topic_hdr_size     = 0;
        size_t partition_hdr_size = 0;
        size_t msgset_hdr_size    = 0;
        size_t bufsize;

        rd_kafka_assert(NULL, !rkpc->rkpc_buf);

        rd_kafka_produce_request_get_header_sizes(
            rkpc->rkpc_rkb->rkb_rk, rkpc->rkpc_api_version,
            rkpc->rkpc_msg_version, &produce_hdr_size, &topic_hdr_size,
            &partition_hdr_size, &msgset_hdr_size, &msg_overhead);
        rd_bool_t flexver = (rkpc->rkpc_api_version >= 9);
        topic_hdr_size = rd_kafka_kstr_wire_size_max(TOPIC_LENGTH_MAX, flexver) +
                         rd_kafka_arraycnt_wire_size_max(flexver);
        size_t msgset_size_len = rd_kafka_arraycnt_wire_size_max(flexver);

        /*
         * Calculate total buffer size to allocate
         */
        bufsize = produce_hdr_size +
                    (topic_hdr_size * rkpc->rkpc_topic_max) +
                    ((partition_hdr_size + msgset_hdr_size + msgset_size_len) *
                     rkpc->rkpc_partition_max);

        /* If copying for small payloads is enabled, allocate enough
         * space for each message to be copied based on this limit.
         */
        if (rk->rk_conf.msg_copy_max_size > 0) {
                bufsize += RD_MIN(rkpc->rkpc_message_bytes_size,
                                  (size_t)rk->rk_conf.msg_copy_max_size *
                                      rkpc->rkpc_message_max);
        }

        /* Add estimed per-message overhead */
        bufsize += msg_overhead * rkpc->rkpc_message_max;

        /* Cap allocation at message.max.bytes */
        if (bufsize > (size_t)rk->rk_conf.max_msg_size)
                bufsize = (size_t)rk->rk_conf.max_msg_size;

        /*
         * Allocate iovecs to hold all headers and messages,
         * and allocate auxilliery space for message headers, etc.
         */
        rkpc->rkpc_buf =
            rd_kafka_buf_new_request(rkpc->rkpc_rkb, RD_KAFKAP_Produce,
                                     rkpc->rkpc_message_max / 2 + 10, bufsize);

        /*  Set ApiVersion and features on the buffer
         * The features will be updated during produce context finalizing
         * to add additional required features such as compression modes.
         */
        rd_kafka_buf_ApiVersion_set(rkpc->rkpc_buf, rkpc->rkpc_api_version,
                                    rkpc->rkpc_features);

        /* Initialize the msgbatch structure.
         * Note: In multi-partition requests, this batch structure is shared across
         * all partitions in the request. The PID must be set for msgbatch_set_first_msg()
         * to work, but per-partition metadata (sequences, etc.) is tracked separately
         * in the hash map (rd_kafka_produce_req_toppar_t structures).
         * The epoch_base_msgid will be set per-partition when calling msgbatch_set_first_msg(). */
        memset(&rkpc->rkpc_buf->rkbuf_batch, 0, sizeof(rkpc->rkpc_buf->rkbuf_batch));
        rd_kafka_msgq_init(&rkpc->rkpc_buf->rkbuf_batch.msgq);
        rkpc->rkpc_buf->rkbuf_batch.pid = rkpc->rkpc_pid;
        rkpc->rkpc_buf->rkbuf_batch.first_seq = -1;
}

/**
 * @brief Write the MessageSet header.
 * @remark Must only be called for MsgVersion 2
 */
/**
 * @brief Packed MessageSet v2 header struct for efficient single-write.
 *
 * This struct must match the Kafka wire format exactly:
 * - BaseOffset: 8 bytes
 * - Length: 4 bytes
 * - PartitionLeaderEpoch: 4 bytes
 * - Magic: 1 byte
 * - CRC: 4 bytes
 * - Attributes: 2 bytes
 * - LastOffsetDelta: 4 bytes
 * - BaseTimestamp: 8 bytes
 * - MaxTimestamp: 8 bytes
 * - ProducerId: 8 bytes
 * - ProducerEpoch: 2 bytes
 * - BaseSequence: 4 bytes
 * - RecordCount: 4 bytes
 * Total: 61 bytes (RD_KAFKAP_MSGSET_V2_SIZE)
 */
typedef struct __attribute__((packed)) rd_msgset_v2_hdr_s {
        int64_t base_offset;            /* Will be updated later */
        int32_t length;                 /* Will be updated later */
        int32_t partition_leader_epoch;
        int8_t  magic;
        int32_t crc;                    /* Will be updated later */
        int16_t attributes;             /* Will be updated later */
        int32_t last_offset_delta;      /* Will be updated later */
        int64_t base_timestamp;         /* Will be updated later */
        int64_t max_timestamp;          /* Will be updated later */
        int64_t producer_id;
        int16_t producer_epoch;
        int32_t base_sequence;          /* Will be updated later */
        int32_t record_count;           /* Will be updated later */
} rd_msgset_v2_hdr_t;

/* Compile-time assert to ensure struct size matches wire format */
_Static_assert(sizeof(rd_msgset_v2_hdr_t) == RD_KAFKAP_MSGSET_V2_SIZE,
               "MessageSet v2 header struct size mismatch");

static void rd_kafka_msgset_writer_write_MessageSet_v2_header(
    rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;
        rd_msgset_v2_hdr_t hdr;

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: v2_header entry buf_len=%" PRIusz,
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   rd_buf_len(&rkbuf->rkbuf_buf));

        rd_kafka_assert(NULL, msetw->msetw_ApiVersion >= 3);
        rd_kafka_assert(NULL, msetw->msetw_MsgVersion == 2);

        /* Initialize header with default values.
         * Fields that need updating later are set to 0 or -1 as appropriate. */
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic          = msetw->msetw_MsgVersion;
        hdr.producer_id    = htobe64(msetw->msetw_pid.id);
        hdr.producer_epoch = htobe16(msetw->msetw_pid.epoch);
        hdr.base_sequence  = htobe32(-1);  /* Updated later for idempotent */

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: v2_header before buf_write (size=%zu)",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   sizeof(hdr));

        /* Write entire header in a single operation (replaces 13 separate writes).
         * Store the start offset for later field updates. */
        msetw->msetw_of_start = rd_kafka_buf_write(rkbuf, &hdr, sizeof(hdr));

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: v2_header after buf_write of_start=%" PRIusz,
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   msetw->msetw_of_start);

        /* Calculate CRC offset for later update */
        msetw->msetw_of_CRC = msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_CRC;

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: v2_header exit of_CRC=%" PRIusz,
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   msetw->msetw_of_CRC);
}


/**
 * @brief Write ProduceRequest headers.
 *        When this function returns the msgset is ready for
 *        writing individual messages.
 *        msetw_MessageSetSize will have been set to the messageset header.
 */
static void
rd_kafka_produce_write_produce_header(rd_kafka_produce_ctx_t *rkpc) {

        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;
        rd_kafka_t *rk        = rkpc->rkpc_rkb->rkb_rk;
        // rd_kafka_topic_t *rkt = msetw->msetw_rktp->rktp_rkt;

        /* V3: TransactionalId */
        if (rkpc->rkpc_api_version >= 3)
                rd_kafka_buf_write_kstr(rkbuf, rk->rk_eos.transactional_id);

        /* RequiredAcks */
        rd_kafka_buf_write_i16(rkbuf, rkpc->rkpc_request_required_acks);

        /* Timeout */
        rd_kafka_buf_write_i32(rkbuf, rkpc->rkpc_request_timeout_ms);

        /* TopicArrayCnt, update later */
        rkpc->rkpc_topic_cnt_offset = rd_kafka_buf_write_i32(rkbuf, 0);
}

static void
rd_kafka_produce_finalize_produce_header(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;

                rkpc->rkpc_appended_topic_cnt,

        /* Update TopicArrayCnt (handles both regular and compact arrays) */
        rd_kafka_buf_finalize_arraycnt(rkbuf, rkpc->rkpc_topic_cnt_offset,
                                       rkpc->rkpc_appended_topic_cnt);

        /* Request-level tags (written after all topics) */
        rd_kafka_buf_write_tags_empty(rkbuf);
}

static void rd_kafka_produce_write_topic_header(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;

        /* Insert topic */
        rd_kafka_buf_write_kstr(rkbuf, rkpc->rkpc_active_topic->rkt_topic);

        /* PartitionArrayCnt, update later */
        rkpc->rkpc_active_topic_partition_cnt_offset =
            rd_kafka_buf_write_i32(rkbuf, 0);

        // TODO(xvandish): Reset active stuff? Do we need to? Upstream doesn't
}

static void
rd_kafka_produce_finalize_topic_header(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;

        /* Update PartitionArrayCnt (handles both regular and compact arrays) */
        rd_kafka_buf_finalize_arraycnt(rkbuf,
                                       rkpc->rkpc_active_topic_partition_cnt_offset,
                                       rkpc->rkpc_appended_partition_cnt);

        /* Topic tags (written after all partitions for this topic) */
        rd_kafka_buf_write_tags_empty(rkbuf);
}

/**
 * @brief Write ProduceRequest partition header.
 */
static void
rd_kafka_produce_write_partition_header(rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: partition_header entry",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        /* Partition */
        rd_kafka_buf_write_i32(rkbuf, msetw->msetw_rktp->rktp_partition);

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: partition_header after partition_id write",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        /* MessageSetSize: Will be finalized later*/
        msetw->msetw_of_MessageSetSize = rd_kafka_buf_write_arraycnt_pos(rkbuf);

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: partition_header after msgsetsize placeholder",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        if (msetw->msetw_MsgVersion == 2) {
                rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                           "%s [%" PRId32 "]: partition_header before v2_header",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);
                /* MessageSet v2 header */
                rd_kafka_msgset_writer_write_MessageSet_v2_header(msetw);
                rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                           "%s [%" PRId32 "]: partition_header after v2_header",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);
                msetw->msetw_MessageSetSize = RD_KAFKAP_MSGSET_V2_SIZE;
        } else {
                /* Older MessageSet */
                msetw->msetw_MessageSetSize = RD_KAFKAP_MSGSET_V0_SIZE;
        }

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: partition_header exit",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);
}



/**
 * @brief Initialize a ProduceRequest MessageSet writer for
 *        the given broker and partition.
 *
 *        A new buffer will be allocated to fit the pending messages in queue.
 *
 * @returns 1 on success and 0 on failure
 *
 * @locality broker thread
 */
static int rd_kafka_msgset_writer_init(rd_kafka_msgset_writer_t *msetw,
                                       rd_kafka_broker_t *rkb,
                                       rd_kafka_toppar_t *rktp,
                                       rd_kafka_msgq_t *rkmq,
                                       rd_kafka_pid_t pid,
                                       uint64_t epoch_base_msgid,
                                       rd_kafka_produce_ctx_t *rkpc) {
        int msgcnt = rd_kafka_msgq_len(
            rkmq);  // TODO(xvandish): is this the right thing to use?

        rd_rkb_dbg(rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: msetw_init entry msgcnt=%d",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition, msgcnt);

        if (msgcnt == 0)
                return 0;

        memset(msetw, 0, sizeof(*msetw));

        msetw->msetw_rktp             = rktp;
        msetw->msetw_rkb              = rkpc->rkpc_rkb;
        msetw->msetw_ApiVersion       = rkpc->rkpc_api_version;
        msetw->msetw_MsgVersion       = rkpc->rkpc_msg_version;
        msetw->msetw_features         = rkpc->rkpc_features;
        msetw->msetw_rkbuf            = rkpc->rkpc_buf;
        msetw->msetw_msgq             = rkmq;  // TODO(xvandish): What should we use here
        msetw->msetw_pid              = rkpc->rkpc_pid;
        msetw->msetw_epoch_base_msgid = epoch_base_msgid;

        /* Max number of messages to send in a batch,
         * limited by current queue size or configured batch size,
         * whichever is lower. */
        msetw->msetw_msgcntmax = RD_MIN(
            msgcnt, msetw->msetw_rkb->rkb_rk->rk_conf.batch_num_messages);
        rd_dassert(msetw->msetw_msgcntmax > 0);

        /* Select topic level message set configuration to use */
        rd_kafka_msgset_writer_select_caps(msetw);

        rd_rkb_dbg(rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: msetw_init before partition_header",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        /* Write the partition header */
        rd_kafka_produce_write_partition_header(msetw);

        rd_rkb_dbg(rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: msetw_init after partition_header",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);


        /* The current buffer position is now where the first message
         * is located.
         * Record the current buffer position so it can be rewound later
         * in case of compression. */
        msetw->msetw_firstmsg.of =
            rd_buf_write_pos(&msetw->msetw_rkbuf->rkbuf_buf);

        /* NOTE: msgbatch is NOT initialized per-partition in multi-partition requests.
         * The msgbatch structure is designed for single-partition batches only.
         * For multi-partition requests, per-partition metadata is tracked in the
         * hash map (rd_kafka_produce_req_toppar_t structures). */
        msetw->msetw_batch = &msetw->msetw_rkbuf->rkbuf_u.Produce.batch;

        return 1;
}



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
static size_t
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
static size_t
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
static size_t rd_kafka_msgset_writer_write_msg(rd_kafka_msgset_writer_t *msetw,
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

/**
 * @brief Write as many messages from the given message queue to
 *        the messageset.
 *
 *        May not write any messages.
 *
 * @returns 1 on success or 0 on error.
 */
static int rd_kafka_msgset_writer_write_msgq(rd_kafka_msgset_writer_t *msetw,
                                             rd_kafka_msgq_t *rkmq) {
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;
        rd_kafka_broker_t *rkb  = msetw->msetw_rkb;
        size_t len              = rd_buf_len(&msetw->msetw_rkbuf->rkbuf_buf);
        size_t max_msg_size =
            RD_MIN((size_t)msetw->msetw_rkb->rkb_rk->rk_conf.max_msg_size,
                   (size_t)msetw->msetw_rkb->rkb_rk->rk_conf.batch_size);
        rd_ts_t int_latency_base;
        rd_ts_t MaxTimestamp = 0;
        rd_kafka_msg_t *rkm;
        int msgcnt        = 0;
        const rd_ts_t now = rd_clock();

        /* Internal latency calculation base.
         * Uses rkm_ts_timeout which is enqueue time + timeout */
        int_latency_base =
            now + ((rd_ts_t)rktp->rktp_rkt->rkt_conf.message_timeout_ms * 1000);

        /* Acquire information from first message for this partition's MessageSet. */
        rkm = TAILQ_FIRST(&rkmq->rkmq_msgs);
        rd_kafka_assert(NULL, rkm);
        msetw->msetw_firstmsg.timestamp = rkm->rkm_timestamp;
        msetw->msetw_firstmsg.msgid = rkm->rkm_u.producer.msgid;

        /* For multi-partition requests, msgbatch_set_first_msg() should only
         * be called once for the entire request, not once per partition,
         * as the batch structure is shared across all partitions.
         * We set epoch_base_msgid from the first partition processed.
         * Note: Per-partition sequence tracking is handled in rd_kafka_produce_req_toppar_t. */
        if (msetw->msetw_batch->first_msgid == 0) {
                msetw->msetw_batch->epoch_base_msgid = msetw->msetw_epoch_base_msgid;
                rd_kafka_msgbatch_set_first_msg(msetw->msetw_batch, rkm);
        }

        rd_rkb_dbg(rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: write_msgq entry: qlen=%d msgcntmax=%d",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   rd_kafka_msgq_len(rkmq), msetw->msetw_msgcntmax);

        /*
         * Write as many messages as possible until buffer is full
         * or limit reached.
         */
        do {
                if (unlikely(msetw->msetw_batch->last_msgid &&
                             msetw->msetw_batch->last_msgid <
                                 rkm->rkm_u.producer.msgid)) {
                        rd_rkb_dbg(rkb, MSG, "PRODUCE",
                                   "%.*s [%" PRId32
                                   "]: "
                                   "Reconstructed MessageSet "
                                   "(%d message(s), %" PRIusz
                                   " bytes, "
                                   "MsgIds %" PRIu64 "..%" PRIu64 ")",
                                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                   rktp->rktp_partition, msgcnt, len,
                                   msetw->msetw_batch->first_msgid,
                                   msetw->msetw_batch->last_msgid);
                        break;
                }

                /* Check if there is enough space in the current messageset
                 * to add this message.
                 * Since calculating the total size of a request at produce()
                 * time is tricky (we don't know the protocol version or
                 * MsgVersion that will be used), we allow a messageset to
                 * overshoot the message.max.bytes limit by one message to
                 * avoid getting stuck here.
                 * The actual messageset size is enforced by the broker. */
                if (unlikely(
                        msgcnt == msetw->msetw_msgcntmax ||
                        (msgcnt > 0 && len + rd_kafka_msg_wire_size(
                                                 rkm, msetw->msetw_MsgVersion) >
                                           max_msg_size))) {
                        rd_rkb_dbg(rkb, MSG, "PRODUCE",
                                   "%.*s [%" PRId32
                                   "]: "
                                   "No more space in current MessageSet "
                                   "(%i message(s), %" PRIusz " bytes)",
                                   RD_KAFKAP_STR_PR(rktp->rktp_rkt->rkt_topic),
                                   rktp->rktp_partition, msgcnt, len);
                        break;
                }

                if (unlikely(rkm->rkm_u.producer.ts_backoff > now)) {
                        /* Stop accumulation when we've reached
                         * a message with a retry backoff in the future */
                        break;
                }

                /* Move message to buffer's queue */
                rd_kafka_msgq_deq(rkmq, rkm, 1);
                rd_kafka_msgq_enq(&msetw->msetw_batch->msgq, rkm);

                msetw->msetw_messages_kvlen += rkm->rkm_len + rkm->rkm_key_len;

                /* Add internal latency metrics */
                rd_avg_add(&rkb->rkb_avg_int_latency,
                           int_latency_base - rkm->rkm_ts_timeout);

                /* Feed int_latency to adaptive batching system */
                if (rkb->rkb_rk->rk_conf.adaptive_batching_enabled)
                        rd_kafka_adaptive_record_int_latency(
                            rkb, int_latency_base - rkm->rkm_ts_timeout);

                /* MessageSet v2's .MaxTimestamp field */
                if (unlikely(MaxTimestamp < rkm->rkm_timestamp))
                        MaxTimestamp = rkm->rkm_timestamp;

                /* Write message to buffer */
                rd_rkb_dbg(rkb, MSG, "MSETW",
                           "%s [%" PRId32 "]: write_msg iter=%d len=%" PRIusz,
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                           msgcnt, len);
                len += rd_kafka_msgset_writer_write_msg(msetw, rkm, msgcnt, 0,
                                                        NULL);

                msgcnt++;

        } while ((rkm = TAILQ_FIRST(&rkmq->rkmq_msgs)));

        rd_rkb_dbg(rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: write_msgq exit: msgcnt=%d qlen=%d",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                   msgcnt, rd_kafka_msgq_len(rkmq));

        msetw->msetw_MaxTimestamp = MaxTimestamp;

        /* Idempotent Producer:
         * When reconstructing a batch to retry make sure
         * the original message sequence span matches identically
         * or we can't guarantee exactly-once delivery.
         * If this check fails we raise a fatal error since
         * it is unrecoverable and most likely caused by a bug
         * in the client implementation.
         * This should not be considered an abortable error for
         * the transactional producer. */
        if (msgcnt > 0 && msetw->msetw_batch->last_msgid) {
                rd_kafka_msg_t *lastmsg;

                lastmsg = rd_kafka_msgq_last(&msetw->msetw_batch->msgq);
                rd_assert(lastmsg);

                if (unlikely(lastmsg->rkm_u.producer.msgid !=
                             msetw->msetw_batch->last_msgid)) {
                        rd_kafka_set_fatal_error(
                            rkb->rkb_rk, RD_KAFKA_RESP_ERR__INCONSISTENT,
                            "Unable to reconstruct MessageSet "
                            "(currently with %d message(s)) "
                            "with msgid range %" PRIu64 "..%" PRIu64
                            ": "
                            "last message added has msgid %" PRIu64
                            ": "
                            "unable to guarantee consistency",
                            msgcnt, msetw->msetw_batch->first_msgid,
                            msetw->msetw_batch->last_msgid,
                            lastmsg->rkm_u.producer.msgid);
                        return 0;
                }
        }
        return 1;
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

/**
 * @brief Finalize MessageSet v2 header fields.
 */
static void rd_kafka_msgset_writer_finalize_MessageSet_v2_header(
    rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        int msgcnt            = rd_kafka_msgq_len(&rkbuf->rkbuf_batch.msgq);

        rd_kafka_assert(NULL, msgcnt > 0);
        rd_kafka_assert(NULL, msetw->msetw_ApiVersion >= 3);

        msetw->msetw_MessageSetSize =
            RD_KAFKAP_MSGSET_V2_SIZE + msetw->msetw_messages_len;

        /* MessageSet.Length is the same as
         * MessageSetSize minus field widths for FirstOffset+Length */
        rd_kafka_buf_update_i32(
            rkbuf, msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_Length,
            (int32_t)msetw->msetw_MessageSetSize - (8 + 4));

        msetw->msetw_Attributes |= RD_KAFKA_MSG_ATTR_CREATE_TIME;

        if (rd_kafka_is_transactional(msetw->msetw_rkb->rkb_rk))
                msetw->msetw_Attributes |=
                    RD_KAFKA_MSGSET_V2_ATTR_TRANSACTIONAL;

        rd_kafka_buf_update_i16(
            rkbuf, msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_Attributes,
            msetw->msetw_Attributes);

        rd_kafka_buf_update_i32(rkbuf,
                                msetw->msetw_of_start +
                                    RD_KAFKAP_MSGSET_V2_OF_LastOffsetDelta,
                                msgcnt - 1);

        rd_kafka_buf_update_i64(
            rkbuf, msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_BaseTimestamp,
            msetw->msetw_firstmsg.timestamp);

        rd_kafka_buf_update_i64(
            rkbuf, msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_MaxTimestamp,
            msetw->msetw_MaxTimestamp);

        /* Calculate BaseSequence for this partition's MessageSet.
         * In multibatch requests, each partition needs its own BaseSequence
         * calculated from the partition's epoch_base_msgid and first message msgid.
         * Fall back to batch.first_seq if msgid wasn't set (shouldn't happen in normal flow). */
        int32_t base_seq = -1;
        if (rd_kafka_pid_valid(msetw->msetw_pid)) {
                if (msetw->msetw_firstmsg.msgid != 0) {
                        /* Calculate per-partition BaseSequence */
                        base_seq = rd_kafka_seq_wrap(msetw->msetw_firstmsg.msgid -
                                                     msetw->msetw_epoch_base_msgid);
                } else {
                        /* Fallback: use shared batch first_seq */
                        base_seq = msetw->msetw_batch->first_seq;
                }
        }

        rd_kafka_buf_update_i32(
            rkbuf, msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_BaseSequence,
            base_seq);

        rd_kafka_buf_update_i32(
            rkbuf, msetw->msetw_of_start + RD_KAFKAP_MSGSET_V2_OF_RecordCount,
            msgcnt);

        rd_kafka_msgset_writer_calc_crc_v2(msetw);
}



/**
 * @brief Finalize the MessageSet header, if applicable.
 */
static void
rd_kafka_msgset_writer_finalize_MessageSet(rd_kafka_msgset_writer_t *msetw) {
        rd_dassert(msetw->msetw_messages_len > 0);

        if (msetw->msetw_MsgVersion == 2)
                rd_kafka_msgset_writer_finalize_MessageSet_v2_header(msetw);
        else
                msetw->msetw_MessageSetSize =
                    RD_KAFKAP_MSGSET_V0_SIZE + msetw->msetw_messages_len;

        /* Update MessageSetSize */
        rd_kafka_buf_finalize_arraycnt(msetw->msetw_rkbuf,
                                       msetw->msetw_of_MessageSetSize,
                                       (int32_t)msetw->msetw_MessageSetSize);
}


/**
 * @brief Finalize the messageset - call when no more messages are to be
 *        added to the messageset.
 *
 *        Will compress, update final values, CRCs, etc.
 *
 *        The messageset writer is destroyed and the buffer is returned
 *        and ready to be transmitted.
 *
 * @param MessagetSetSizep will be set to the finalized MessageSetSize
 *
 * @returns the buffer to transmit or NULL if there were no messages
 *          in messageset.
 */
static rd_kafka_buf_t *
rd_kafka_msgset_writer_finalize(rd_kafka_msgset_writer_t *msetw,
                                size_t *MessageSetSizep) {
        rd_kafka_buf_t *rkbuf   = msetw->msetw_rkbuf;
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;
        size_t len;
        int cnt;

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: finalize entry",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        /* No messages added, bail out early. */
        if (unlikely((cnt = rd_kafka_msgq_len(&rkbuf->rkbuf_batch.msgq)) ==
                     0)) {
                /* NOTE: In multi-partition requests, we don't destroy the buffer here
                 * because it's shared across partitions. The caller will handle cleanup. */
                return NULL;
        }


        /* Total size of messages */
        len = rd_buf_write_pos(&msetw->msetw_rkbuf->rkbuf_buf) -
              msetw->msetw_firstmsg.of;
        rd_assert(len > 0);
        rd_assert(len <= (size_t)rktp->rktp_rkt->rkt_rk->rk_conf.max_msg_size);

        rd_atomic64_add(&rktp->rktp_c.tx_msgs, cnt);
        rd_atomic64_add(&rktp->rktp_c.tx_msg_bytes,
                        msetw->msetw_messages_kvlen);

        /* Compress the message set */
        if (msetw->msetw_compression) {
                rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                           "%s [%" PRId32 "]: compressing (len=%" PRIusz ")",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition, len);
                if (rd_kafka_msgset_writer_compress(msetw, &len) == -1)
                        msetw->msetw_compression = 0;
                rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                           "%s [%" PRId32 "]: compress done (len=%" PRIusz ")",
                           rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition, len);
        }

        msetw->msetw_messages_len = len;

        /* Finalize MessageSet header fields */
        rd_kafka_msgset_writer_finalize_MessageSet(msetw);

        /* Partition tags */
        rd_kafka_buf_write_tags_empty(rkbuf);
        /* Topic tags are written in produce_ctx_finalize_topic(), not here */

        /* Return final MessageSetSize */
        *MessageSetSizep = msetw->msetw_MessageSetSize;

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "PRODUCE",
                   "%s [%" PRId32
                   "]: "
                   "Produce MessageSet with %i message(s) (%" PRIusz
                   " bytes, "
                   "ApiVersion %d, MsgVersion %d, MsgId %" PRIu64
                   ", "
                   "BaseSeq %" PRId32 ", %s, %s)",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition, cnt,
                   msetw->msetw_MessageSetSize, msetw->msetw_ApiVersion,
                   msetw->msetw_MsgVersion, msetw->msetw_batch->first_msgid,
                   msetw->msetw_batch->first_seq,
                   rd_kafka_pid2str(msetw->msetw_pid),
                   msetw->msetw_compression
                       ? rd_kafka_compression2str(msetw->msetw_compression)
                       : "uncompressed");

        rd_kafka_msgq_verify_order(rktp, &msetw->msetw_batch->msgq,
                                   msetw->msetw_batch->first_msgid, rd_false);

        /* Update per-topic batch metrics for producer stats. */
        rd_avg_add(&rktp->rktp_rkt->rkt_avg_batchcnt, cnt);
        rd_avg_add(&rktp->rktp_rkt->rkt_avg_batchsize,
                   (int64_t)msetw->msetw_messages_len);

        /* NOTE: In multi-partition requests, we do NOT call msgbatch_ready_produce()
         * because the batch->rktp is not set (the batch is shared across partitions).
         * In-flight tracking is handled per-partition in the request context
         * (rd_kafka_ProduceRequest_finalize). For single-partition requests, the
         * batch has a valid rktp and this would be called. */
        if (msetw->msetw_batch->rktp) {
                rd_kafka_msgbatch_ready_produce(msetw->msetw_batch);
        } else {
        }

        return rkbuf;
}

/* Initialize a produce batch calculator to determine upfront limits
 * for a produce batch */
void rd_kafka_produce_calculator_init(rd_kafka_produce_calculator_t *rkpca,
                                      rd_kafka_broker_t *rkb) {
        memset(rkpca, 0, sizeof(*rkpca));

        int api_version = 0;
        int msg_version = 0;
        int features    = 0;

        /* Retrieve the capabilities to be used to calculate hdr sizes */
        rd_kafka_produce_request_select_caps(rkb, &api_version, &msg_version,
                                             &features);

        rd_kafka_produce_request_get_header_sizes(
            rkb->rkb_rk, api_version, msg_version,
            &rkpca->rkpca_produce_header_size, &rkpca->rkpca_topic_header_size,
            &rkpca->rkpca_partition_header_size,
            &rkpca->rkpca_message_set_header_size,
            &rkpca->rkpca_message_overhead);
        rkpca->rkpca_flexver = (api_version >= 9);
}

/* Attempt to add a partition into the running size/count calculation for a
 * multi-partition Produce request.
 *
 * Callers:
 *   - broker batching (`rd_kafka_broker_produce_batch_append`) to decide if the
 *     current toppar fits before actually writing it into the request.
 *   - unit tests that exercise limits (partition cap, batch_num_messages,
 *     message.max.bytes) without serializing a real request.
 *
 * Expectations (idealised flow):
 *   1) Calculator is seeded once per candidate batch via
 *      rd_kafka_produce_calculator_init() to pick Api/Msg versions and header
 *      sizes.
 *   2) Call rd_kafka_produce_calculator_add() for each toppar in tentative
 *      order; if it returns 1 the caller may enqueue that toppar in the batch,
 *      if it returns 0 the caller must not include it and should flush/send the
 *      current batch (then start a new one).
 *
 * The result encodes whether at least one batch of messages from this partition
 * would fit given the running totals and config constraints. */
int rd_kafka_produce_calculator_add(rd_kafka_produce_calculator_t *rkpca,
                                    rd_kafka_toppar_t *rktp) {
        rd_kafka_t *rk = rktp->rktp_rkt->rkt_rk;
        int batch_cnt;
        int topic_changed;
        size_t calculated_size;
        size_t batch_msg_size;
        size_t batch_msg_cnt;
        size_t topic_name_size = 0;
        size_t partition_cnt_size_delta = 0;
        size_t prev_partcnt_size = 0;
        size_t msgset_size_len = 0;
        size_t msgset_payload_size = 0;
        int prev_partcnt = 0;
        int batch_index;
        int added_batch_cnt;


        // TODO(xvandish): Think about removing this. Why would anyone want to make
        // their requests worse?
        if (rkpca->rkpca_partition_cnt >=
            rktp->rktp_rkt->rkt_rk->rk_conf.produce_request_max_partitions) {
                return 0;
        }

        // Initialize topic level fields if first add attempt
        if (rkpca->rkpca_topic_cnt == 0) {
                rkpca->rkpca_request_required_acks =
                    rktp->rktp_rkt->rkt_conf.required_acks;
                rkpca->rkpca_request_timeout_ms =
                    rktp->rktp_rkt->rkt_conf.request_timeout_ms;
        } else if (rkpca->rkpca_request_required_acks !=
                       rktp->rktp_rkt->rkt_conf.required_acks ||
                   rkpca->rkpca_request_timeout_ms !=
                       rktp->rktp_rkt->rkt_conf.request_timeout_ms) {
                /* Can't add messages from topics that don't match current
                 * batches settings
                 */
                // TODO(xvandish): Should we return something here different than 0 to indicate
                // that this will never be possibly? Would this cause a spin-loop?
                return 0;
        }

        batch_cnt = (rd_kafka_msgq_len(&rktp->rktp_xmit_msgq) +
                     rk->rk_conf.batch_num_messages - 1) /
                    rk->rk_conf.batch_num_messages;

        if (batch_cnt == 0)
                return 0;

        topic_changed = rkpca->rkpca_rkt_prev != rktp->rktp_rkt;

        /* Produce header */
        calculated_size = rkpca->rkpca_produce_header_size;

        /* Topic name sizes + PartitionArrayCnt field sizes */
        calculated_size += rkpca->rkpca_topic_name_size;
        calculated_size += rkpca->rkpca_partition_cnt_size;
        calculated_size += rkpca->rkpca_message_set_size_len;
        if (topic_changed) {
            topic_name_size = rd_kafka_kstr_wire_size(
                    rktp->rktp_rkt->rkt_topic, rkpca->rkpca_flexver);
            calculated_size += topic_name_size;
        }

        /* Partition headers */
        calculated_size +=
            rkpca->rkpca_partition_header_size * rkpca->rkpca_partition_cnt;

        /* Message set headers */
        calculated_size +=
            rkpca->rkpca_message_set_header_size * rkpca->rkpca_partition_cnt;

        /* Message overhead */
        calculated_size +=
            rkpca->rkpca_message_overhead * rkpca->rkpca_message_cnt;

        /* Messages */
        calculated_size += rkpca->rkpca_message_size;

        // TODO(xvandish): This would be a good spot to add tolerance
        if (calculated_size > rk->rk_conf.max_msg_size)
                return 0;

        /* If one batch of messages fits, the return success.
         * Calculate for the entire set of batches afterwards. */
        int xmit_msgq_len = rd_kafka_msgq_len(&rktp->rktp_xmit_msgq);
        added_batch_cnt = 0;
        batch_msg_size  = 0;
        batch_msg_cnt   = 0;

        for (batch_index = 0; batch_index < batch_cnt; batch_index++) {
                int pass_msg_cnt = RD_MIN((xmit_msgq_len - batch_msg_cnt),
                                          rk->rk_conf.batch_num_messages);
                prev_partcnt = topic_changed ? 0 : rkpca->rkpca_active_topic_partition_cnt;
                prev_partcnt_size = topic_changed ? 0 : rkpca->rkpca_active_topic_partition_cnt_size;
                partition_cnt_size_delta = rd_kafka_arraycnt_wire_size(prev_partcnt + 1, rkpca->rkpca_flexver) - prev_partcnt_size;

                size_t pass_partition_header =
                    rkpca->rkpca_partition_header_size;
                size_t pass_msg_set_header =
                    rkpca->rkpca_message_set_header_size;
                size_t pass_msg_overhead =
                    rkpca->rkpca_message_overhead * pass_msg_cnt;
                size_t pass_msg_size = rd_kafka_msgq_bytes_prefix(
                    &rktp->rktp_xmit_msgq, pass_msg_cnt);
                msgset_payload_size =
                    pass_msg_set_header + pass_msg_overhead + pass_msg_size;
                msgset_size_len =
                    rd_kafka_arraycnt_wire_size(msgset_payload_size,
                                                rkpca->rkpca_flexver);

                /* Don't factor in individual messages into the
                 * calculation so that partial batches can be written */
                size_t pass_total = pass_partition_header + msgset_size_len +
                                    pass_msg_set_header + partition_cnt_size_delta;

                if (calculated_size + pass_total > rk->rk_conf.max_msg_size)
                        break;

                pass_total += pass_msg_overhead + pass_msg_size;

                batch_msg_cnt += pass_msg_cnt;
                batch_msg_size += pass_msg_size;
                calculated_size += pass_total;
                ++added_batch_cnt;

                /* For now, only one message-set at a time is allowed.
                 * This is a limitation of being able to look up the correct
                 * message data in rd_kafka_handle_Producer_parse */
                /* TODO(xvandish): This is a big improvement opportunity */
                if (rk->rk_conf.debug & (RD_KAFKA_DBG_MSG | RD_KAFKA_DBG_QUEUE)) {
                        int msgs_left_behind =
                            xmit_msgq_len - (int)batch_msg_cnt;
                        size_t remaining_capacity =
                            rk->rk_conf.max_msg_size - calculated_size;
                        rd_kafka_dbg(
                            rk, MSG, "MBATCH",
                            "Calculator added toppar %s[%" PRId32
                            "]: msgs_added=%d msgs_left=%d "
                            "est_size=%zu remaining_capacity=%zu (%.1f%% fill)",
                            rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition,
                            (int)batch_msg_cnt, msgs_left_behind,
                            calculated_size, remaining_capacity,
                            (calculated_size * 100.0) /
                                rk->rk_conf.max_msg_size);
                }
                break;
        }

        if (added_batch_cnt == 0) {
                return 0;
        }

        rkpca->rkpca_topic_cnt += topic_changed;
        rkpca->rkpca_partition_cnt += added_batch_cnt;
        rkpca->rkpca_message_cnt += batch_msg_cnt;
        rkpca->rkpca_message_size += batch_msg_size;
        if (topic_changed) {
            rkpca->rkpca_topic_name_size += topic_name_size;
        }
        rkpca->rkpca_partition_cnt_size += partition_cnt_size_delta;
        rkpca->rkpca_message_set_size_len += msgset_size_len;
        rkpca->rkpca_active_topic_partition_cnt = prev_partcnt +1;
        rkpca->rkpca_active_topic_partition_cnt_size =
            rd_kafka_arraycnt_wire_size(rkpca->rkpca_active_topic_partition_cnt,
                                        rkpca->rkpca_flexver);
        rkpca->rkpca_rkt_prev = rktp->rktp_rkt;
        return 1;
}



/**
 * @breif Initialize a produce context and begin filling
 *              in the produce request header.
 *
 * @param rkb broker to create context for
 * @param rkpc produce context to append to
 *
 * @locality broker thread
 */
int rd_kafka_produce_ctx_init(rd_kafka_produce_ctx_t *rkpc,
                              rd_kafka_broker_t *rkb,
                              int topic_max,
                              int partition_max,
                              int message_max,
                              size_t message_bytes_size,
                              int required_acks,
                              int request_timeout_ms,
                              rd_kafka_pid_t pid,
                              void *opaque) {
        memset(rkpc, 0, sizeof(*rkpc));

        if (unlikely(topic_max <= 0 || partition_max <= 0 || message_max <= 0))
                return 0;

        rkpc->rkpc_rkb                   = rkb;
        rkpc->rkpc_topic_max             = topic_max;
        rkpc->rkpc_partition_max         = partition_max;
        rkpc->rkpc_request_required_acks = required_acks;
        rkpc->rkpc_request_timeout_ms    = request_timeout_ms;
        rkpc->rkpc_opaque                = opaque;

        /* Idempotent Producer:
         * Store request's PID for matching on response
         * if the instance PID has changed and thus made
         * the request obsolete */
        rkpc->rkpc_pid = pid;

        rkpc->rkpc_first_msg_timeout = INT64_MAX;

        /* Max number of messages to send in a produce request,
         * limited by message_max or configured batch size x partition count,
         * whichever is lower */
        rkpc->rkpc_message_max =
            RD_MIN(message_max, rkb->rkb_rk->rk_conf.batch_num_messages);

        rkpc->rkpc_message_bytes_size = message_bytes_size;

        /* Retruitve the capabilities to be used for the produce request */
        rd_kafka_produce_request_select_caps(
            rkpc->rkpc_rkb, &rkpc->rkpc_api_version, &rkpc->rkpc_msg_version,
            &rkpc->rkpc_features);

        /* Allocate backing buffer */
        rd_kafka_produce_request_alloc_buf(rkpc);

        /* ProduceRequest uses FlexibleVersions starting from v9 */
        if (rkpc->rkpc_api_version >= 9) {
                rd_kafka_buf_upgrade_flexver_request(rkpc->rkpc_buf);
        }

        /* Construct first part of Produce header */
        rd_kafka_produce_write_produce_header(rkpc);

        return 1;
}

/**
 * @brief Append a message set for the passed in toppar to an
 *        existing produce context.
 *
 * @param rkpc produce context to append to
 * @param rktp partition to be appended
 *
 * @returns 1 on success and 0 on error
 *
 * @locality broker thread
 */
int rd_kafka_produce_ctx_append_toppar(rd_kafka_produce_ctx_t *rkpc,
                                       rd_kafka_toppar_t *rktp,
                                       int *appended_msg_cnt,
                                       size_t *appended_msg_bytes) {
        rd_kafka_msgq_t msgq;
        rd_kafka_msgq_init(&msgq);


        /* early out if there are no messages to append */
        int queue_msg_cnt_start      = rd_kafka_msgq_len(&rktp->rktp_xmit_msgq);
        size_t queue_msg_bytes_start = rd_kafka_msgq_size(&rktp->rktp_xmit_msgq);
        if (unlikely(queue_msg_cnt_start == 0)) {
                return 0;
        }

        /* Check if this is a new topic */
        if (rktp->rktp_rkt != rkpc->rkpc_active_topic) {
                /* If there's an active topic, finalize it */
                if (rkpc->rkpc_active_topic) {
                        rd_kafka_produce_finalize_topic_header(rkpc);
                }

                /* Set up and write new topic header */
                rkpc->rkpc_active_topic               = rktp->rktp_rkt;
                rkpc->rkpc_active_topic_partition_cnt = 0;
                ++rkpc->rkpc_appended_topic_cnt;  // TODO(xvandish). Is
                                                  // pre-increment correct?
                rd_kafka_produce_write_topic_header(rkpc);
        }

        /* move msg q before write */
        // rd_kafka_msgq_move(*msgq, &rkpc->rkpc_buf->rkbuf_ba);
        rd_kafka_msgq_move(&msgq, &rkpc->rkpc_buf->rkbuf_batch.msgq);

        /* Store the size of the message queue and the current offset of
         * write buffer. If no data is written to the message set, then
         * don't write the header.
         */
        size_t write_offset = rd_buf_write_pos(&rkpc->rkpc_buf->rkbuf_buf);

        /* produce the message set */
        rd_kafka_msgset_writer_t msetw;
        uint64_t epoch_base_msgid;

        /* Get the partition's epoch base msgid for sequence number calculation */
        rd_kafka_toppar_lock(rktp);
        epoch_base_msgid = rktp->rktp_eos.epoch_base_msgid;
        rd_kafka_toppar_unlock(rktp);

        rd_rkb_dbg(rkpc->rkpc_rkb, MSG, "PRODUCE",
                   "Using epoch_base_msgid %" PRIu64 " for %s [%"PRId32"]",
                   epoch_base_msgid,
                   rktp->rktp_rkt->rkt_topic->str,
                   rktp->rktp_partition);

        if (unlikely(!rd_kafka_msgset_writer_init(
                &msetw, rkpc->rkpc_rkb, rktp, &rktp->rktp_xmit_msgq, rkpc->rkpc_pid,
                epoch_base_msgid,
                rkpc))) {
                return 0;
        }

        rd_ts_t first_msg_timeout;
        rd_kafka_msg_t *first_msg = TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs);
        if (unlikely(!first_msg)) {
                return 0;
        }
        first_msg_timeout = first_msg->rkm_ts_timeout;

        if (!rd_kafka_msgset_writer_write_msgq(&msetw, &rktp->rktp_xmit_msgq)) {
                /* Error while writing messages to MessageSet,
                 * move all messages back on the xmit queue. */
                rd_kafka_msgq_insert_msgq(
                    &rktp->rktp_xmit_msgq, &msetw.msetw_batch->msgq,
                    rktp->rktp_rkt->rkt_conf.msg_order_cmp);
        }

        int queue_msg_cnt_end      = rd_kafka_msgq_len(&rktp->rktp_xmit_msgq);
        size_t queue_msg_bytes_end = rd_kafka_msgq_size(&rktp->rktp_xmit_msgq);


        rd_kafka_msgset_writer_finalize(&msetw, appended_msg_bytes);

        /* concatenate msg queues together */
        rd_kafka_msgq_concat(&msgq, &rkpc->rkpc_buf->rkbuf_batch.msgq);
        rd_kafka_msgq_move(&rkpc->rkpc_buf->rkbuf_batch.msgq, &msgq);

        /* If no messages were written, seek to the beginning of this write
         * so that the context can be finalized */
        if (queue_msg_cnt_start == queue_msg_cnt_end) {
                rd_buf_write_seek(&rkpc->rkpc_buf->rkbuf_buf, write_offset);
                return 0;
        }


        /* update first timeout if needed */
        rkpc->rkpc_first_msg_timeout =
            RD_MIN(rkpc->rkpc_first_msg_timeout, first_msg_timeout);

        /* If the entire queue was not written, then this context is full */
        int queue_message_cnt_appended =
            queue_msg_cnt_start - queue_msg_cnt_end;
        if (queue_msg_cnt_start != queue_message_cnt_appended) {
                rkpc->rkpc_full = 1;

                rd_rkb_dbg(rkpc->rkpc_rkb, MSG, "MBATCH",
                           "Partition %s[%"PRId32"] only partially "
                           "written to request (%d/%d msgs appended); "
                           "will need a new request for remaining messages",
                           rktp->rktp_rkt->rkt_topic->str,
                           rktp->rktp_partition,
                           queue_message_cnt_appended,
                           queue_msg_cnt_start);
        }

        /* Add additional required features */
        rkpc->rkpc_features |= msetw.msetw_features;

        /* Store partition book-keeping data */
        rkpc->rkpc_active_firstmsg.msgid = msetw.msetw_batch->first_msgid;
        rkpc->rkpc_active_firstmsg.seq   = msetw.msetw_batch->first_seq;

        ++rkpc->rkpc_active_topic_partition_cnt;
        ++rkpc->rkpc_appended_partition_cnt;
        rkpc->rkpc_appended_message_cnt += queue_message_cnt_appended;
        rkpc->rkpc_appended_message_bytes +=
            queue_msg_bytes_start - queue_msg_bytes_end;
        *appended_msg_cnt = queue_message_cnt_appended;

        /* Batch wait time: time spent in the broker's per-partition queue
         * before being packaged into a (multi)Produce request. */
        if (rktp->rktp_ts_xmit_enq) {
                rd_ts_t wait_us = rd_clock() - rktp->rktp_ts_xmit_enq;
                if (wait_us >= 0)
                        rd_avg_add(&rkpc->rkpc_rkb->rkb_avg_batch_wait,
                                   (int64_t)wait_us);
        }

        if (queue_msg_cnt_end == 0)
                rktp->rktp_ts_xmit_enq = 0;

        return 1;
}

/**
 * @brief For the appended message sets go back and finalize
 *              the produce request header, and return the generated buffer.
 *              No further message sets may be added once a context has
 *              been finalized.
 *
 * @param rkpc produce context to finalize
 *
 * @returns the buffer to transmit or NULL if there was nothing to transmit
 *
 * @locality broker thread
 */
rd_kafka_buf_t *rd_kafka_produce_ctx_finalize(rd_kafka_produce_ctx_t *rkpc) {
        /* if nothing was generated, free the buffer and return NULL */
        if (unlikely(rkpc->rkpc_appended_topic_cnt == 0)) {
                rd_kafka_buf_destroy(rkpc->rkpc_buf);
                return NULL;
        }

        rd_kafka_produce_finalize_topic_header(rkpc);
        rd_kafka_produce_finalize_produce_header(rkpc);

        /* Update features since they may have had more added due to
         * topic level configuration
         */
        rkpc->rkpc_buf->rkbuf_features = rkpc->rkpc_features;

        rd_rkb_dbg(
            rkpc->rkpc_rkb, MSG, "PRODUCE",
            "Produce request with %i topic(s) %i partition(s) %i message(s) "
            "%llu message byte(s)"
            " (ApiVersion %d, MsgVersion %d)",
            rkpc->rkpc_appended_topic_cnt, rkpc->rkpc_appended_partition_cnt,
            rkpc->rkpc_appended_message_cnt, rkpc->rkpc_appended_message_bytes,
            rkpc->rkpc_api_version, rkpc->rkpc_msg_version);

        return rkpc->rkpc_buf;
}

/**
 * @brief Legacy single-partition API for unit tests.
 *        This is a compatibility wrapper around the new multi-partition API.
 *
 * @returns buffer on success, or NULL on error.
 */
rd_kafka_buf_t *rd_kafka_msgset_create_ProduceRequest(rd_kafka_broker_t *rkb,
                                                      rd_kafka_toppar_t *rktp,
                                                      rd_kafka_msgq_t *rkmq,
                                                      const rd_kafka_pid_t pid,
                                                      uint64_t epoch_base_msgid,
                                                     size_t *MessageSetSizep) {
        rd_kafka_produce_ctx_t ctx;
        rd_kafka_buf_t *rkbuf = NULL;
        int appended_msg_cnt  = 0;
        size_t appended_msg_bytes = 0;

        /* Use the standard ProduceRequest helpers so we get a proper req_ctx
         * with per-toppar bookkeeping for idempotent/error handling. */
        if (!rd_kafka_produce_ctx_init(
                &ctx, rkb, 1, 1, rd_kafka_msgq_len(rkmq),
                rd_kafka_msgq_size(rkmq),
                rktp->rktp_rkt->rkt_conf.required_acks,
                rktp->rktp_rkt->rkt_conf.request_timeout_ms, pid, NULL))
                return NULL;

        /* Legacy single-partition path: tie the batch to this toppar so the
         * response handler has a valid rktp for bookkeeping. */
        rd_kafka_msgbatch_init(&ctx.rkpc_buf->rkbuf_batch, rktp, pid,
                               epoch_base_msgid);

        /* Move messages into the toppar xmit queue for append */
        rd_kafka_toppar_lock(rktp);
        rd_kafka_msgq_move(&rktp->rktp_xmit_msgq, rkmq);
        rd_kafka_toppar_unlock(rktp);

        if (!rd_kafka_produce_ctx_append_toppar(&ctx, rktp, &appended_msg_cnt,
                                                &appended_msg_bytes)) {
                rd_kafka_msgq_move(rkmq, &rktp->rktp_xmit_msgq);
                return NULL;
        }

        rkbuf = rd_kafka_produce_ctx_finalize(&ctx);

        /* Move any remaining messages back to caller's queue for the next
         * iteration. */
        rd_kafka_msgq_move(rkmq, &rktp->rktp_xmit_msgq);

        if (rkbuf && MessageSetSizep)
                *MessageSetSizep = rkbuf->rkbuf_totlen;

        return rkbuf;
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

        return fails;
}
