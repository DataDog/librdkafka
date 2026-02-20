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
#include "rdkafka_lz4.h"
#include "rdkafka_msgset_writer_int.h"

#if WITH_ZSTD
#include "rdkafka_zstd.h"
#endif

#include "snappy.h"
#include "rdvarint.h"
#include "crc32c.h"

/**
 * @brief Select produce request capabilities based on
 *       broker features.
 * @locality broker thread
 */
static RD_INLINE void
rd_kafka_produce_request_select_caps_mbv2(rd_kafka_broker_t *rkb,
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

/**
 * @brief Select message set features to use based on broker's
 *        feature compatibility and topic configuration.
 *
 * @returns -1 if a MsgVersion (or ApiVersion) could not be selected, else 0.
 * @locality broker thread
 */
static RD_INLINE int
rd_kafka_msgset_writer_select_caps_mbv2(rd_kafka_msgset_writer_t *msetw) {
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
rd_kafka_produce_request_get_header_sizes_mbv2(rd_kafka_t *rk,
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
static void rd_kafka_produce_request_alloc_buf_mbv2(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_t *rk      = rkpc->rkpc_rkb->rkb_rk;
        size_t msg_overhead = 0;

        size_t produce_hdr_size   = 0;
        size_t topic_hdr_size     = 0;
        size_t partition_hdr_size = 0;
        size_t msgset_hdr_size    = 0;
        size_t bufsize;

        rd_kafka_assert(NULL, !rkpc->rkpc_buf);

        rd_kafka_produce_request_get_header_sizes_mbv2(
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
        rkpc->rkpc_buf->rkbuf_u.rkbuf_produce_engine = RD_KAFKA_PRODUCE_MBV2;
        rd_kafka_msgbatch_t *batch =
            &rkpc->rkpc_buf->rkbuf_u.rkbuf_produce.mbv2.batch;
        memset(batch, 0, sizeof(*batch));
        rd_kafka_msgq_init(&batch->msgq);
        batch->pid       = rkpc->rkpc_pid;
        batch->first_seq = -1;
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

static void rd_kafka_msgset_writer_write_MessageSet_v2_header_mbv2(
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
rd_kafka_produce_write_produce_header_mbv2(rd_kafka_produce_ctx_t *rkpc) {

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
rd_kafka_produce_finalize_produce_header_mbv2(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;

        /* Update TopicArrayCnt (handles both regular and compact arrays) */
        rd_kafka_buf_finalize_arraycnt(rkbuf, rkpc->rkpc_topic_cnt_offset,
                                       rkpc->rkpc_appended_topic_cnt);

        /* Request-level tags (written after all topics) */
        rd_kafka_buf_write_tags_empty(rkbuf);
}

static void rd_kafka_produce_write_topic_header_mbv2(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;

        /* Insert topic */
        rd_kafka_buf_write_kstr(rkbuf, rkpc->rkpc_active_topic->rkt_topic);

        /* PartitionArrayCnt, update later */
        rkpc->rkpc_active_topic_partition_cnt_offset =
            rd_kafka_buf_write_i32(rkbuf, 0);

        // TODO(xvandish): Reset active stuff? Do we need to? Upstream doesn't
}

static void
rd_kafka_produce_finalize_topic_header_mbv2(rd_kafka_produce_ctx_t *rkpc) {
        rd_kafka_buf_t *rkbuf = rkpc->rkpc_buf;

        /* Update PartitionArrayCnt (handles both regular and compact arrays) */
        rd_kafka_buf_finalize_arraycnt(rkbuf,
                                       rkpc->rkpc_active_topic_partition_cnt_offset,
                                       rkpc->rkpc_active_topic_partition_cnt);

        /* Topic tags (written after all partitions for this topic) */
        rd_kafka_buf_write_tags_empty(rkbuf);
}

static RD_INLINE void
rd_kafka_produce_ctx_clear_active_topic(rd_kafka_produce_ctx_t *rkpc) {
        rkpc->rkpc_active_topic                      = NULL;
        rkpc->rkpc_active_topic_partition_cnt        = 0;
        rkpc->rkpc_active_topic_partition_cnt_offset = 0;
}

static RD_INLINE void
rd_kafka_produce_ctx_rollback_new_topic_header_mbv2(rd_kafka_produce_ctx_t *rkpc,
                                               size_t new_topic_header_offset) {
        rd_buf_write_seek(&rkpc->rkpc_buf->rkbuf_buf, new_topic_header_offset);
        if (rkpc->rkpc_appended_topic_cnt > 0)
                --rkpc->rkpc_appended_topic_cnt;
        rd_kafka_produce_ctx_clear_active_topic(rkpc);
}

/**
 * @brief Write ProduceRequest partition header.
 */
static void
rd_kafka_produce_write_partition_header_mbv2(rd_kafka_msgset_writer_t *msetw) {
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
                rd_kafka_msgset_writer_write_MessageSet_v2_header_mbv2(msetw);
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
static int rd_kafka_msgset_writer_init_mbv2(rd_kafka_msgset_writer_t *msetw,
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
        rd_kafka_msgset_writer_select_caps_mbv2(msetw);

        rd_rkb_dbg(rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: msetw_init before partition_header",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        /* Write the partition header */
        rd_kafka_produce_write_partition_header_mbv2(msetw);

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
        msetw->msetw_rkbuf->rkbuf_u.rkbuf_produce_engine = RD_KAFKA_PRODUCE_MBV2;
        msetw->msetw_batch =
            &msetw->msetw_rkbuf->rkbuf_u.rkbuf_produce.mbv2.batch;

        return 1;
}


/**
 * @brief Write as many messages from the given message queue to
 *        the messageset.
 *
 *        May not write any messages.
 *
 * @returns 1 on success or 0 on error.
 */
static int rd_kafka_msgset_writer_write_msgq_mbv2(rd_kafka_msgset_writer_t *msetw,
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

/**
 * @brief Finalize MessageSet v2 header fields.
 */
static void rd_kafka_msgset_writer_finalize_MessageSet_v2_header_mbv2(
    rd_kafka_msgset_writer_t *msetw) {
        rd_kafka_buf_t *rkbuf = msetw->msetw_rkbuf;
        int msgcnt =
            rd_kafka_msgq_len(&rkbuf->rkbuf_u.rkbuf_produce.mbv2.batch.msgq);

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
rd_kafka_msgset_writer_finalize_MessageSet_mbv2(rd_kafka_msgset_writer_t *msetw) {
        rd_dassert(msetw->msetw_messages_len > 0);

        if (msetw->msetw_MsgVersion == 2)
                rd_kafka_msgset_writer_finalize_MessageSet_v2_header_mbv2(msetw);
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
rd_kafka_msgset_writer_finalize_mbv2(rd_kafka_msgset_writer_t *msetw,
                                size_t *MessageSetSizep) {
        rd_kafka_buf_t *rkbuf   = msetw->msetw_rkbuf;
        rd_kafka_toppar_t *rktp = msetw->msetw_rktp;
        size_t len;
        int cnt;

        rd_rkb_dbg(msetw->msetw_rkb, MSG, "MSETW",
                   "%s [%" PRId32 "]: finalize entry",
                   rktp->rktp_rkt->rkt_topic->str, rktp->rktp_partition);

        /* No messages added, bail out early. */
        if (unlikely((cnt = rd_kafka_msgq_len(
                           &rkbuf->rkbuf_u.rkbuf_produce.mbv2.batch.msgq)) ==
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
        rd_kafka_msgset_writer_finalize_MessageSet_mbv2(msetw);

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
void rd_kafka_produce_calculator_init_mbv2(rd_kafka_produce_calculator_t *rkpca,
                                         rd_kafka_broker_t *rkb) {
        memset(rkpca, 0, sizeof(*rkpca));

        int api_version = 0;
        int msg_version = 0;
        int features    = 0;

        /* Retrieve the capabilities to be used to calculate hdr sizes */
        rd_kafka_produce_request_select_caps_mbv2(rkb, &api_version, &msg_version,
                                             &features);

        rd_kafka_produce_request_get_header_sizes_mbv2(
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
 *      rd_kafka_produce_calculator_init_mbv2() to pick Api/Msg versions and header
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
int rd_kafka_produce_ctx_init_mbv2(rd_kafka_produce_ctx_t *rkpc,
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
        rd_kafka_produce_request_select_caps_mbv2(
            rkpc->rkpc_rkb, &rkpc->rkpc_api_version, &rkpc->rkpc_msg_version,
            &rkpc->rkpc_features);

        /* Allocate backing buffer */
        rd_kafka_produce_request_alloc_buf_mbv2(rkpc);

        /* ProduceRequest uses FlexibleVersions starting from v9 */
        if (rkpc->rkpc_api_version >= 9) {
                rd_kafka_buf_upgrade_flexver_request(rkpc->rkpc_buf);
        }

        /* Construct first part of Produce header */
        rd_kafka_produce_write_produce_header_mbv2(rkpc);

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
int rd_kafka_produce_ctx_append_toppar_mbv2(rd_kafka_produce_ctx_t *rkpc,
                                       rd_kafka_toppar_t *rktp,
                                       int *appended_msg_cnt,
                                       size_t *appended_msg_bytes) {
        rd_kafka_msgq_t msgq;
        rd_kafka_msgq_init(&msgq);
        rd_bool_t wrote_new_topic_header = rd_false;
        size_t new_topic_header_offset  = 0;


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
                        rd_kafka_produce_finalize_topic_header_mbv2(rkpc);
                }

                wrote_new_topic_header =
                    rd_true; /* may need to roll back on 0-msg append */
                new_topic_header_offset =
                    rd_buf_write_pos(&rkpc->rkpc_buf->rkbuf_buf);

                /* Set up and write new topic header */
                rkpc->rkpc_active_topic               = rktp->rktp_rkt;
                rkpc->rkpc_active_topic_partition_cnt = 0;
                ++rkpc->rkpc_appended_topic_cnt;  // TODO(xvandish). Is
                                                  // pre-increment correct?
                rd_kafka_produce_write_topic_header_mbv2(rkpc);
        }

        /* move msg q before write */
        // rd_kafka_msgq_move(*msgq, &rkpc->rkpc_buf->rkbuf_ba);
        rd_kafka_msgq_move(&msgq,
                           &rkpc->rkpc_buf->rkbuf_u.rkbuf_produce.mbv2.batch.msgq);

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

        if (unlikely(!rd_kafka_msgset_writer_init_mbv2(
                &msetw, rkpc->rkpc_rkb, rktp, &rktp->rktp_xmit_msgq, rkpc->rkpc_pid,
                epoch_base_msgid,
                rkpc))) {
                if (wrote_new_topic_header &&
                    rkpc->rkpc_active_topic_partition_cnt == 0) {
                        rd_kafka_produce_ctx_rollback_new_topic_header_mbv2(
                            rkpc, new_topic_header_offset);
                }
                return 0;
        }

        rd_ts_t first_msg_timeout;
        rd_kafka_msg_t *first_msg = TAILQ_FIRST(&rktp->rktp_xmit_msgq.rkmq_msgs);
        if (unlikely(!first_msg)) {
                if (wrote_new_topic_header &&
                    rkpc->rkpc_active_topic_partition_cnt == 0) {
                        rd_kafka_produce_ctx_rollback_new_topic_header_mbv2(
                            rkpc, new_topic_header_offset);
                }
                return 0;
        }
        first_msg_timeout = first_msg->rkm_ts_timeout;

        if (!rd_kafka_msgset_writer_write_msgq_mbv2(&msetw, &rktp->rktp_xmit_msgq)) {
                /* Error while writing messages to MessageSet,
                 * move all messages back on the xmit queue. */
                rd_kafka_msgq_insert_msgq(
                    &rktp->rktp_xmit_msgq, &msetw.msetw_batch->msgq,
                    rktp->rktp_rkt->rkt_conf.msg_order_cmp);
        }

        int queue_msg_cnt_end      = rd_kafka_msgq_len(&rktp->rktp_xmit_msgq);
        size_t queue_msg_bytes_end = rd_kafka_msgq_size(&rktp->rktp_xmit_msgq);


        rd_kafka_msgset_writer_finalize_mbv2(&msetw, appended_msg_bytes);

        /* concatenate msg queues together */
        rd_kafka_msgq_concat(&msgq,
                             &rkpc->rkpc_buf->rkbuf_u.rkbuf_produce.mbv2.batch.msgq);
        rd_kafka_msgq_move(&rkpc->rkpc_buf->rkbuf_u.rkbuf_produce.mbv2.batch.msgq,
                           &msgq);

        /* If no messages were written, seek to the beginning of this write
         * so that the context can be finalized */
        if (queue_msg_cnt_start == queue_msg_cnt_end) {
                /* If we started a new topic but didn't write any messages
                 * (e.g., first message is in backoff), roll back the topic
                 * header to avoid leaving an empty topic in the request. */
                if (wrote_new_topic_header &&
                    rkpc->rkpc_active_topic_partition_cnt == 0) {
                        rd_kafka_produce_ctx_rollback_new_topic_header_mbv2(
                            rkpc, new_topic_header_offset);
                } else {
                        rd_buf_write_seek(&rkpc->rkpc_buf->rkbuf_buf,
                                          write_offset);
                }
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
        rd_kafka_toppar_producer_mbv2_t *tpv2 = rktp->rktp_producer_mbv2;

        if (tpv2 && tpv2->rktp_ts_xmit_enq) {
            rd_ts_t wait_us = rd_clock() - tpv2->rktp_ts_xmit_enq;
            if (wait_us >= 0 && rkpc->rkpc_rkb->rkb_producer_mbv2)
                rd_avg_add(
                    &rkpc->rkpc_rkb->rkb_producer_mbv2->rkbp_avg_batch_wait,
                    (int64_t)wait_us);
        }

        if (queue_msg_cnt_end == 0 && tpv2)
                tpv2->rktp_ts_xmit_enq = 0;

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
rd_kafka_buf_t *rd_kafka_produce_ctx_finalize_mbv2(rd_kafka_produce_ctx_t *rkpc) {
        /* if nothing was generated, free the buffer and return NULL */
        /* the topic header can be written, but if messages are in backoff
         * for all partitions, there might not be any messages written
         */
        if (unlikely(rkpc->rkpc_appended_topic_cnt == 0 ||
                     rkpc->rkpc_appended_message_cnt == 0)) {
                rd_kafka_buf_destroy(rkpc->rkpc_buf);
                return NULL;
        }

        if (rkpc->rkpc_active_topic)
                rd_kafka_produce_finalize_topic_header_mbv2(rkpc);
        rd_kafka_produce_finalize_produce_header_mbv2(rkpc);

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
rd_kafka_buf_t *rd_kafka_msgset_create_ProduceRequest_mbv2(rd_kafka_broker_t *rkb,
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
        if (!rd_kafka_produce_ctx_init_mbv2(
                &ctx, rkb, 1, 1, rd_kafka_msgq_len(rkmq),
                rd_kafka_msgq_size(rkmq),
                rktp->rktp_rkt->rkt_conf.required_acks,
                rktp->rktp_rkt->rkt_conf.request_timeout_ms, pid, NULL))
                return NULL;

        /* Legacy single-partition path: tie the batch to this toppar so the
         * response handler has a valid rktp for bookkeeping. */
        rd_kafka_msgbatch_init(&ctx.rkpc_buf->rkbuf_u.rkbuf_produce.mbv2.batch,
                               rktp, pid,
                               epoch_base_msgid);

        /* Move messages into the toppar xmit queue for append */
        rd_kafka_toppar_lock(rktp);
        rd_kafka_msgq_move(&rktp->rktp_xmit_msgq, rkmq);
        rd_kafka_toppar_unlock(rktp);

        if (!rd_kafka_produce_ctx_append_toppar_mbv2(
                &ctx, rktp, &appended_msg_cnt, &appended_msg_bytes)) {
                rd_kafka_msgq_move(rkmq, &rktp->rktp_xmit_msgq);
                return NULL;
        }

        rkbuf = rd_kafka_produce_ctx_finalize_mbv2(&ctx);

        /* Move any remaining messages back to caller's queue for the next
         * iteration. */
        rd_kafka_msgq_move(rkmq, &rktp->rktp_xmit_msgq);

        if (rkbuf && MessageSetSizep)
                *MessageSetSizep = rkbuf->rkbuf_totlen;

        return rkbuf;
}
