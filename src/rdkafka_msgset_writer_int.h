/*
 * librdkafka - Apache Kafka C library
 *
 * Internal msgset writer shared definitions.
 */

#pragma once

#include "rd.h"
#include "rdkafka_int.h"
#include "rdkafka_msg.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_proto.h"
#include "rdvarint.h"

/* KAFKA-3219 - max topic length is 249 chars */
#define TOPIC_LENGTH_MAX 249

/** @brief The maximum ProduceRequest ApiVersion supported by librdkafka */
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
                                           *   rkbuf_u.rkbuf_produce.*.batch */

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

/* Common writer helpers shared across v1/v2 implementations */
size_t rd_kafka_msgset_writer_write_msg_v0_1(rd_kafka_msgset_writer_t *msetw,
                                             rd_kafka_msg_t *rkm,
                                             int64_t Offset,
                                             int8_t MsgAttributes,
                                             void (*free_cb)(void *));
size_t rd_kafka_msgset_writer_write_msg_v2(rd_kafka_msgset_writer_t *msetw,
                                           rd_kafka_msg_t *rkm,
                                           int64_t Offset,
                                           int8_t MsgAttributes,
                                           void (*free_cb)(void *));
size_t rd_kafka_msgset_writer_write_msg(rd_kafka_msgset_writer_t *msetw,
                                        rd_kafka_msg_t *rkm,
                                        int64_t Offset,
                                        int8_t MsgAttributes,
                                        void (*free_cb)(void *));
int rd_kafka_msgset_writer_compress(rd_kafka_msgset_writer_t *msetw,
                                    size_t *outlenp);
void rd_kafka_msgset_writer_calc_crc_v2(rd_kafka_msgset_writer_t *msetw);
rd_kafka_buf_t *rd_kafka_msgset_writer_finalize_mbv1(
    rd_kafka_msgset_writer_t *msetw, size_t *MessageSetSizep);
