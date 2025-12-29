/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2024 Datadog, Inc.
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

#ifndef _RDKAFKA_STATS_H_
#define _RDKAFKA_STATS_H_

#include <stdint.h>

/**
 * @file rdkafka_stats.h
 * @brief Typed statistics structures for efficient stats consumption.
 *
 * These structures replace the JSON-based statistics callback with
 * typed structs that can be directly consumed by C/C++ code and
 * efficiently converted to Rust structs via FFI.
 */

/** Maximum length for name fields (broker names, topic names, etc.) */
#define RD_KAFKA_STATS_NAME_MAX 256

/** Maximum number of request types (matches RD_KAFKAP__NUM) */
#define RD_KAFKA_STATS_REQ_TYPES_MAX 64

/**
 * @brief Rolling window statistics (histogram/average).
 *
 * Matches the JSON "window" objects for int_latency, rtt, throttle, etc.
 * These are sampled estimates maintained by an HDR histogram internally.
 */
typedef struct rd_kafka_avg_stats_s {
        int64_t min;      /**< Minimum value in window */
        int64_t max;      /**< Maximum value in window */
        int64_t avg;      /**< Average value */
        int64_t sum;      /**< Sum of all values */
        int64_t cnt;      /**< Number of samples */
        int64_t stddev;   /**< Standard deviation */
        int64_t p50;      /**< 50th percentile */
        int64_t p75;      /**< 75th percentile */
        int64_t p90;      /**< 90th percentile */
        int64_t p95;      /**< 95th percentile */
        int64_t p99;      /**< 99th percentile */
        int64_t p99_99;   /**< 99.99th percentile */
        int64_t oor;      /**< Out of range count */
        int32_t hdrsize;  /**< HDR histogram memory size */
        int32_t _pad;     /**< Alignment padding */
} rd_kafka_avg_stats_t;

/**
 * @brief Per-partition statistics.
 */
typedef struct rd_kafka_partition_stats_s {
        int32_t partition;   /**< Partition ID */
        int32_t broker_id;   /**< Current broker handling this partition */
        int32_t leader;      /**< Leader broker ID */

        int8_t desired;  /**< Partition is explicitly desired */
        int8_t unknown;  /**< Partition not in topic metadata */
        int8_t _pad[2];

        /* Producer queue stats */
        int32_t msgq_cnt;        /**< Messages in first-level queue */
        int64_t msgq_bytes;      /**< Bytes in first-level queue */
        int32_t xmit_msgq_cnt;   /**< Messages ready in transmit queue */
        int64_t xmit_msgq_bytes; /**< Bytes ready in transmit queue */

        /* Consumer stats */
        int32_t fetchq_cnt;   /**< Prefetched messages in fetch queue */
        int64_t fetchq_size;  /**< Bytes in fetch queue */
        int32_t fetch_state;  /**< Consumer fetch state (rd_kafka_fetch_state_t) */

        /* Offsets */
        int64_t query_offset;          /**< Current/last logical offset query */
        int64_t next_offset;           /**< Next offset to fetch */
        int64_t app_offset;            /**< Last message passed to app + 1 */
        int64_t stored_offset;         /**< Offset to be committed */
        int32_t stored_leader_epoch;   /**< Leader epoch for stored offset */
        int64_t committed_offset;      /**< Last committed offset */
        int32_t committed_leader_epoch; /**< Leader epoch for committed offset */
        int64_t eof_offset;            /**< Last EOF signaled offset */
        int64_t lo_offset;             /**< Low watermark offset */
        int64_t hi_offset;             /**< High watermark offset */
        int64_t ls_offset;             /**< Last stable offset */
        int64_t consumer_lag;          /**< hi_offset - committed_offset */
        int64_t consumer_lag_stored;   /**< hi_offset - stored_offset */
        int32_t leader_epoch;          /**< Current leader epoch */
        int32_t _pad2;

        /* Counters */
        int64_t txmsgs;        /**< Total messages transmitted */
        int64_t txbytes;       /**< Total bytes transmitted */
        int64_t rxmsgs;        /**< Total messages consumed */
        int64_t rxbytes;       /**< Total bytes consumed */
        int64_t msgs;          /**< Total messages (produced or consumed) */
        int64_t rx_ver_drops;  /**< Dropped outdated messages */
        int64_t msgs_inflight; /**< Messages in-flight to/from broker */
        int64_t next_ack_seq;  /**< Next expected acked sequence (idempotent) */
        int64_t next_err_seq;  /**< Next expected errored sequence (idempotent) */
        int64_t acked_msgid;   /**< Last acked internal message ID */
} rd_kafka_partition_stats_t;

/**
 * @brief Per-topic statistics.
 */
typedef struct rd_kafka_topic_stats_s {
        char name[RD_KAFKA_STATS_NAME_MAX]; /**< Topic name */
        int64_t age_us;                     /**< Age since creation (microseconds) */
        int64_t metadata_age_us;            /**< Age of metadata (microseconds) */

        rd_kafka_avg_stats_t batchsize; /**< Bytes per batch */
        rd_kafka_avg_stats_t batchcnt;  /**< Messages per batch */

        uint32_t partition_cnt;                  /**< Number of partitions */
        uint32_t _pad;
        rd_kafka_partition_stats_t *partitions;  /**< Array of partition stats */
} rd_kafka_topic_stats_t;

/**
 * @brief Broker toppar reference (minimal info for toppars on a broker).
 */
typedef struct rd_kafka_broker_toppar_ref_s {
        char topic[RD_KAFKA_STATS_NAME_MAX]; /**< Topic name */
        int32_t partition;                   /**< Partition ID */
        int32_t _pad;
} rd_kafka_broker_toppar_ref_t;

/**
 * @brief Request type count with pre-populated name.
 */
typedef struct rd_kafka_req_count_s {
        char name[32];   /**< API key name (e.g., "Produce", "Fetch") */
        int64_t count;   /**< Number of requests of this type */
} rd_kafka_req_count_t;

/**
 * @brief Per-broker statistics.
 */
typedef struct rd_kafka_broker_stats_s {
        char name[RD_KAFKA_STATS_NAME_MAX];     /**< Broker name (host:port/id) */
        int32_t nodeid;                         /**< Broker ID (-1 for bootstrap) */
        char nodename[RD_KAFKA_STATS_NAME_MAX]; /**< Broker hostname:port */
        char source[32];                        /**< Source: configured, learned, etc. */
        int32_t state;                          /**< Broker state (rd_kafka_broker_state_t) */
        int64_t stateage_us;                    /**< Time since last state change */

        /* Buffer counts */
        int32_t outbuf_cnt;     /**< Requests awaiting transmission */
        int32_t outbuf_msg_cnt; /**< Messages awaiting transmission */
        int32_t waitresp_cnt;   /**< Requests in-flight awaiting response */
        int32_t waitresp_msg_cnt; /**< Messages in-flight awaiting response */

        /* Counters */
        int64_t tx;           /**< Total requests sent */
        int64_t tx_bytes;     /**< Total bytes sent */
        int64_t tx_errs;      /**< Total transmission errors */
        int64_t tx_retries;   /**< Total request retries */
        int64_t tx_idle_us;   /**< Microseconds since last send */
        int64_t req_timeouts; /**< Total request timeouts */
        int64_t rx;           /**< Total responses received */
        int64_t rx_bytes;     /**< Total bytes received */
        int64_t rx_errs;      /**< Total receive errors */
        int64_t rx_idle_us;   /**< Microseconds since last receive */
        int64_t rx_corriderrs; /**< Unmatched correlation IDs */
        int64_t rx_partial;   /**< Partial message sets received */
        int64_t zbuf_grow;    /**< Decompression buffer grows */
        int64_t buf_grow;     /**< Buffer size increases (deprecated) */
        int64_t wakeups;      /**< Broker thread poll wakeups */
        int64_t connects;     /**< Connection attempts */
        int64_t disconnects;  /**< Disconnections */

        /* Latency averages */
        rd_kafka_avg_stats_t int_latency;    /**< Internal producer queue latency */
        rd_kafka_avg_stats_t outbuf_latency; /**< Output buffer latency */
        rd_kafka_avg_stats_t rtt;            /**< Round-trip time */
        rd_kafka_avg_stats_t throttle;       /**< Broker throttling time */

        /* Producer request stats */
        rd_kafka_avg_stats_t produce_partitions; /**< Partitions per ProduceRequest */
        rd_kafka_avg_stats_t produce_messages;   /**< Messages per ProduceRequest */
        rd_kafka_avg_stats_t produce_reqsize;    /**< Request size */
        rd_kafka_avg_stats_t produce_fill;       /**< Fill percentage (permille) */
        rd_kafka_avg_stats_t batch_wait;         /**< Batch wait time */

        /* Request type counts (only non-zero entries) */
        uint32_t req_cnt;                /**< Number of request type entries */
        uint32_t _pad2;
        rd_kafka_req_count_t *reqs;      /**< Request counts with names */

        /* Toppars assigned to this broker */
        uint32_t toppar_cnt;
        uint32_t _pad;
        rd_kafka_broker_toppar_ref_t *toppars;
} rd_kafka_broker_stats_t;

/**
 * @brief Consumer group statistics.
 */
typedef struct rd_kafka_cgrp_stats_s {
        int32_t state;           /**< Consumer group state */
        int64_t stateage_us;     /**< Time since last state change */
        int32_t join_state;      /**< Consumer group join state */
        int64_t rebalance_age_us; /**< Time since last rebalance */
        int32_t rebalance_cnt;   /**< Total rebalances */
        char rebalance_reason[RD_KAFKA_STATS_NAME_MAX]; /**< Last rebalance reason */
        int32_t assignment_size; /**< Current assignment partition count */
        int32_t _pad;
} rd_kafka_cgrp_stats_t;

/**
 * @brief Exactly-once semantics / idempotent producer statistics.
 */
typedef struct rd_kafka_eos_stats_s {
        int32_t idemp_state;       /**< Idempotent producer state */
        int64_t idemp_stateage_us; /**< Time since last idemp state change */
        int32_t txn_state;         /**< Transactional producer state */
        int64_t txn_stateage_us;   /**< Time since last txn state change */
        int8_t txn_may_enq;        /**< Transaction allows enqueuing */
        int8_t _pad[3];
        int64_t producer_id;       /**< Current producer ID (-1 if none) */
        int16_t producer_epoch;    /**< Current epoch (-1 if none) */
        int16_t _pad2;
        int32_t epoch_cnt;         /**< Number of producer ID assignments */
} rd_kafka_eos_stats_t;

/**
 * @brief Top-level statistics structure.
 *
 * This is the main structure passed to the statistics callback.
 * All nested arrays (brokers, topics, partitions) are allocated
 * in a single memory block for efficient allocation/deallocation.
 */
typedef struct rd_kafka_stats_s {
        /* Identification */
        char name[RD_KAFKA_STATS_NAME_MAX];      /**< Client handle name */
        char client_id[RD_KAFKA_STATS_NAME_MAX]; /**< Configured client.id */
        int32_t type;   /**< Client type: RD_KAFKA_PRODUCER or RD_KAFKA_CONSUMER */
        int32_t _pad;

        /* Timestamps */
        int64_t ts_us;    /**< Monotonic timestamp (microseconds since start) */
        int64_t time_sec; /**< Unix epoch timestamp (seconds) */
        int64_t age_us;   /**< Client age (microseconds) */

        /* Global state */
        int32_t replyq;             /**< Reply queue length */
        uint32_t msg_cnt;           /**< Current message count */
        uint64_t msg_size;          /**< Current message size */
        uint32_t msg_max;           /**< Max message count */
        uint64_t msg_size_max;      /**< Max message size */
        int32_t simple_cnt;         /**< Simple consumer count */
        int32_t metadata_cache_cnt; /**< Topics in metadata cache */

        /* Totals (aggregated across brokers) */
        int64_t tx;           /**< Total requests sent */
        int64_t tx_bytes;     /**< Total bytes sent */
        int64_t rx;           /**< Total responses received */
        int64_t rx_bytes;     /**< Total bytes received */
        int64_t txmsgs;       /**< Total messages produced */
        int64_t txmsg_bytes;  /**< Total message bytes produced */
        int64_t rxmsgs;       /**< Total messages consumed */
        int64_t rxmsg_bytes;  /**< Total message bytes consumed */

        /* Brokers */
        uint32_t broker_cnt;
        uint32_t _pad2;
        rd_kafka_broker_stats_t *brokers;

        /* Topics */
        uint32_t topic_cnt;
        uint32_t _pad3;
        rd_kafka_topic_stats_t *topics;

        /* Consumer group (optional) */
        int8_t has_cgrp;
        int8_t _pad4[7];
        rd_kafka_cgrp_stats_t cgrp;

        /* EOS (optional) */
        int8_t has_eos;
        int8_t _pad5[7];
        rd_kafka_eos_stats_t eos;

        /* Fatal error (optional) */
        int8_t has_fatal;
        int8_t _pad6[3];
        int32_t fatal_err;                            /**< rd_kafka_resp_err_t */
        char fatal_reason[512];                       /**< Error reason string */
        int32_t fatal_cnt;                            /**< Fatal error count */
        int32_t _pad7;
} rd_kafka_stats_t;

/* Forward declaration */
struct rd_kafka_s;

/**
 * @brief Allocate and populate a stats structure from the client state.
 *
 * @param rk The Kafka client handle.
 * @returns A new stats structure, or NULL on allocation failure.
 *          The caller must free with rd_kafka_stats_destroy().
 */
rd_kafka_stats_t *rd_kafka_stats_new(struct rd_kafka_s *rk);

/**
 * @brief Free a stats structure.
 *
 * @param stats The stats structure to free (may be NULL).
 */
void rd_kafka_stats_destroy(rd_kafka_stats_t *stats);


#endif /* _RDKAFKA_STATS_H_ */
