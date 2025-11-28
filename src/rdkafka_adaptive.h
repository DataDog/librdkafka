/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
 * All rights reserved.
 *
 * Adaptive batching - dynamically adjusts batching parameters based on
 * congestion signals using a TCP Vegas-inspired algorithm.
 */

#ifndef _RDKAFKA_ADAPTIVE_H_
#define _RDKAFKA_ADAPTIVE_H_

#include "rd.h"
#include "rdtime.h"

/**
 * @brief RTT tracking statistics for Vegas-style congestion detection.
 *
 * Tracks both baseline (minimum seen) and current (EWMA smoothed) RTT
 * to calculate queue depth estimates.
 */
typedef struct rd_kafka_adaptive_rtt_stats_s {
        rd_ts_t rtt_base;         /**< Minimum RTT seen (baseline) */
        rd_ts_t rtt_current;      /**< Current smoothed RTT (EWMA) */
        rd_ts_t rtt_samples[16];  /**< Recent samples for percentile calc */
        int rtt_sample_idx;       /**< Circular buffer index */
        int rtt_sample_cnt;       /**< Number of valid samples */
        rd_ts_t rtt_last_update;  /**< Timestamp of last RTT update */
        rd_ts_t rtt_base_update;  /**< Timestamp of last baseline update */
} rd_kafka_adaptive_rtt_stats_t;

/**
 * @brief Internal latency tracking for client-side congestion detection.
 *
 * Internal latency = time from message enqueue to actually being sent.
 * This is a fast leading indicator of local congestion.
 */
typedef struct rd_kafka_adaptive_int_latency_stats_s {
        rd_ts_t int_lat_base;        /**< Minimum int_latency seen (baseline) */
        rd_ts_t int_lat_current;     /**< Current smoothed int_latency (EWMA) */
        rd_ts_t int_lat_last_update; /**< Timestamp of last update */
        rd_ts_t int_lat_base_update; /**< Timestamp of last baseline update */
} rd_kafka_adaptive_int_latency_stats_t;

/**
 * @brief Runtime-adjustable batching parameters.
 *
 * These are the effective values that change at runtime based on
 * congestion signals. They float within the configured min/max bounds.
 */
typedef struct rd_kafka_adaptive_params_s {
        /* Current effective values (adjusted at runtime) */
        rd_ts_t linger_us;           /**< Current linger in microseconds */
        int64_t batch_max_bytes;     /**< Current max bytes per request */

        /* Configured bounds (from config, immutable after init) */
        rd_ts_t linger_min_us;       /**< Minimum linger (floor) */
        rd_ts_t linger_max_us;       /**< Maximum linger (ceiling) */
        int64_t batch_max_bytes_min; /**< Minimum batch size */
        int64_t batch_max_bytes_max; /**< Maximum batch size */
} rd_kafka_adaptive_params_t;

/**
 * @brief Adaptive batching state for a broker connection.
 *
 * Contains all state needed for the Vegas-inspired congestion control:
 * - RTT tracking (broker-side signal)
 * - Internal latency tracking (client-side signal)
 * - Congestion scores
 * - Adjustment tracking for stability
 */
typedef struct rd_kafka_adaptive_state_s {
        /* Vegas parameters (thresholds) */
        double alpha; /**< Low threshold - below this, speed up */
        double beta;  /**< High threshold - above this, slow down */

        /* Current congestion state */
        double congestion_score;     /**< Combined congestion estimate */
        double rtt_congestion;       /**< RTT-based congestion component */
        double int_lat_congestion;   /**< Int latency congestion component */
        rd_ts_t last_adjustment;     /**< When we last changed parameters */

        /* RTT tracking (broker-side) */
        rd_kafka_adaptive_rtt_stats_t rtt_stats;

        /* Internal latency tracking (client-side queue wait) */
        rd_kafka_adaptive_int_latency_stats_t int_lat_stats;

        /* Adjustment tracking for stability */
        int consecutive_increases; /**< Track runaway detection */
        int consecutive_decreases;

        /* Covariance tracking (Cinnamon improvement) */
        double prev_throughput;
        double prev_rtt;

        /* Statistics counters */
        int64_t adjustments_up;   /**< Count of slow-down adjustments */
        int64_t adjustments_down; /**< Count of speed-up adjustments */
} rd_kafka_adaptive_state_t;

/* Forward declaration */
struct rd_kafka_broker_s;

/**
 * @brief Initialize adaptive batching state for a broker.
 *
 * @param rkb The broker to initialize adaptive state for.
 */
void rd_kafka_adaptive_init(struct rd_kafka_broker_s *rkb);

/**
 * @brief Destroy adaptive batching state for a broker.
 *
 * @param rkb The broker to destroy adaptive state for.
 */
void rd_kafka_adaptive_destroy(struct rd_kafka_broker_s *rkb);

/**
 * @brief Record an RTT sample from a produce response.
 *
 * Updates the EWMA smoothed RTT and baseline tracking.
 *
 * @param rkb The broker that received the response.
 * @param rtt_us The RTT in microseconds.
 */
void rd_kafka_adaptive_record_rtt(struct rd_kafka_broker_s *rkb,
                                  rd_ts_t rtt_us);

/**
 * @brief Record an internal latency sample.
 *
 * Internal latency = time from message enqueue to being sent.
 * Updates the EWMA smoothed value and baseline tracking.
 *
 * @param rkb The broker.
 * @param int_latency_us The internal latency in microseconds.
 */
void rd_kafka_adaptive_record_int_latency(struct rd_kafka_broker_s *rkb,
                                          rd_ts_t int_latency_us);

/**
 * @brief Calculate current congestion score.
 *
 * Combines RTT-based (Vegas) and internal latency signals.
 * Returns MAX of both signals - react to either showing pressure.
 *
 * @param rkb The broker.
 * @returns Combined congestion score (0.0 = no congestion, >0 = congested)
 */
double rd_kafka_adaptive_calc_congestion(struct rd_kafka_broker_s *rkb);

/**
 * @brief Perform adaptive parameter adjustment.
 *
 * Called periodically to check congestion and adjust linger/batch params.
 * Implements the Vegas-inspired algorithm with coupled adjustments.
 *
 * @param rkb The broker to adjust parameters for.
 */
void rd_kafka_adaptive_adjust(struct rd_kafka_broker_s *rkb);

/**
 * @brief Get current adaptive linger value.
 *
 * Returns the adaptive linger if enabled, otherwise the static config value.
 *
 * @param rkb The broker.
 * @returns Current linger in microseconds.
 */
rd_ts_t rd_kafka_adaptive_get_linger_us(struct rd_kafka_broker_s *rkb);

/**
 * @brief Get current adaptive batch max bytes.
 *
 * Returns the adaptive batch limit if enabled, otherwise the static config.
 *
 * @param rkb The broker.
 * @returns Current max bytes per request.
 */
int64_t rd_kafka_adaptive_get_batch_max_bytes(struct rd_kafka_broker_s *rkb);

/**
 * @brief Check if it's time to perform an adjustment.
 *
 * @param rkb The broker.
 * @returns rd_true if adjustment interval has elapsed.
 */
rd_bool_t rd_kafka_adaptive_should_adjust(struct rd_kafka_broker_s *rkb);

#endif /* _RDKAFKA_ADAPTIVE_H_ */
