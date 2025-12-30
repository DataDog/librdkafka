/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
 * All rights reserved.
 *
 * Adaptive batching implementation - dynamically adjusts batching parameters
 * based on congestion signals using a TCP Vegas-inspired algorithm.
 *
 * Two signals are used for congestion detection:
 * 1. RTT (broker-side) - measures queue depth on brokers
 * 2. Internal latency (client-side) - measures local queue wait time
 *
 * When congestion is detected, we increase BOTH linger AND batch limits
 * together (coupled adjustment) to achieve fewer, larger requests.
 */

#include "rdkafka_int.h"
#include "rdkafka_adaptive.h"
#include "rdkafka_broker.h"

/* EWMA smoothing factor for RTT and internal latency.
 * Lower = smoother but slower to react.
 * Higher = more responsive but noisier. */
#define RD_KAFKA_ADAPTIVE_EWMA_ALPHA 0.1

/* Baseline decay: how much to decay baseline per interval.
 * This allows adaptation to permanent infrastructure changes. */
#define RD_KAFKA_ADAPTIVE_BASELINE_DECAY 1.01

/* Baseline decay interval in microseconds (10 seconds) */
#define RD_KAFKA_ADAPTIVE_BASELINE_DECAY_INTERVAL_US (10 * 1000 * 1000)

/* Multiplicative adjustment step (20% increase/decrease) */
#define RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP 1.2

/* Maximum consecutive increases before checking for runaway */
#define RD_KAFKA_ADAPTIVE_RUNAWAY_THRESHOLD 5

/* RTT staleness threshold in microseconds (5 seconds).
 * If no RTT samples received within this period, skip adjustment.
 * This prevents speeding up when broker is disconnected or unresponsive. */
#define RD_KAFKA_ADAPTIVE_STALENESS_THRESHOLD_US (5 * 1000 * 1000)

/* Backlog threshold for triggering drain mode.
 * When queue depth exceeds this AND congestion is low, we force
 * minimum linger to drain the backlog faster. */
#define RD_KAFKA_ADAPTIVE_BACKLOG_THRESHOLD 1000


/**
 * @brief Initialize adaptive batching state for a broker.
 */
void rd_kafka_adaptive_init(rd_kafka_broker_t *rkb) {
        rd_kafka_adaptive_state_t *state  = &rkb->rkb_adaptive_state;
        rd_kafka_adaptive_params_t *params = &rkb->rkb_adaptive_params;
        rd_kafka_conf_t *conf              = &rkb->rkb_rk->rk_conf;

        memset(state, 0, sizeof(*state));
        memset(params, 0, sizeof(*params));

        /* Initialize Vegas thresholds from config */
        state->alpha = conf->adaptive_alpha;
        state->beta  = conf->adaptive_beta;

        /* Initialize params from config bounds */
        params->linger_min_us       = conf->adaptive_linger_min_us;
        params->linger_max_us       = conf->adaptive_linger_max_us;
        params->batch_max_bytes_min = conf->adaptive_batch_min_bytes;
        params->batch_max_bytes_max = conf->adaptive_batch_max_bytes;

        /* Start at minimum values (most aggressive batching) */
        params->linger_us       = params->linger_min_us;
        params->batch_max_bytes = params->batch_max_bytes_min;

        /* Initialize timestamps */
        state->last_adjustment                   = rd_clock();
        state->rtt_stats.rtt_last_update         = 0;
        state->rtt_stats.rtt_base_update         = 0;
        state->int_lat_stats.int_lat_last_update = 0;
        state->int_lat_stats.int_lat_base_update = 0;

        rd_rkb_dbg(rkb, BROKER, "ADAPTIVE",
                   "Adaptive batching initialized: alpha=%.2f beta=%.2f "
                   "linger=[%"PRId64"ms-%"PRId64"ms] batch=[%"PRId64"KB-%"PRId64"KB]",
                   state->alpha, state->beta,
                   params->linger_min_us / 1000, params->linger_max_us / 1000,
                   params->batch_max_bytes_min / 1024,
                   params->batch_max_bytes_max / 1024);
}


/**
 * @brief Destroy adaptive batching state for a broker.
 */
void rd_kafka_adaptive_destroy(rd_kafka_broker_t *rkb) {
        /* Currently no dynamic allocations to free */
        (void)rkb;
}


/**
 * @brief Record an RTT sample from a produce response.
 *
 * Updates:
 * - Circular buffer of recent samples (for percentile calculations)
 * - EWMA smoothed current RTT
 * - Baseline (minimum) RTT with slow decay
 */
void rd_kafka_adaptive_record_rtt(rd_kafka_broker_t *rkb, rd_ts_t rtt_us) {
        rd_kafka_adaptive_rtt_stats_t *stats = &rkb->rkb_adaptive_state.rtt_stats;
        rd_ts_t now                          = rd_clock();

        if (rtt_us <= 0)
                return;

        /* Add to circular buffer */
        stats->rtt_samples[stats->rtt_sample_idx] = rtt_us;
        stats->rtt_sample_idx = (stats->rtt_sample_idx + 1) % 16;
        if (stats->rtt_sample_cnt < 16)
                stats->rtt_sample_cnt++;

        /* Update EWMA smoothed RTT */
        if (stats->rtt_current == 0) {
                /* First sample */
                stats->rtt_current = rtt_us;
        } else {
                stats->rtt_current = (rd_ts_t)(
                    RD_KAFKA_ADAPTIVE_EWMA_ALPHA * (double)rtt_us +
                    (1.0 - RD_KAFKA_ADAPTIVE_EWMA_ALPHA) *
                        (double)stats->rtt_current);
        }

        /* Update baseline (minimum RTT) */
        if (stats->rtt_base == 0 || rtt_us < stats->rtt_base) {
                stats->rtt_base        = rtt_us;
                stats->rtt_base_update = now;
        }

        /* Decay baseline slowly to adapt to infrastructure changes */
        if (stats->rtt_base_update > 0 &&
            now - stats->rtt_base_update >
                RD_KAFKA_ADAPTIVE_BASELINE_DECAY_INTERVAL_US) {
                stats->rtt_base =
                    (rd_ts_t)((double)stats->rtt_base *
                              RD_KAFKA_ADAPTIVE_BASELINE_DECAY);
                stats->rtt_base_update = now;
        }

        stats->rtt_last_update = now;
}


/**
 * @brief Record an internal latency sample.
 *
 * Internal latency = time from message enqueue to being sent.
 * This is a fast leading indicator of local congestion.
 */
void rd_kafka_adaptive_record_int_latency(rd_kafka_broker_t *rkb,
                                          rd_ts_t int_latency_us) {
        rd_kafka_adaptive_int_latency_stats_t *stats =
            &rkb->rkb_adaptive_state.int_lat_stats;
        rd_ts_t now = rd_clock();

        if (int_latency_us <= 0)
                return;

        /* Update EWMA smoothed internal latency */
        if (stats->int_lat_current == 0) {
                /* First sample */
                stats->int_lat_current = int_latency_us;
        } else {
                stats->int_lat_current = (rd_ts_t)(
                    RD_KAFKA_ADAPTIVE_EWMA_ALPHA * (double)int_latency_us +
                    (1.0 - RD_KAFKA_ADAPTIVE_EWMA_ALPHA) *
                        (double)stats->int_lat_current);
        }

        /* Update baseline (minimum internal latency) */
        if (stats->int_lat_base == 0 || int_latency_us < stats->int_lat_base) {
                stats->int_lat_base        = int_latency_us;
                stats->int_lat_base_update = now;
        }

        /* Decay baseline slowly */
        if (stats->int_lat_base_update > 0 &&
            now - stats->int_lat_base_update >
                RD_KAFKA_ADAPTIVE_BASELINE_DECAY_INTERVAL_US) {
                stats->int_lat_base =
                    (rd_ts_t)((double)stats->int_lat_base *
                              RD_KAFKA_ADAPTIVE_BASELINE_DECAY);
                stats->int_lat_base_update = now;
        }

        stats->int_lat_last_update = now;
}


/**
 * @brief Calculate current congestion score.
 *
 * Uses RTT-based congestion detection (Vegas formula): (current - base) / base
 *
 * NOTE: int_latency signal is currently disabled. It was too noisy because:
 * - Baseline used absolute minimum, which ratcheted down to ~100µs
 * - Normal variance then looked like 100x+ congestion
 * - Caused constant oscillation between SLOW_DOWN and SPEED_UP
 *
 * TODO: Re-enable int_latency with EWMA baseline + floor once RTT-only is stable.
 */
double rd_kafka_adaptive_calc_congestion(rd_kafka_broker_t *rkb) {
        rd_kafka_adaptive_state_t *state = &rkb->rkb_adaptive_state;

        /* Signal 1: RTT-based (Vegas) - authoritative, stable baseline */
        double rtt_congestion = 0.0;
        if (state->rtt_stats.rtt_base > 0 &&
            state->rtt_stats.rtt_current > state->rtt_stats.rtt_base) {
                rtt_congestion =
                    (double)(state->rtt_stats.rtt_current -
                             state->rtt_stats.rtt_base) /
                    (double)state->rtt_stats.rtt_base;
        }

        /* Signal 2: Internal latency - DISABLED (too noisy with min-based baseline)
         * TODO: Re-enable with EWMA baseline + floor */
        double int_lat_congestion = 0.0;
#if 0
        if (state->int_lat_stats.int_lat_base > 0 &&
            state->int_lat_stats.int_lat_current >
                state->int_lat_stats.int_lat_base) {
                int_lat_congestion =
                    (double)(state->int_lat_stats.int_lat_current -
                             state->int_lat_stats.int_lat_base) /
                    (double)state->int_lat_stats.int_lat_base;
        }
#endif

        /* Store individual components for observability */
        state->rtt_congestion     = rtt_congestion;
        state->int_lat_congestion = int_lat_congestion;

        /* Use RTT signal only for now */
        return RD_MAX(0.0, rtt_congestion);
}


/**
 * @brief Check if increasing limits is actually helping.
 *
 * Cinnamon improvement: detect if we're in runaway mode where
 * increasing limits isn't improving throughput but RTT keeps growing.
 *
 * DISABLED: Was causing oscillation during sustained degradation.
 * TODO: Re-enable with proper throughput tracking and cooldown period.
 */
#if 0
static void
rd_kafka_adaptive_check_covariance(rd_kafka_broker_t *rkb,
                                   double current_throughput,
                                   double current_rtt) {
        rd_kafka_adaptive_state_t *state = &rkb->rkb_adaptive_state;

        if (state->consecutive_increases > RD_KAFKA_ADAPTIVE_RUNAWAY_THRESHOLD) {
                double throughput_delta =
                    current_throughput - state->prev_throughput;
                double rtt_delta = current_rtt - state->prev_rtt;

                if (throughput_delta <= 0 && rtt_delta > 0) {
                        /* Increasing limits isn't helping - we're overloaded */
                        rd_rkb_dbg(rkb, BROKER, "ADAPTIVE",
                                   "Sustained overload detected: throughput "
                                   "not improving, RTT increasing. "
                                   "Resetting baseline.");

                        /* Reset baseline to current RTT to accept new normal */
                        state->rtt_stats.rtt_base = state->rtt_stats.rtt_current;
                        state->rtt_stats.rtt_base_update = rd_clock();

                        state->int_lat_stats.int_lat_base =
                            state->int_lat_stats.int_lat_current;
                        state->int_lat_stats.int_lat_base_update = rd_clock();

                        state->consecutive_increases = 0;
                }
        }

        state->prev_throughput = current_throughput;
        state->prev_rtt        = current_rtt;
}
#endif


/**
 * @brief Perform adaptive parameter adjustment.
 *
 * Called periodically to:
 * 1. Calculate congestion from RTT and internal latency signals
 * 2. Adjust linger and batch limits based on thresholds
 * 3. Enforce hard bounds
 *
 * Key insight: adjustments are COUPLED - when congestion is detected,
 * we increase BOTH linger AND batch limits together.
 */
void rd_kafka_adaptive_adjust(rd_kafka_broker_t *rkb) {
        rd_kafka_adaptive_state_t *state   = &rkb->rkb_adaptive_state;
        rd_kafka_adaptive_params_t *params = &rkb->rkb_adaptive_params;
        rd_ts_t old_linger                 = params->linger_us;
        int64_t old_batch                  = params->batch_max_bytes;
        const char *action;
        rd_ts_t now = rd_clock();

        /* Don't adjust if broker is not connected */
        if (rkb->rkb_state != RD_KAFKA_BROKER_STATE_UP)
                return;

        /* Don't adjust if RTT data is stale (no recent samples).
         * This prevents speeding up when broker is disconnected or
         * just reconnected but hasn't received responses yet. */
        if ((now - state->rtt_stats.rtt_last_update) >
            RD_KAFKA_ADAPTIVE_STALENESS_THRESHOLD_US)
                return;

        /* Calculate congestion from both signals */
        double congestion       = rd_kafka_adaptive_calc_congestion(rkb);
        state->congestion_score = congestion;

        /* Check for backlog that needs draining.
         * If queue is large but RTT has recovered (congestion low),
         * speed up to drain faster using normal adjustment step. */
        int queue_depth = rd_kafka_curr_msgs_cnt(rkb->rkb_rk);
        if (queue_depth > RD_KAFKA_ADAPTIVE_BACKLOG_THRESHOLD &&
            congestion < state->alpha) {
                /* Backlog with healthy RTT - speed up to drain it */
                rd_ts_t old_linger  = params->linger_us;
                int64_t old_batch   = params->batch_max_bytes;

                params->linger_us = (rd_ts_t)((double)params->linger_us /
                                              RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP);
                params->batch_max_bytes =
                    (int64_t)((double)params->batch_max_bytes /
                              RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP);

                /* Enforce bounds */
                params->linger_us = RD_MAX(params->linger_min_us,
                                           params->linger_us);
                params->batch_max_bytes = RD_MAX(params->batch_max_bytes_min,
                                                 params->batch_max_bytes);

                state->backlog_drain_events++;
                state->adjustments_down++;

                if (params->linger_us != old_linger ||
                    params->batch_max_bytes != old_batch) {
                        rd_rkb_dbg(rkb, BROKER, "ADAPTIVE",
                                   "BACKLOG_DRAIN: queue_depth=%d (threshold=%d) "
                                   "congestion=%.3f < alpha=%.2f "
                                   "linger=%"PRId64"ms->%"PRId64"ms "
                                   "batch=%"PRId64"KB->%"PRId64"KB",
                                   queue_depth, RD_KAFKA_ADAPTIVE_BACKLOG_THRESHOLD,
                                   congestion, state->alpha,
                                   old_linger / 1000, params->linger_us / 1000,
                                   old_batch / 1024, params->batch_max_bytes / 1024);
                }

                state->last_adjustment = now;
                return;
        }

        if (congestion < state->alpha) {
                /* Low congestion - we can speed up (decrease linger/batch) */
                params->linger_us = (rd_ts_t)((double)params->linger_us /
                                              RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP);
                params->batch_max_bytes =
                    (int64_t)((double)params->batch_max_bytes /
                              RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP);

                state->consecutive_decreases++;
                state->consecutive_increases = 0;
                state->adjustments_down++;
                action = "SPEED_UP";

        } else if (congestion > state->beta) {
                /* High congestion - slow down (increase linger AND batch) */
                /* COUPLED ADJUSTMENT: both change together */
                params->linger_us = (rd_ts_t)((double)params->linger_us *
                                              RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP);
                params->batch_max_bytes =
                    (int64_t)((double)params->batch_max_bytes *
                              RD_KAFKA_ADAPTIVE_ADJUSTMENT_STEP);

                state->consecutive_increases++;
                state->consecutive_decreases = 0;
                state->adjustments_up++;
                action = "SLOW_DOWN";

                /* Runaway detection DISABLED - was causing oscillation during
                 * sustained degradation. The check would reset baseline after
                 * 5 consecutive increases, which then triggered SPEED_UP,
                 * followed by more SLOW_DOWNs, creating a loop.
                 *
                 * Instead, we rely on:
                 * 1. Hard bounds (linger_max, batch_max) to cap growth
                 * 2. Baseline decay (1% per 10s) for gradual adaptation
                 *
                 * TODO: Re-enable with proper throughput tracking and cooldown.
                 */
#if 0
                rd_kafka_adaptive_check_covariance(
                    rkb,
                    0.0, /* TODO: pass actual throughput */
                    (double)state->rtt_stats.rtt_current);
#endif

        } else {
                /* Stable - maintain current settings */
                state->consecutive_increases = 0;
                state->consecutive_decreases = 0;
                action                       = "STABLE";
        }

        /* Enforce hard bounds: clamp to [min, max] range */
        params->linger_us = RD_MAX(params->linger_min_us,
                                   RD_MIN(params->linger_max_us,
                                          params->linger_us));
        params->batch_max_bytes = RD_MAX(params->batch_max_bytes_min,
                                         RD_MIN(params->batch_max_bytes_max,
                                                params->batch_max_bytes));

        state->last_adjustment = rd_clock();

        /* Log adjustment if values changed */
        if (params->linger_us != old_linger ||
            params->batch_max_bytes != old_batch) {
                rd_rkb_dbg(rkb, BROKER, "ADAPTIVE",
                           "%s: congestion=%.3f (rtt=%.3f int_lat=%.3f) "
                           "linger=%"PRId64"ms->%"PRId64"ms "
                           "batch=%"PRId64"KB->%"PRId64"KB "
                           "rtt_base=%"PRId64"us rtt_cur=%"PRId64"us "
                           "int_lat_base=%"PRId64"us int_lat_cur=%"PRId64"us",
                           action, congestion, state->rtt_congestion,
                           state->int_lat_congestion, old_linger / 1000,
                           params->linger_us / 1000, old_batch / 1024,
                           params->batch_max_bytes / 1024,
                           state->rtt_stats.rtt_base,
                           state->rtt_stats.rtt_current,
                           state->int_lat_stats.int_lat_base,
                           state->int_lat_stats.int_lat_current);
        }
}


/**
 * @brief Get current adaptive linger value.
 */
rd_ts_t rd_kafka_adaptive_get_linger_us(rd_kafka_broker_t *rkb) {
        if (rkb->rkb_rk->rk_conf.adaptive_batching_enabled)
                return rkb->rkb_adaptive_params.linger_us;
        else
                return rkb->rkb_rk->rk_conf.broker_linger_us;
}


/**
 * @brief Get current adaptive batch max bytes.
 */
int64_t rd_kafka_adaptive_get_batch_max_bytes(rd_kafka_broker_t *rkb) {
        if (rkb->rkb_rk->rk_conf.adaptive_batching_enabled)
                return rkb->rkb_adaptive_params.batch_max_bytes;
        else
                return rkb->rkb_rk->rk_conf.broker_batch_max_bytes;
}


/**
 * @brief Check if it's time to perform an adjustment.
 */
rd_bool_t rd_kafka_adaptive_should_adjust(rd_kafka_broker_t *rkb) {
        rd_kafka_adaptive_state_t *state = &rkb->rkb_adaptive_state;
        rd_ts_t now                      = rd_clock();

        if (!rkb->rkb_rk->rk_conf.adaptive_batching_enabled)
                return rd_false;

        return (now - state->last_adjustment) >
               rkb->rkb_rk->rk_conf.adaptive_adjustment_interval_us;
}
