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

#include "rdkafka_int.h"
#include "rdkafka_broker.h"
#include "rdkafka_topic.h"
#include "rdkafka_partition.h"
#include "rdkafka_cgrp.h"
#include "rdkafka_stats.h"
#include "rdkafka_adaptive.h"
#include "rdavg.h"

#include <string.h>
#include <time.h>

/**
 * @brief Helper to safely copy a string into a fixed-size buffer.
 */
static void rd_kafka_stats_strcpy(char *dst, size_t dst_size, const char *src) {
        if (src) {
                rd_snprintf(dst, dst_size, "%s", src);
        } else {
                dst[0] = '\0';
        }
}

/**
 * @brief Convert rd_avg_t to rd_kafka_avg_stats_t.
 *
 * This performs a rollover of the source average and extracts values.
 * The caller must call rd_avg_destroy() on the temporary avg after use.
 */
static void rd_kafka_stats_avg_populate(rd_kafka_avg_stats_t *dst,
                                         rd_avg_t *src) {
        rd_avg_t avg;

        rd_avg_rollover(&avg, src);

        dst->min     = avg.ra_v.minv;
        dst->max     = avg.ra_v.maxv;
        dst->avg     = avg.ra_v.avg;
        dst->sum     = avg.ra_v.sum;
        dst->cnt     = avg.ra_v.cnt;
        dst->stddev  = (int64_t)avg.ra_hist.stddev;
        dst->p50     = avg.ra_hist.p50;
        dst->p75     = avg.ra_hist.p75;
        dst->p90     = avg.ra_hist.p90;
        dst->p95     = avg.ra_hist.p95;
        dst->p99     = avg.ra_hist.p99;
        dst->p99_99  = avg.ra_hist.p99_99;
        dst->oor     = avg.ra_hist.oor;
        dst->hdrsize = avg.ra_hist.hdrsize;

        rd_avg_destroy(&avg);
}

/**
 * @brief Count elements needed for allocation sizing.
 */
struct rd_kafka_stats_counts {
        int broker_cnt;
        int topic_cnt;
        int total_partitions;
        int total_broker_toppars;
        int total_broker_reqs; /**< Total non-zero req counts across all brokers */
};

static void rd_kafka_stats_count(rd_kafka_t *rk,
                                  struct rd_kafka_stats_counts *counts) {
        rd_kafka_broker_t *rkb;
        rd_kafka_topic_t *rkt;

        memset(counts, 0, sizeof(*counts));

        /* Count brokers and their toppars and non-zero req counts */
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_kafka_toppar_t *rktp;
                int i;
                counts->broker_cnt++;

                rd_kafka_broker_lock(rkb);
                TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
                        counts->total_broker_toppars++;
                }
                /* Count non-zero request type counts */
                for (i = 0; i < RD_KAFKAP__NUM; i++) {
                        if (rd_atomic64_get(&rkb->rkb_c.reqtype[i]) > 0)
                                counts->total_broker_reqs++;
                }
                rd_kafka_broker_unlock(rkb);
        }

        /* Count topics and partitions */
        TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
                int i;
                rd_kafka_toppar_t *rktp;

                counts->topic_cnt++;

                rd_kafka_topic_rdlock(rkt);
                /* Regular partitions */
                counts->total_partitions += rkt->rkt_partition_cnt;
                /* Desired partitions (rkt_desp) */
                RD_LIST_FOREACH(rktp, &rkt->rkt_desp, i) {
                        counts->total_partitions++;
                }
                /* Unknown/unassigned partition */
                if (rkt->rkt_ua)
                        counts->total_partitions++;
                rd_kafka_topic_rdunlock(rkt);
        }
}

/**
 * @brief Populate partition stats.
 */
static void rd_kafka_stats_partition_populate(rd_kafka_partition_stats_t *dst,
                                               rd_kafka_toppar_t *rktp,
                                               rd_kafka_t *rk,
                                               rd_ts_t now,
                                               int64_t *txmsgs_total,
                                               int64_t *txmsg_bytes_total,
                                               int64_t *rxmsgs_total,
                                               int64_t *rxmsg_bytes_total) {
        int64_t end_offset;
        int64_t consumer_lag        = -1;
        int64_t consumer_lag_stored = -1;
        struct offset_stats offs;

        rd_kafka_toppar_lock(rktp);

        dst->partition = rktp->rktp_partition;
        dst->broker_id = rktp->rktp_broker ? rktp->rktp_broker->rkb_nodeid : -1;
        dst->leader    = rktp->rktp_leader_id;

        dst->desired = (rktp->rktp_flags & RD_KAFKA_TOPPAR_F_DESIRED) ? 1 : 0;
        dst->unknown = (rktp->rktp_flags & RD_KAFKA_TOPPAR_F_UNKNOWN) ? 1 : 0;

        /* Producer queue stats */
        dst->msgq_cnt        = rd_kafka_msgq_len(&rktp->rktp_msgq);
        dst->msgq_bytes      = rd_kafka_msgq_size(&rktp->rktp_msgq);
        dst->xmit_msgq_cnt   = 0; /* FIXME: xmit_msgq is local to broker thread */
        dst->xmit_msgq_bytes = 0;

        /* Consumer stats */
        dst->fetchq_cnt  = rd_kafka_q_len(rktp->rktp_fetchq);
        dst->fetchq_size = rd_kafka_q_size(rktp->rktp_fetchq);
        dst->fetch_state = rktp->rktp_fetch_state;

        /* Grab finalized offset stats */
        offs = rktp->rktp_offsets_fin;

        /* Offsets */
        dst->query_offset          = rktp->rktp_query_pos.offset;
        dst->next_offset           = offs.fetch_pos.offset;
        dst->app_offset            = rktp->rktp_app_pos.offset;
        dst->stored_offset         = rktp->rktp_stored_pos.offset;
        dst->stored_leader_epoch   = rktp->rktp_stored_pos.leader_epoch;
        dst->committed_offset      = rktp->rktp_committed_pos.offset;
        dst->committed_leader_epoch = rktp->rktp_committed_pos.leader_epoch;
        dst->eof_offset            = offs.eof_offset;
        dst->lo_offset             = rktp->rktp_lo_offset;
        dst->hi_offset             = rktp->rktp_hi_offset;
        dst->ls_offset             = rktp->rktp_ls_offset;
        dst->leader_epoch          = rktp->rktp_leader_epoch;

        /* Calculate consumer lag */
        end_offset = (rk->rk_conf.isolation_level == RD_KAFKA_READ_COMMITTED)
                         ? rktp->rktp_ls_offset
                         : rktp->rktp_hi_offset;

        if (end_offset != RD_KAFKA_OFFSET_INVALID) {
                if (rktp->rktp_stored_pos.offset >= 0 &&
                    rktp->rktp_stored_pos.offset <= end_offset)
                        consumer_lag_stored =
                            end_offset - rktp->rktp_stored_pos.offset;
                if (rktp->rktp_committed_pos.offset >= 0 &&
                    rktp->rktp_committed_pos.offset <= end_offset)
                        consumer_lag =
                            end_offset - rktp->rktp_committed_pos.offset;
        }
        dst->consumer_lag        = consumer_lag;
        dst->consumer_lag_stored = consumer_lag_stored;

        /* Counters */
        dst->txmsgs  = rd_atomic64_get(&rktp->rktp_c.tx_msgs);
        dst->txbytes = rd_atomic64_get(&rktp->rktp_c.tx_msg_bytes);
        dst->rxmsgs  = rd_atomic64_get(&rktp->rktp_c.rx_msgs);
        dst->rxbytes = rd_atomic64_get(&rktp->rktp_c.rx_msg_bytes);

        dst->msgs = rk->rk_type == RD_KAFKA_PRODUCER
                        ? rd_atomic64_get(&rktp->rktp_c.producer_enq_msgs)
                        : rd_atomic64_get(&rktp->rktp_c.rx_msgs);

        dst->rx_ver_drops  = rd_atomic64_get(&rktp->rktp_c.rx_ver_drops);
        dst->msgs_inflight = rd_atomic32_get(&rktp->rktp_msgs_inflight);
        dst->next_ack_seq  = rktp->rktp_eos.next_ack_seq;
        dst->next_err_seq  = rktp->rktp_eos.next_err_seq;
        dst->acked_msgid   = rktp->rktp_eos.acked_msgid;

        /* Accumulate totals */
        if (txmsgs_total)
                *txmsgs_total += dst->txmsgs;
        if (txmsg_bytes_total)
                *txmsg_bytes_total += dst->txbytes;
        if (rxmsgs_total)
                *rxmsgs_total += dst->rxmsgs;
        if (rxmsg_bytes_total)
                *rxmsg_bytes_total += dst->rxbytes;

        rd_kafka_toppar_unlock(rktp);
}

/**
 * @brief Populate topic stats.
 */
static int rd_kafka_stats_topic_populate(rd_kafka_topic_stats_t *dst,
                                          rd_kafka_topic_t *rkt,
                                          rd_kafka_partition_stats_t **part_ptr,
                                          rd_kafka_t *rk,
                                          rd_ts_t now,
                                          int64_t *txmsgs_total,
                                          int64_t *txmsg_bytes_total,
                                          int64_t *rxmsgs_total,
                                          int64_t *rxmsg_bytes_total) {
        int i, j;
        rd_kafka_toppar_t *rktp;
        int partition_idx = 0;

        rd_kafka_topic_rdlock(rkt);

        /* Copy topic name */
        rd_kafka_stats_strcpy(dst->name, sizeof(dst->name),
                               rkt->rkt_topic->str);

        dst->age_us          = now - rkt->rkt_ts_create;
        dst->metadata_age_us = rkt->rkt_ts_metadata ? (now - rkt->rkt_ts_metadata) : 0;

        /* Batch stats */
        rd_kafka_stats_avg_populate(&dst->batchsize, &rkt->rkt_avg_batchsize);
        rd_kafka_stats_avg_populate(&dst->batchcnt, &rkt->rkt_avg_batchcnt);

        /* Count partitions for this topic */
        dst->partition_cnt = rkt->rkt_partition_cnt;
        RD_LIST_FOREACH(rktp, &rkt->rkt_desp, j) {
                dst->partition_cnt++;
        }
        if (rkt->rkt_ua)
                dst->partition_cnt++;

        /* Set partition array pointer */
        dst->partitions = *part_ptr;

        /* Populate regular partitions */
        for (i = 0; i < rkt->rkt_partition_cnt; i++) {
                rd_kafka_stats_partition_populate(
                    &dst->partitions[partition_idx++], rkt->rkt_p[i], rk, now,
                    txmsgs_total, txmsg_bytes_total, rxmsgs_total,
                    rxmsg_bytes_total);
        }

        /* Populate desired partitions */
        RD_LIST_FOREACH(rktp, &rkt->rkt_desp, j) {
                rd_kafka_stats_partition_populate(
                    &dst->partitions[partition_idx++], rktp, rk, now,
                    txmsgs_total, txmsg_bytes_total, rxmsgs_total,
                    rxmsg_bytes_total);
        }

        /* Populate unknown/unassigned partition */
        if (rkt->rkt_ua) {
                rd_kafka_stats_partition_populate(
                    &dst->partitions[partition_idx++], rkt->rkt_ua, rk, now,
                    NULL, NULL, NULL, NULL); /* Don't count UA in totals */
        }

        rd_kafka_topic_rdunlock(rkt);

        /* Advance the partition pointer for the next topic */
        *part_ptr += partition_idx;

        return partition_idx;
}

/**
 * @brief Populate broker stats.
 */
static void rd_kafka_stats_broker_populate(rd_kafka_broker_stats_t *dst,
                                            rd_kafka_broker_t *rkb,
                                            rd_kafka_broker_toppar_ref_t **toppar_ptr,
                                            rd_kafka_req_count_t **req_ptr,
                                            rd_ts_t now,
                                            int64_t *tx_total,
                                            int64_t *tx_bytes_total,
                                            int64_t *rx_total,
                                            int64_t *rx_bytes_total) {
        rd_kafka_toppar_t *rktp;
        int toppar_idx = 0;
        int req_idx    = 0;
        rd_ts_t txidle = -1, rxidle = -1;
        int i;

        rd_kafka_broker_lock(rkb);

        rd_kafka_stats_strcpy(dst->name, sizeof(dst->name), rkb->rkb_name);
        dst->nodeid = rkb->rkb_nodeid;
        rd_kafka_stats_strcpy(dst->nodename, sizeof(dst->nodename),
                               rkb->rkb_nodename);
        rd_kafka_stats_strcpy(dst->source, sizeof(dst->source),
                               rd_kafka_confsource2str(rkb->rkb_source));
        dst->state      = rkb->rkb_state;
        dst->stateage_us = rkb->rkb_ts_state ? now - rkb->rkb_ts_state : 0;

        /* Buffer counts */
        dst->outbuf_cnt      = rd_atomic32_get(&rkb->rkb_outbufs.rkbq_cnt);
        dst->outbuf_msg_cnt  = rd_atomic32_get(&rkb->rkb_outbufs.rkbq_msg_cnt);
        dst->waitresp_cnt    = rd_atomic32_get(&rkb->rkb_waitresps.rkbq_cnt);
        dst->waitresp_msg_cnt = rd_atomic32_get(&rkb->rkb_waitresps.rkbq_msg_cnt);

        /* Calculate idle times */
        if (rkb->rkb_state >= RD_KAFKA_BROKER_STATE_UP) {
                txidle = rd_atomic64_get(&rkb->rkb_c.ts_send);
                rxidle = rd_atomic64_get(&rkb->rkb_c.ts_recv);

                if (txidle)
                        txidle = RD_MAX(now - txidle, 0);
                else
                        txidle = -1;

                if (rxidle)
                        rxidle = RD_MAX(now - rxidle, 0);
                else
                        rxidle = -1;
        }

        /* Counters */
        dst->tx           = rd_atomic64_get(&rkb->rkb_c.tx);
        dst->tx_bytes     = rd_atomic64_get(&rkb->rkb_c.tx_bytes);
        dst->tx_errs      = rd_atomic64_get(&rkb->rkb_c.tx_err);
        dst->tx_retries   = rd_atomic64_get(&rkb->rkb_c.tx_retries);
        dst->tx_idle_us   = txidle;
        dst->req_timeouts = rd_atomic64_get(&rkb->rkb_c.req_timeouts);
        dst->rx           = rd_atomic64_get(&rkb->rkb_c.rx);
        dst->rx_bytes     = rd_atomic64_get(&rkb->rkb_c.rx_bytes);
        dst->rx_errs      = rd_atomic64_get(&rkb->rkb_c.rx_err);
        dst->rx_idle_us   = rxidle;
        dst->rx_corriderrs = rd_atomic64_get(&rkb->rkb_c.rx_corrid_err);
        dst->rx_partial   = rd_atomic64_get(&rkb->rkb_c.rx_partial);
        dst->zbuf_grow    = rd_atomic64_get(&rkb->rkb_c.zbuf_grow);
        dst->buf_grow     = rd_atomic64_get(&rkb->rkb_c.buf_grow);
        dst->wakeups      = rd_atomic64_get(&rkb->rkb_c.wakeups);
        dst->connects     = rd_atomic32_get(&rkb->rkb_c.connects);
        dst->disconnects  = rd_atomic32_get(&rkb->rkb_c.disconnects);

        /* Accumulate totals */
        if (tx_total)
                *tx_total += dst->tx;
        if (tx_bytes_total)
                *tx_bytes_total += dst->tx_bytes;
        if (rx_total)
                *rx_total += dst->rx;
        if (rx_bytes_total)
                *rx_bytes_total += dst->rx_bytes;

        /* Latency averages */
        rd_kafka_stats_avg_populate(&dst->int_latency, &rkb->rkb_avg_int_latency);
        rd_kafka_stats_avg_populate(&dst->outbuf_latency, &rkb->rkb_avg_outbuf_latency);
        rd_kafka_stats_avg_populate(&dst->rtt, &rkb->rkb_avg_rtt);
        rd_kafka_stats_avg_populate(&dst->throttle, &rkb->rkb_avg_throttle);

        /* Producer request stats */
        rd_kafka_stats_avg_populate(&dst->produce_partitions, &rkb->rkb_avg_produce_partitions);
        rd_kafka_stats_avg_populate(&dst->produce_messages, &rkb->rkb_avg_produce_messages);
        rd_kafka_stats_avg_populate(&dst->produce_reqsize, &rkb->rkb_avg_produce_reqsize);
        rd_kafka_stats_avg_populate(&dst->produce_fill, &rkb->rkb_avg_produce_fill);
        rd_kafka_stats_avg_populate(&dst->batch_wait, &rkb->rkb_avg_batch_wait);

        /* Adaptive batching stats */
        dst->adaptive_enabled = rkb->rkb_rk->rk_conf.adaptive_batching_enabled;
        if (dst->adaptive_enabled) {
                rd_kafka_adaptive_state_t *state = &rkb->rkb_adaptive_state;
                rd_kafka_adaptive_params_t *params = &rkb->rkb_adaptive_params;

                dst->adaptive_linger_us = params->linger_us;
                dst->adaptive_batch_max_bytes = params->batch_max_bytes;
                dst->adaptive_congestion = state->congestion_score;
                dst->adaptive_rtt_congestion = state->rtt_congestion;
                dst->adaptive_int_lat_congestion = state->int_lat_congestion;
                dst->adaptive_rtt_base_us = state->rtt_stats.rtt_base;
                dst->adaptive_rtt_current_us = state->rtt_stats.rtt_current;
                dst->adaptive_int_lat_base_us = state->int_lat_stats.int_lat_base;
                dst->adaptive_int_lat_current_us = state->int_lat_stats.int_lat_current;
                dst->adaptive_adjustments_up = state->adjustments_up;
                dst->adaptive_adjustments_down = state->adjustments_down;
        }

        /* Request type counts (only non-zero entries with names) */
        dst->reqs = *req_ptr;
        for (i = 0; i < RD_KAFKAP__NUM; i++) {
                int64_t cnt = rd_atomic64_get(&rkb->rkb_c.reqtype[i]);
                if (cnt > 0) {
                        rd_kafka_req_count_t *req = &dst->reqs[req_idx++];
                        rd_kafka_stats_strcpy(req->name, sizeof(req->name),
                                               rd_kafka_ApiKey2str((int16_t)i));
                        req->count = cnt;
                }
        }
        dst->req_cnt = req_idx;

        /* Count and populate toppars */
        dst->toppar_cnt = 0;
        TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
                dst->toppar_cnt++;
        }

        dst->toppars = *toppar_ptr;
        TAILQ_FOREACH(rktp, &rkb->rkb_toppars, rktp_rkblink) {
                rd_kafka_broker_toppar_ref_t *ref = &dst->toppars[toppar_idx++];
                rd_kafka_stats_strcpy(ref->topic, sizeof(ref->topic),
                                       rktp->rktp_rkt->rkt_topic->str);
                ref->partition = rktp->rktp_partition;
        }

        rd_kafka_broker_unlock(rkb);

        /* Advance pointers for the next broker */
        *toppar_ptr += toppar_idx;
        *req_ptr += req_idx;
}

/**
 * @brief Allocate and populate a stats structure.
 */
rd_kafka_stats_t *rd_kafka_stats_new(rd_kafka_t *rk) {
        struct rd_kafka_stats_counts counts;
        size_t total_size;
        char *buf;
        char *ptr;
        rd_kafka_stats_t *stats;
        rd_kafka_broker_t *rkb;
        rd_kafka_topic_t *rkt;
        rd_ts_t now;
        unsigned int tot_cnt;
        size_t tot_size;
        int broker_idx   = 0;
        int topic_idx    = 0;
        rd_kafka_partition_stats_t *part_ptr;
        rd_kafka_broker_toppar_ref_t *toppar_ptr;
        rd_kafka_req_count_t *req_ptr;

        rd_kafka_rdlock(rk);

        /* Count elements for sizing */
        rd_kafka_stats_count(rk, &counts);

        /* Calculate total allocation size */
        total_size = sizeof(rd_kafka_stats_t);
        total_size += counts.broker_cnt * sizeof(rd_kafka_broker_stats_t);
        total_size += counts.topic_cnt * sizeof(rd_kafka_topic_stats_t);
        total_size += counts.total_partitions * sizeof(rd_kafka_partition_stats_t);
        total_size += counts.total_broker_toppars * sizeof(rd_kafka_broker_toppar_ref_t);
        total_size += counts.total_broker_reqs * sizeof(rd_kafka_req_count_t);

        /* Single allocation */
        buf = rd_calloc(1, total_size);
        if (!buf) {
                rd_kafka_rdunlock(rk);
                return NULL;
        }

        /* Layout pointers within the allocation */
        ptr   = buf;
        stats = (rd_kafka_stats_t *)ptr;
        ptr += sizeof(rd_kafka_stats_t);

        stats->brokers = (rd_kafka_broker_stats_t *)ptr;
        stats->broker_cnt = counts.broker_cnt;
        ptr += counts.broker_cnt * sizeof(rd_kafka_broker_stats_t);

        stats->topics = (rd_kafka_topic_stats_t *)ptr;
        stats->topic_cnt = counts.topic_cnt;
        ptr += counts.topic_cnt * sizeof(rd_kafka_topic_stats_t);

        part_ptr = (rd_kafka_partition_stats_t *)ptr;
        ptr += counts.total_partitions * sizeof(rd_kafka_partition_stats_t);

        toppar_ptr = (rd_kafka_broker_toppar_ref_t *)ptr;
        ptr += counts.total_broker_toppars * sizeof(rd_kafka_broker_toppar_ref_t);

        req_ptr = (rd_kafka_req_count_t *)ptr;

        /* Get current time */
        now = rd_clock();

        /* Populate global stats */
        rd_kafka_stats_strcpy(stats->name, sizeof(stats->name), rk->rk_name);
        rd_kafka_stats_strcpy(stats->client_id, sizeof(stats->client_id),
                               rk->rk_conf.client_id_str);
        stats->type     = rk->rk_type;
        stats->ts_us    = now;
        stats->time_sec = (int64_t)time(NULL);
        stats->age_us   = now - rk->rk_ts_created;

        rd_kafka_curr_msgs_get(rk, &tot_cnt, &tot_size);
        stats->replyq             = rd_kafka_q_len(rk->rk_rep);
        stats->msg_cnt            = tot_cnt;
        stats->msg_size           = tot_size;
        stats->msg_max            = rk->rk_curr_msgs.max_cnt;
        stats->msg_size_max       = rk->rk_curr_msgs.max_size;
        stats->simple_cnt         = rd_atomic32_get(&rk->rk_simple_cnt);
        stats->metadata_cache_cnt = rk->rk_metadata_cache.rkmc_cnt;

        /* Initialize totals */
        stats->tx          = 0;
        stats->tx_bytes    = 0;
        stats->rx          = 0;
        stats->rx_bytes    = 0;
        stats->txmsgs      = 0;
        stats->txmsg_bytes = 0;
        stats->rxmsgs      = 0;
        stats->rxmsg_bytes = 0;

        /* Populate brokers */
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_kafka_stats_broker_populate(
                    &stats->brokers[broker_idx++], rkb, &toppar_ptr, &req_ptr,
                    now, &stats->tx, &stats->tx_bytes, &stats->rx,
                    &stats->rx_bytes);
        }

        /* Populate topics */
        TAILQ_FOREACH(rkt, &rk->rk_topics, rkt_link) {
                rd_kafka_stats_topic_populate(
                    &stats->topics[topic_idx++], rkt, &part_ptr, rk, now,
                    &stats->txmsgs, &stats->txmsg_bytes, &stats->rxmsgs,
                    &stats->rxmsg_bytes);
        }

        /* Consumer group stats */
        if (rk->rk_cgrp) {
                rd_kafka_cgrp_t *rkcg = rk->rk_cgrp;
                stats->has_cgrp      = 1;
                stats->cgrp.state    = rkcg->rkcg_state;
                stats->cgrp.stateage_us =
                    rkcg->rkcg_ts_statechange ? now - rkcg->rkcg_ts_statechange : 0;
                stats->cgrp.join_state = rkcg->rkcg_join_state;
                stats->cgrp.rebalance_age_us =
                    rkcg->rkcg_c.ts_rebalance ? now - rkcg->rkcg_c.ts_rebalance : 0;
                stats->cgrp.rebalance_cnt = rkcg->rkcg_c.rebalance_cnt;
                rd_kafka_stats_strcpy(stats->cgrp.rebalance_reason,
                                       sizeof(stats->cgrp.rebalance_reason),
                                       rkcg->rkcg_c.rebalance_reason);
                stats->cgrp.assignment_size = rkcg->rkcg_c.assignment_size;
        } else {
                stats->has_cgrp = 0;
        }

        /* EOS stats */
        if (rd_kafka_is_idempotent(rk)) {
                stats->has_eos              = 1;
                stats->eos.idemp_state      = rk->rk_eos.idemp_state;
                stats->eos.idemp_stateage_us = now - rk->rk_eos.ts_idemp_state;
                stats->eos.txn_state        = rk->rk_eos.txn_state;
                stats->eos.txn_stateage_us  = now - rk->rk_eos.ts_txn_state;
                stats->eos.txn_may_enq      = rd_atomic32_get(&rk->rk_eos.txn_may_enq) ? 1 : 0;
                stats->eos.producer_id      = rk->rk_eos.pid.id;
                stats->eos.producer_epoch   = rk->rk_eos.pid.epoch;
                stats->eos.epoch_cnt        = rk->rk_eos.epoch_cnt;
        } else {
                stats->has_eos = 0;
        }

        /* Fatal error */
        {
                rd_kafka_resp_err_t err = rd_atomic32_get(&rk->rk_fatal.err);
                if (err) {
                        stats->has_fatal   = 1;
                        stats->fatal_err   = err;
                        rd_kafka_stats_strcpy(stats->fatal_reason,
                                               sizeof(stats->fatal_reason),
                                               rk->rk_fatal.errstr);
                        stats->fatal_cnt = rk->rk_fatal.cnt;
                } else {
                        stats->has_fatal = 0;
                }
        }

        rd_kafka_rdunlock(rk);

        return stats;
}

/**
 * @brief Free a stats structure.
 */
void rd_kafka_stats_destroy(rd_kafka_stats_t *stats) {
        if (stats)
                rd_free(stats);
}
