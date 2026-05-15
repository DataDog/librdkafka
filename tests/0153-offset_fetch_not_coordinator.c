/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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

#include "test.h"

#include "../src/rdkafka_protocol.h"


static void assert_offset_fetch_retries_require_coord_query(
    rd_kafka_mock_cluster_t *mcluster,
    int num_errs) {
        rd_kafka_mock_request_t **requests;
        size_t request_cnt;
        int offset_fetch_cnt          = 0;
        int find_coord_cnt            = 0;
        int finds_since_offset_fetch  = 0;
        rd_bool_t seen_offset_fetch   = rd_false;
        rd_bool_t relevant_req_logged = rd_false;
        size_t i;

        requests = rd_kafka_mock_get_requests(mcluster, &request_cnt);

        TEST_SAY("Relevant request sequence after assign:");
        for (i = 0; i < request_cnt; i++) {
                int16_t api_key = rd_kafka_mock_request_api_key(requests[i]);

                if (api_key != RD_KAFKAP_OffsetFetch &&
                    api_key != RD_KAFKAP_FindCoordinator)
                        continue;

                TEST_SAY("%s%s", relevant_req_logged ? " -> " : " ",
                         api_key == RD_KAFKAP_OffsetFetch ? "OffsetFetch"
                                                           : "FindCoordinator");
                relevant_req_logged = rd_true;

                if (api_key == RD_KAFKAP_FindCoordinator) {
                        find_coord_cnt++;
                        if (seen_offset_fetch)
                                finds_since_offset_fetch++;
                        continue;
                }

                offset_fetch_cnt++;
                if (seen_offset_fetch) {
                        TEST_ASSERT(finds_since_offset_fetch > 0,
                                    "OffsetFetch retry #%d was issued without "
                                    "a preceding FindCoordinator request",
                                    offset_fetch_cnt);
                }

                seen_offset_fetch          = rd_true;
                finds_since_offset_fetch   = 0;
        }
        TEST_SAY("\n");

        TEST_ASSERT(offset_fetch_cnt == num_errs + 1,
                    "Expected %d OffsetFetch request(s), got %d",
                    num_errs + 1, offset_fetch_cnt);

        TEST_ASSERT(find_coord_cnt >= num_errs,
                    "Expected at least %d FindCoordinator request(s), got %d",
                    num_errs, find_coord_cnt);

        rd_kafka_mock_request_destroy_array(requests, request_cnt);
}


/**
 * @brief Regression test: static assign() + OFFSET_STORED + OffsetFetch
 *        returning request-level NOT_COORDINATOR must recover, not hang.
 *
 *  - Before the fix: the OffsetFetch error handler emitted only a soft
 *    RD_KAFKA_OP_COORD_QUERY for a NOT_COORDINATOR response. The
 *    follow-up FindCoordinator typically returned the same broker, so
 *    rd_kafka_cgrp_coord_update() short-circuited, the cgrp stayed in
 *    STATE_UP, and the queried partitions were left orphaned on
 *    .queried — rd_kafka_assignment_serve_pending() short-circuited
 *    forever on `.queried->cnt == 0`. Consumers using static
 *    rd_kafka_assign() without rd_kafka_subscribe() (no heartbeat path
 *    to nudge the coord state) hung indefinitely.
 *
 *  - After the fix: NOT_COORDINATOR escalates to coord_dead (clearing
 *    coord_id and curr_coord), and the queried partitions are moved back
 *    to .pending. The cgrp transitions QUERY_COORD → WAIT_BROKER_TRANSPORT
 *    → UP, which calls rd_kafka_assignment_serve() and re-issues the
 *    OffsetFetch, picking up the committed offset.
 */
static void do_test_static_assign_with_stored_offset(
    rd_kafka_resp_err_t err_to_inject,
    int num_errs) {
        const char *bootstraps;
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        rd_kafka_t *c;
        rd_kafka_topic_partition_list_t *parts;
        rd_kafka_topic_partition_t *rktpar;
        const char *topic   = "test-not-coord";
        const char *groupid = "g-not-coord";
        const int msgcnt    = 100;
        const int64_t committed_offset = msgcnt / 2;
        rd_kafka_topic_partition_list_t *commit_offsets;
        int received = 0;
        int64_t first_offset = -1;
        test_timing_t timing;
        int i;

        SUB_TEST_QUICK("err=%s num_errs=%d", rd_kafka_err2name(err_to_inject),
                       num_errs);

        mcluster = test_mock_cluster_new(3, &bootstraps);
        rd_kafka_mock_coordinator_set(mcluster, "group", groupid, 1);

        /* Seed the topic with messages */
        test_produce_msgs_easy_v(topic, 0, 0, 0, msgcnt, 10,
                                 "bootstrap.servers", bootstraps,
                                 "batch.num.messages", "10", NULL);

        test_conf_init(&conf, NULL, 60);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "security.protocol", "PLAINTEXT");
        test_conf_set(conf, "group.id", groupid);
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");
        /* Low coord query interval so the periodic cgrp coordinator-query
         * timer fires quickly within the test deadline. */
        test_conf_set(conf, "coordinator.query.interval.ms", "2000");

        c = test_create_consumer(groupid, NULL, conf, NULL);

        /* Commit a known starting offset for partition 0 (no subscribe()
         * needed — rd_kafka_commit() with explicit offsets is enough). */
        commit_offsets = rd_kafka_topic_partition_list_new(1);
        rktpar = rd_kafka_topic_partition_list_add(commit_offsets, topic, 0);
        rktpar->offset = committed_offset;
        TEST_CALL_ERR__(rd_kafka_commit(c, commit_offsets, 0 /*sync*/));
        rd_kafka_topic_partition_list_destroy(commit_offsets);

        rd_kafka_mock_start_request_tracking(mcluster);

        /* Inject the request-level coordinator error onto the next
         * OffsetFetch requests (these are the ones the static assign() path
         * triggers via rd_kafka_assignment_serve_pending). */
        for (i = 0; i < num_errs; i++) {
                rd_kafka_mock_push_request_errors(mcluster,
                                                  RD_KAFKAP_OffsetFetch, 1,
                                                  err_to_inject);
        }

        /* Static assign — never call subscribe(). Use OFFSET_STORED so
         * librdkafka has to issue an OffsetFetch to discover the starting
         * position. */
        parts = rd_kafka_topic_partition_list_new(1);
        rktpar = rd_kafka_topic_partition_list_add(parts, topic, 0);
        rktpar->offset = RD_KAFKA_OFFSET_STORED;
        test_consumer_assign("static assign with OFFSET_STORED", c, parts);
        rd_kafka_topic_partition_list_destroy(parts);

        /* Poll up to 30s and expect to receive the messages from the
         * committed offset onwards. Without the fix this loop times out
         * with zero messages received because the partition is orphaned
         * on assignment.queried. */
        TIMING_START(&timing, "poll-after-not-coordinator");
        while (received < msgcnt - committed_offset &&
               TIMING_DURATION(&timing) < 30 * 1000 * 1000) {
                rd_kafka_message_t *rkm =
                    rd_kafka_consumer_poll(c, 1000);
                if (!rkm)
                        continue;
                if (rkm->err) {
                        TEST_SAY("poll returned event: %s\n",
                                 rd_kafka_err2str(rkm->err));
                        rd_kafka_message_destroy(rkm);
                        continue;
                }
                if (first_offset < 0)
                        first_offset = rkm->offset;
                received++;
                rd_kafka_message_destroy(rkm);
        }
        TIMING_STOP(&timing);

        TEST_ASSERT(received >= msgcnt - committed_offset,
                    "Expected to consume at least %d message(s) after "
                    "recovery from %s, got %d. Consumer is stuck.",
                    msgcnt - (int)committed_offset,
                    rd_kafka_err2name(err_to_inject), received);

        TEST_ASSERT(first_offset == committed_offset,
                    "Expected first received offset to be %" PRId64
                    " (the committed offset), got %" PRId64,
                    committed_offset, first_offset);

        assert_offset_fetch_retries_require_coord_query(mcluster, num_errs);

        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0153_offset_fetch_not_coordinator(int argc, char **argv) {
        TEST_SKIP_MOCK_CLUSTER(0);

        /* Only the classic protocol path is affected: KIP-848 uses a
         * different rebalance flow and runs heartbeats unconditionally. */
        if (!test_consumer_group_protocol_classic()) {
                TEST_SKIP("Test only runs with classic consumer protocol\n");
                return 0;
        }

        /* Normal OFFSET_STORED path without injected coordinator errors. */
        do_test_static_assign_with_stored_offset(RD_KAFKA_RESP_ERR_NO_ERROR, 0);

        /* Primary scenario from the incident: NOT_COORDINATOR. */
        do_test_static_assign_with_stored_offset(
            RD_KAFKA_RESP_ERR_NOT_COORDINATOR, 3);

        /* The equivalent coordinator-class error must also recover. */
        do_test_static_assign_with_stored_offset(
            RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE, 3);

        return 0;
}
