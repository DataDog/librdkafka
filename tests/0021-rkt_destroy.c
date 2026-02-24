/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
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

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */


/**
 * Issue #502
 * Crash if rd_kafka_topic_destroy() is called before all messages
 * have been produced.
 * This only happens when using a partitioner (producing to PARTITION_UA)
 */



static void test_rkt_destroy_engine(const char *engine_name) {
        const char *topic = test_mk_topic_name(__FUNCTION__, 0);
        rd_kafka_t *rk;
        rd_kafka_topic_t *rkt;
        rd_kafka_conf_t *pconf;
        const int msgcnt = 1000;
        uint64_t testid;
        int remains = 0;

        SUB_TEST("%s", engine_name);

        test_conf_init(&pconf, NULL, 10);
        test_conf_set(pconf, "produce.engine", engine_name);

        testid = test_id_generate();
        rk     = test_create_handle(RD_KAFKA_PRODUCER, pconf);
        rkt    = test_create_producer_topic(rk, topic, NULL);
        test_wait_topic_exists(rk, topic, 5000);


        test_produce_msgs_nowait(rk, rkt, testid, RD_KAFKA_PARTITION_UA, 0,
                                 msgcnt, NULL, 0, 0, &remains);

        rd_kafka_topic_destroy(rkt);

        test_wait_delivery(rk, &remains);

        rd_kafka_destroy(rk);

        SUB_TEST_PASS();
}

int main_0021_rkt_destroy(int argc, char **argv) {
        test_rkt_destroy_engine("v1");
        test_rkt_destroy_engine("v2");

        return 0;
}
