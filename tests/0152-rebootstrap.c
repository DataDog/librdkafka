/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2025, Confluent Inc.
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

/**
 * @brief Verify the case where there are no bootstrap servers
 *        and the client is re-bootstrapped after brokers were added
 *        manually.
 */
static void
do_test_rebootstrap_local_no_bootstrap_servers(rd_kafka_type_t rk_type,
                                               const char *engine_name) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;

        if (rk_type == RD_KAFKA_PRODUCER)
                SUB_TEST_QUICK("producer produce.engine=%s", engine_name);
        else
                SUB_TEST_QUICK("consumer");
        test_conf_init(&conf, NULL, 30);
        if (rk_type == RD_KAFKA_PRODUCER)
                test_conf_set(conf, "produce.engine", engine_name);
        rk = test_create_handle(rk_type, conf);
        rd_kafka_brokers_add(rk, "localhost:9999");

        /* Give it time to trigger ALL_BROKERS_DOWN */
        rd_sleep(1);
        rd_kafka_destroy(rk);
        SUB_TEST_PASS();
}

int main_0152_rebootstrap_local(int argc, char **argv) {
        const char *engine_names[] = {"v1", "v2"};
        size_t i;

        for (i = 0; i < RD_ARRAYSIZE(engine_names); i++)
                do_test_rebootstrap_local_no_bootstrap_servers(
                    RD_KAFKA_PRODUCER, engine_names[i]);
        do_test_rebootstrap_local_no_bootstrap_servers(RD_KAFKA_CONSUMER, NULL);

        return 0;
}
