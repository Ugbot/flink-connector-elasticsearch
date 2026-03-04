/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.sink;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link DefaultBulkItemFailureHandler} and {@link IgnoringBulkItemFailureHandler}. */
class BulkItemFailureHandlerTest {

    private final Operation dummyOp =
            new Operation(IndexOperation.of(op -> op.index("test").document(Map.of("key", "val"))));

    @Test
    void defaultHandler_retriesOn429() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(429, "too_many_requests", "rate limited", dummyOp));
    }

    @Test
    void defaultHandler_retriesOn500() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(500, "internal_server_error", "internal error", dummyOp));
    }

    @Test
    void defaultHandler_retriesOn503() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(503, "service_unavailable", "unavailable", dummyOp));
    }

    @Test
    void defaultHandler_retriesOnRejectedExecution() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(
                        429,
                        "es_rejected_execution_exception",
                        "rejected execution",
                        dummyOp));
    }

    @Test
    void defaultHandler_retriesOnCircuitBreaking() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(
                        429, "circuit_breaking_exception", "circuit broken", dummyOp));
    }

    @Test
    void defaultHandler_failsOn400() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.FAIL,
                handler.onItemFailure(
                        400, "mapper_parsing_exception", "mapping error", dummyOp));
    }

    @Test
    void defaultHandler_failsOn404() {
        DefaultBulkItemFailureHandler handler = new DefaultBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.FAIL,
                handler.onItemFailure(404, "index_not_found_exception", "not found", dummyOp));
    }

    @Test
    void ignoringHandler_dropsOn400() {
        IgnoringBulkItemFailureHandler handler = new IgnoringBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.DROP,
                handler.onItemFailure(
                        400, "mapper_parsing_exception", "mapping error", dummyOp));
    }

    @Test
    void ignoringHandler_retriesOn429() {
        IgnoringBulkItemFailureHandler handler = new IgnoringBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(429, "too_many_requests", "rate limited", dummyOp));
    }

    @Test
    void ignoringHandler_retriesOn5xx() {
        IgnoringBulkItemFailureHandler handler = new IgnoringBulkItemFailureHandler();
        assertEquals(
                BulkItemFailureHandler.Decision.RETRY,
                handler.onItemFailure(502, "bad_gateway", "bad gateway", dummyOp));
    }
}
