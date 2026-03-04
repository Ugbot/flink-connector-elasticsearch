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

import org.apache.flink.annotation.PublicEvolving;

import java.util.Set;

/**
 * Failure handler that silently drops all non-retryable failures.
 *
 * <p>Transient errors (429, 5xx, thread pool saturation, circuit breaking) are still retried.
 * Everything else is dropped without failing the job.
 *
 * <p>Equivalent to the ES7 connector's {@code IgnoringFailureHandler}.
 */
@PublicEvolving
public class IgnoringBulkItemFailureHandler implements BulkItemFailureHandler {

    private static final long serialVersionUID = 1L;

    private static final Set<String> RETRYABLE_ERROR_TYPES =
            Set.of(
                    "es_rejected_execution_exception",
                    "circuit_breaking_exception",
                    "too_many_requests");

    @Override
    public Decision onItemFailure(
            int statusCode, String errorType, String errorReason, Operation failedOperation) {
        if (statusCode == 429) {
            return Decision.RETRY;
        }
        if (statusCode >= 500 && statusCode <= 599) {
            return Decision.RETRY;
        }
        if (errorType != null && RETRYABLE_ERROR_TYPES.contains(errorType)) {
            return Decision.RETRY;
        }
        return Decision.DROP;
    }
}
