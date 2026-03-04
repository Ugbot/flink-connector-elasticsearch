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

import java.io.Serializable;

/**
 * Handler for individual bulk item failures in Elasticsearch responses.
 *
 * <p>When a bulk request to Elasticsearch returns partial failures, each failed item is evaluated by
 * this handler to determine the appropriate action: retry, drop, or fail the job.
 *
 * <p>Built-in implementations:
 *
 * <ul>
 *   <li>{@link DefaultBulkItemFailureHandler} - Retries transient errors (429, 5xx), fails on
 *       others
 *   <li>{@link IgnoringBulkItemFailureHandler} - Drops all non-retryable failures silently
 * </ul>
 */
@PublicEvolving
@FunctionalInterface
public interface BulkItemFailureHandler extends Serializable {

    /** The action to take for a failed bulk item. */
    enum Decision {
        /** Re-enqueue the item for retry. */
        RETRY,
        /** Drop the item (will be sent to DLQ if configured, otherwise discarded). */
        DROP,
        /** Fail the Flink job immediately. */
        FAIL
    }

    /**
     * Determine how to handle a failed bulk item.
     *
     * @param statusCode the HTTP status code from Elasticsearch (e.g. 400, 429, 500)
     * @param errorType the error type string from Elasticsearch (e.g.
     *     "mapper_parsing_exception")
     * @param errorReason the human-readable error reason from Elasticsearch
     * @param failedOperation the operation that failed
     * @return the decision for how to handle this failure
     */
    Decision onItemFailure(
            int statusCode, String errorType, String errorReason, Operation failedOperation);
}
