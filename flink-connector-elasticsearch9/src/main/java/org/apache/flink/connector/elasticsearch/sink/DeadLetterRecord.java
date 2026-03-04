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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a failed document that will be written to the Dead Letter Queue index.
 *
 * <p>Captures the original operation details alongside error information for debugging and
 * reprocessing.
 */
class DeadLetterRecord {

    private final String timestamp;
    private final String originalIndex;
    private final int errorStatus;
    private final String errorType;
    private final String errorReason;
    private final String operationType;

    DeadLetterRecord(
            String originalIndex,
            int errorStatus,
            String errorType,
            String errorReason,
            String operationType) {
        this.timestamp = Instant.now().toString();
        this.originalIndex = originalIndex;
        this.errorStatus = errorStatus;
        this.errorType = errorType;
        this.errorReason = errorReason;
        this.operationType = operationType;
    }

    Map<String, Object> toMap() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("@timestamp", timestamp);

        Map<String, Object> error = new LinkedHashMap<>();
        error.put("type", errorType);
        error.put("reason", errorReason);
        error.put("status", errorStatus);
        doc.put("error", error);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("index", originalIndex);
        source.put("operation_type", operationType);
        doc.put("source", source);

        return doc;
    }
}
