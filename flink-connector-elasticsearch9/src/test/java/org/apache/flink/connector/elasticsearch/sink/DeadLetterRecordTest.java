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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Tests for {@link DeadLetterRecord}. */
class DeadLetterRecordTest {

    @Test
    @SuppressWarnings("unchecked")
    void testToMap() {
        DeadLetterRecord record =
                new DeadLetterRecord(
                        "my-index", 400, "mapper_parsing_exception", "field type mismatch", "index");

        Map<String, Object> map = record.toMap();

        assertNotNull(map.get("@timestamp"));

        Map<String, Object> error = (Map<String, Object>) map.get("error");
        assertEquals("mapper_parsing_exception", error.get("type"));
        assertEquals("field type mismatch", error.get("reason"));
        assertEquals(400, error.get("status"));

        Map<String, Object> source = (Map<String, Object>) map.get("source");
        assertEquals("my-index", source.get("index"));
        assertEquals("index", source.get("operation_type"));
    }
}
