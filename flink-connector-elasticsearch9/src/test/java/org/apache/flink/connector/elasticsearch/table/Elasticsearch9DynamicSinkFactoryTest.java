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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.table.factories.Factory;

import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link Elasticsearch9DynamicSinkFactory}. */
class Elasticsearch9DynamicSinkFactoryTest {

    @Test
    void testFactoryIdentifier() {
        Elasticsearch9DynamicSinkFactory factory = new Elasticsearch9DynamicSinkFactory();
        assertEquals("elasticsearch-9", factory.factoryIdentifier());
    }

    @Test
    void testRequiredOptions() {
        Elasticsearch9DynamicSinkFactory factory = new Elasticsearch9DynamicSinkFactory();
        assertTrue(factory.requiredOptions().stream().anyMatch(o -> o.key().equals("hosts")));
        assertTrue(factory.requiredOptions().stream().anyMatch(o -> o.key().equals("index")));
    }

    @Test
    void testOptionalOptionsContainDlq() {
        Elasticsearch9DynamicSinkFactory factory = new Elasticsearch9DynamicSinkFactory();
        assertTrue(
                factory.optionalOptions().stream()
                        .anyMatch(o -> o.key().equals("sink.dead-letter-index")));
    }

    @Test
    void testSpiDiscovery() {
        ServiceLoader<Factory> loader = ServiceLoader.load(Factory.class);
        boolean found = false;
        for (Factory factory : loader) {
            if (factory instanceof Elasticsearch9DynamicSinkFactory) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Elasticsearch9DynamicSinkFactory should be discoverable via SPI");
    }
}
