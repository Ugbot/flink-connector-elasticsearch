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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import org.apache.hc.core5.http.HttpHost;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.ALLOW_INSECURE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.BULK_FLUSH_MAX_SIZE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.CERTIFICATE_FINGERPRINT_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.DEAD_LETTER_INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch9DynamicSinkFactory.USERNAME_OPTION;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Configuration for the Elasticsearch 9 table sink. */
@Internal
class Elasticsearch9Configuration {

    private final ReadableConfig config;

    Elasticsearch9Configuration(ReadableConfig config) {
        this.config = config;
    }

    List<HttpHost> getHosts() {
        return config.get(HOSTS_OPTION).stream()
                .map(
                        host -> {
                            try {
                                return HttpHost.create(host);
                            } catch (java.net.URISyntaxException e) {
                                throw new IllegalArgumentException(
                                        "Invalid Elasticsearch host: " + host, e);
                            }
                        })
                .collect(Collectors.toList());
    }

    String getIndex() {
        return config.get(INDEX_OPTION);
    }

    Optional<String> getUsername() {
        return config.getOptional(USERNAME_OPTION);
    }

    Optional<String> getPassword() {
        return config.getOptional(PASSWORD_OPTION);
    }

    String getKeyDelimiter() {
        return config.get(KEY_DELIMITER_OPTION);
    }

    int getBulkFlushMaxActions() {
        return config.get(BULK_FLUSH_MAX_ACTIONS_OPTION);
    }

    MemorySize getBulkFlushMaxByteSize() {
        return config.get(BULK_FLUSH_MAX_SIZE_OPTION);
    }

    long getBulkFlushIntervalMs() {
        return config.get(BULK_FLUSH_INTERVAL_OPTION).toMillis();
    }

    Optional<String> getDeadLetterIndex() {
        return config.getOptional(DEAD_LETTER_INDEX_OPTION);
    }

    boolean isAllowInsecure() {
        return config.get(ALLOW_INSECURE_OPTION);
    }

    Optional<String> getCertificateFingerprint() {
        return config.getOptional(CERTIFICATE_FINGERPRINT_OPTION);
    }

    Optional<Integer> getParallelism() {
        return config.getOptional(SINK_PARALLELISM);
    }
}
