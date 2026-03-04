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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBaseITCase.DummyData;
import static org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBaseITCase.ELASTICSEARCH_IMAGE;
import static org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBaseITCase.assertIdsAreWritten;

/** Integration tests for {@link Elasticsearch9AsyncSink} against a secure Elasticsearch cluster. */
@Testcontainers
class Elasticsearch9AsyncSinkSecureITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch9AsyncSinkSecureITCase.class);
    private static final String ES_CLUSTER_USERNAME = "elastic";
    private static final String ES_CLUSTER_PASSWORD = "s3cret";

    @Container
    private static final ElasticsearchContainer ES_CONTAINER = createSecureElasticsearchContainer();

    private Rest5Client client;

    @BeforeEach
    void setUp() {
        this.client = getRest5Client();
    }

    @AfterEach
    void shutdown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testWriteToSecureElasticsearch9() throws Exception {
        final String index = "test-write-to-secure-elasticsearch9";

        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1)) {

            env.setRestartStrategy(RestartStrategies.noRestart());

            final Elasticsearch9AsyncSink<DummyData> sink =
                    Elasticsearch9AsyncSinkBuilder.<DummyData>builder()
                            .setMaxBatchSize(5)
                            .setHosts(
                                    new HttpHost(
                                            "https",
                                            ES_CONTAINER.getHost(),
                                            ES_CONTAINER.getFirstMappedPort()))
                            .setElementConverter(
                                    (element, ctx) ->
                                            new IndexOperation.Builder<>()
                                                    .index(index)
                                                    .id(element.getId())
                                                    .document(element)
                                                    .build())
                            .setUsername(ES_CLUSTER_USERNAME)
                            .setPassword(ES_CLUSTER_PASSWORD)
                            .setSslContextSupplier(() -> ES_CONTAINER.createSslContextFromCa())
                            .build();

            env.fromElements("first", "second", "third", "fourth", "fifth")
                    .map(
                            (MapFunction<String, DummyData>)
                                    value -> new DummyData(value + "_v1_index", value))
                    .sinkTo(sink);

            env.execute();
        }

        assertIdsAreWritten(client, index, new String[] {"first_v1_index", "second_v1_index"});
    }

    static ElasticsearchContainer createSecureElasticsearchContainer() {
        ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withPassword(ES_CLUSTER_PASSWORD) /* set password */
                        .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        // Set log message based wait strategy as the default wait strategy is not aware of TLS
        container
                .withEnv("logger.org.elasticsearch", "INFO")
                .setWaitStrategy(
                        new LogMessageWaitStrategy()
                                .withRegEx(".*\"message\":\"started.*")
                                .withStartupTimeout(Duration.ofMinutes(5)));

        return container;
    }

    private Rest5Client getRest5Client() {
        String credentials =
                Base64.getEncoder()
                        .encodeToString(
                                (ES_CLUSTER_USERNAME + ":" + ES_CLUSTER_PASSWORD)
                                        .getBytes(StandardCharsets.UTF_8));
        return Rest5Client.builder(
                        new HttpHost(
                                "https",
                                ES_CONTAINER.getHost(),
                                ES_CONTAINER.getFirstMappedPort()))
                .setDefaultHeaders(
                        new org.apache.hc.core5.http.Header[] {
                            new BasicHeader("Authorization", "Basic " + credentials)
                        })
                .setSSLContext(ES_CONTAINER.createSslContextFromCa())
                .build();
    }
}
