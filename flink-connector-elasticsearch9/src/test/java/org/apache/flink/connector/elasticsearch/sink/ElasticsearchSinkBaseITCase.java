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

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Base Integration tests class for Elasticsearch 9. */
@Testcontainers
public class ElasticsearchSinkBaseITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkBaseITCase.class);

    public Rest5Client client;

    public static final String ELASTICSEARCH_VERSION = "9.0.2";

    public static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                    .withTag(ELASTICSEARCH_VERSION);

    @Container
    public static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();

    public static ElasticsearchContainer createElasticsearchContainer() {
        try (ElasticsearchContainer container =
                new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
                        .withEnv("xpack.security.enabled", "false"); ) {
            container
                    .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                    .withEnv("logger.org.elasticsearch", "ERROR")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

            container.setWaitStrategy(
                    Wait.defaultWaitStrategy().withStartupTimeout(Duration.ofMinutes(5)));

            return container;
        }
    }

    public Rest5Client getRest5Client() {
        return Rest5Client.builder(
                        new HttpHost(
                                "http",
                                ES_CONTAINER.getHost(),
                                ES_CONTAINER.getFirstMappedPort()))
                .build();
    }

    public static void assertIdsAreWritten(Rest5Client client, String index, String[] ids)
            throws IOException, ParseException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());

        assertThat(response.getStatusCode()).isEqualTo(200);

        for (String id : ids) {
            System.out.println(id);
            assertThat(responseEntity).contains(id);
        }
    }

    public static void assertIdsAreNotWritten(Rest5Client client, String index, String[] ids)
            throws IOException, ParseException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());

        assertThat(response.getStatusCode()).isEqualTo(200);

        for (String id : ids) {
            assertThat(responseEntity).doesNotContain(id);
        }
    }

    /** DummyData is a POJO to helping during integration tests. */
    public static class DummyData {
        private final String id;

        private final String name;

        public DummyData(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
