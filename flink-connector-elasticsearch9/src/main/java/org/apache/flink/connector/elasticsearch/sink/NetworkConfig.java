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

import org.apache.flink.util.function.SerializableSupplier;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** A factory that creates valid ElasticsearchClient instances for Elasticsearch 9.x. */
public class NetworkConfig implements Serializable {
    private final List<HttpHost> hosts;

    private final List<Header> headers;

    private final String username;

    private final String password;

    @Nullable private final SerializableSupplier<SSLContext> sslContextSupplier;

    public NetworkConfig(
            List<HttpHost> hosts,
            String username,
            String password,
            List<Header> headers,
            SerializableSupplier<SSLContext> sslContextSupplier) {
        checkState(!hosts.isEmpty(), "Hosts must not be empty");
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.headers = headers;
        this.sslContextSupplier = sslContextSupplier;
    }

    public ElasticsearchAsyncClient createEsClient() {
        return new ElasticsearchAsyncClient(
                new Rest5ClientTransport(this.getRest5Client(), new JacksonJsonpMapper()));
    }

    private Rest5Client getRest5Client() {
        Rest5ClientBuilder builder =
                Rest5Client.builder(hosts.toArray(new HttpHost[0]));

        List<Header> allHeaders = new ArrayList<>();

        if (username != null && password != null) {
            String credentials =
                    Base64.getEncoder()
                            .encodeToString(
                                    (username + ":" + password)
                                            .getBytes(StandardCharsets.UTF_8));
            allHeaders.add(new BasicHeader("Authorization", "Basic " + credentials));
        }

        if (headers != null) {
            allHeaders.addAll(headers);
        }

        if (!allHeaders.isEmpty()) {
            builder.setDefaultHeaders(allHeaders.toArray(new Header[0]));
        }

        if (sslContextSupplier != null) {
            builder.setSSLContext(sslContextSupplier.get());
        }

        return builder.build();
    }
}
