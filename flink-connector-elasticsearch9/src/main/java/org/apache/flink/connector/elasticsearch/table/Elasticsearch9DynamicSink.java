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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch9AsyncSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.hc.core5.http.HttpHost;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSink} for Elasticsearch 9. */
@Internal
class Elasticsearch9DynamicSink implements DynamicTableSink {

    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final Elasticsearch9Configuration config;
    private final List<PrimaryKeyInfo> primaryKeys;
    private final DataType physicalRowDataType;

    Elasticsearch9DynamicSink(
            EncodingFormat<SerializationSchema<RowData>> format,
            Elasticsearch9Configuration config,
            List<PrimaryKeyInfo> primaryKeys,
            DataType physicalRowDataType) {
        this.format = checkNotNull(format);
        this.config = checkNotNull(config);
        this.primaryKeys = checkNotNull(primaryKeys);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                format.createRuntimeEncoder(context, physicalRowDataType);

        RowDataToBulkOperationConverter converter =
                new RowDataToBulkOperationConverter(
                        config.getIndex(),
                        serializationSchema,
                        primaryKeys,
                        config.getKeyDelimiter(),
                        DataType.getFieldNames(physicalRowDataType));

        Elasticsearch9AsyncSinkBuilder<RowData> builder =
                Elasticsearch9AsyncSinkBuilder.<RowData>builder()
                        .setHosts(
                                config.getHosts().toArray(new HttpHost[0]))
                        .setElementConverter(converter)
                        .setMaxBatchSize(config.getBulkFlushMaxActions())
                        .setMaxBatchSizeInBytes(config.getBulkFlushMaxByteSize().getBytes())
                        .setMaxTimeInBufferMS(config.getBulkFlushIntervalMs());

        config.getUsername().ifPresent(builder::setUsername);
        config.getPassword().ifPresent(builder::setPassword);
        config.getDeadLetterIndex().ifPresent(builder::setDeadLetterIndex);
        config.getCertificateFingerprint().ifPresent(builder::setCertificateFingerprint);

        if (config.isAllowInsecure()) {
            builder.allowInsecure();
        }

        return SinkV2Provider.of(
                builder.build(), config.getParallelism().orElse(null));
    }

    @Override
    public DynamicTableSink copy() {
        return new Elasticsearch9DynamicSink(format, config, primaryKeys, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch-9";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Elasticsearch9DynamicSink that = (Elasticsearch9DynamicSink) o;
        return Objects.equals(format, that.format)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(primaryKeys, that.primaryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, physicalRowDataType, primaryKeys);
    }
}
