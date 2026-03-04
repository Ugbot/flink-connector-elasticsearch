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
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.JsonData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.StringJoiner;

/**
 * Converts Flink {@link RowData} to Elasticsearch 9 {@link BulkOperationVariant} for the table
 * sink.
 *
 * <p>Handles INSERT, UPDATE_AFTER, and DELETE operations by creating the appropriate
 * IndexOperation or DeleteOperation.
 */
@Internal
class RowDataToBulkOperationConverter
        implements ElementConverter<RowData, BulkOperationVariant>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String index;
    private final SerializationSchema<RowData> serializationSchema;
    private final List<PrimaryKeyInfo> primaryKeys;
    private final String keyDelimiter;
    private final List<String> fieldNames;

    private transient boolean opened = false;

    RowDataToBulkOperationConverter(
            String index,
            SerializationSchema<RowData> serializationSchema,
            List<PrimaryKeyInfo> primaryKeys,
            String keyDelimiter,
            List<String> fieldNames) {
        this.index = index;
        this.serializationSchema = serializationSchema;
        this.primaryKeys = primaryKeys;
        this.keyDelimiter = keyDelimiter;
        this.fieldNames = fieldNames;
    }

    @Override
    public BulkOperationVariant apply(RowData element, SinkWriter.Context context) {
        if (!opened) {
            try {
                serializationSchema.open(null);
            } catch (Exception e) {
                throw new RuntimeException("Failed to open serialization schema", e);
            }
            opened = true;
        }

        RowKind kind = element.getRowKind();
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                return createUpsert(element);
            case DELETE:
                return createDelete(element);
            case UPDATE_BEFORE:
                return null;
            default:
                throw new UnsupportedOperationException("Unsupported row kind: " + kind);
        }
    }

    private BulkOperationVariant createUpsert(RowData row) {
        byte[] document = serializationSchema.serialize(row);
        String key = extractKey(row);

        return IndexOperation.of(
                op -> {
                    op.index(index);
                    if (key != null) {
                        op.id(key);
                    }
                    op.document(JsonData.fromJson(new String(document, java.nio.charset.StandardCharsets.UTF_8)));
                    return op;
                });
    }

    private BulkOperationVariant createDelete(RowData row) {
        String key = extractKey(row);
        if (key == null) {
            throw new IllegalStateException(
                    "DELETE operation requires a primary key but no primary key was defined.");
        }

        return DeleteOperation.of(op -> op.index(index).id(key));
    }

    @Nullable
    private String extractKey(RowData row) {
        if (primaryKeys.isEmpty()) {
            return null;
        }

        StringJoiner joiner = new StringJoiner(keyDelimiter);
        for (PrimaryKeyInfo pkInfo : primaryKeys) {
            String value = formatField(row, pkInfo.index, pkInfo.logicalType);
            joiner.add(value);
        }
        return joiner.toString();
    }

    private String formatField(RowData row, int index, LogicalType type) {
        if (row.isNullAt(index)) {
            return "null";
        }

        LogicalTypeRoot root = type.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
                return row.getString(index).toString();
            case BOOLEAN:
                return String.valueOf(row.getBoolean(index));
            case TINYINT:
                return String.valueOf(row.getByte(index));
            case SMALLINT:
                return String.valueOf(row.getShort(index));
            case INTEGER:
                return String.valueOf(row.getInt(index));
            case BIGINT:
                return String.valueOf(row.getLong(index));
            case FLOAT:
                return String.valueOf(row.getFloat(index));
            case DOUBLE:
                return String.valueOf(row.getDouble(index));
            case DECIMAL:
                int precision = ((org.apache.flink.table.types.logical.DecimalType) type).getPrecision();
                int scale = ((org.apache.flink.table.types.logical.DecimalType) type).getScale();
                return row.getDecimal(index, precision, scale).toBigDecimal().toPlainString();
            case DATE:
                return LocalDate.ofEpochDay(row.getInt(index)).toString();
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay((long) row.getInt(index) * 1_000_000L)
                        .format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return row.getTimestamp(index, 3).toInstant().toString();
            case INTERVAL_YEAR_MONTH:
                return String.valueOf(row.getInt(index));
            case INTERVAL_DAY_TIME:
                return String.valueOf(row.getLong(index));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported primary key type: " + type);
        }
    }
}
