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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.StringUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** Factory for creating Elasticsearch 9 table sinks. */
@Internal
public class Elasticsearch9DynamicSinkFactory implements DynamicTableSinkFactory {

    public static final String FACTORY_IDENTIFIER = "elasticsearch-9";

    public static final ConfigOption<List<String>> HOSTS_OPTION =
            ConfigOptions.key("hosts")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Elasticsearch hosts to connect to.");

    public static final ConfigOption<String> INDEX_OPTION =
            ConfigOptions.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Elasticsearch index for every record.");

    public static final ConfigOption<String> USERNAME_OPTION =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username used to connect to Elasticsearch instance.");

    public static final ConfigOption<String> PASSWORD_OPTION =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password used to connect to Elasticsearch instance.");

    public static final ConfigOption<String> KEY_DELIMITER_OPTION =
            ConfigOptions.key("document-id.key-delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter for composite keys e.g., \"$\" would result in IDs \"KEY1$KEY2$KEY3\".");

    public static final ConfigOption<Integer> BULK_FLUSH_MAX_ACTIONS_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-actions")
                    .intType()
                    .defaultValue(500)
                    .withDescription(
                            "Maximum number of actions to buffer for each bulk request.");

    public static final ConfigOption<MemorySize> BULK_FLUSH_MAX_SIZE_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("5mb"))
                    .withDescription("Maximum size of buffered actions per bulk request.");

    public static final ConfigOption<Duration> BULK_FLUSH_INTERVAL_OPTION =
            ConfigOptions.key("sink.bulk-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("Bulk flush interval.");

    public static final ConfigOption<String> FORMAT_OPTION =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "The format must produce a valid JSON document.");

    public static final ConfigOption<String> DEAD_LETTER_INDEX_OPTION =
            ConfigOptions.key("sink.dead-letter-index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Elasticsearch index to write non-retryable failures to (Dead Letter Queue). Disabled by default.");

    public static final ConfigOption<Boolean> ALLOW_INSECURE_OPTION =
            ConfigOptions.key("connection.allow-insecure")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow insecure HTTPS connections (skip certificate validation).");

    public static final ConfigOption<String> CERTIFICATE_FINGERPRINT_OPTION =
            ConfigOptions.key("connection.certificate-fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Certificate fingerprint for HTTPS verification.");

    private static final Set<LogicalTypeRoot> ALLOWED_PRIMARY_KEY_TYPES =
            Set.of(
                    LogicalTypeRoot.CHAR,
                    LogicalTypeRoot.VARCHAR,
                    LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.DECIMAL,
                    LogicalTypeRoot.TINYINT,
                    LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER,
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.FLOAT,
                    LogicalTypeRoot.DOUBLE,
                    LogicalTypeRoot.DATE,
                    LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                    LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                    LogicalTypeRoot.INTERVAL_DAY_TIME);

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT_OPTION);

        helper.validate();

        Elasticsearch9Configuration config = new Elasticsearch9Configuration(helper.getOptions());
        validateConfiguration(config);

        List<PrimaryKeyInfo> primaryKeys = extractPrimaryKeys(context);

        return new Elasticsearch9DynamicSink(
                format,
                config,
                primaryKeys,
                context.getPhysicalRowDataType());
    }

    private void validateConfiguration(Elasticsearch9Configuration config) {
        config.getHosts();
        if (config.getIndex().isEmpty()) {
            throw new ValidationException(
                    String.format("'%s' must not be empty.", INDEX_OPTION.key()));
        }
        int maxActions = config.getBulkFlushMaxActions();
        if (maxActions != -1 && maxActions < 1) {
            throw new ValidationException(
                    String.format(
                            "'%s' must be at least 1. Got: %s",
                            BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
        }
        Optional<String> username = config.getUsername();
        if (username.isPresent() && !StringUtils.isNullOrWhitespaceOnly(username.get())) {
            Optional<String> password = config.getPassword();
            if (password.isEmpty() || StringUtils.isNullOrWhitespaceOnly(password.get())) {
                throw new ValidationException(
                        String.format(
                                "'%s' and '%s' must be set at the same time.",
                                USERNAME_OPTION.key(), PASSWORD_OPTION.key()));
            }
        }
    }

    private List<PrimaryKeyInfo> extractPrimaryKeys(Context context) {
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        int[] primaryKeyIndexes = context.getPrimaryKeyIndexes();

        if (primaryKeyIndexes.length != 0) {
            DataType pkDataType =
                    Projection.of(primaryKeyIndexes).project(physicalRowDataType);
            validatePrimaryKeyTypes(pkDataType);
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return Arrays.stream(primaryKeyIndexes)
                .mapToObj(
                        index -> {
                            Optional<Column> column = resolvedSchema.getColumn(index);
                            if (column.isEmpty()) {
                                throw new IllegalStateException(
                                        String.format(
                                                "No primary key column found with index '%s'.",
                                                index));
                            }
                            LogicalType logicalType =
                                    column.get().getDataType().getLogicalType();
                            return new PrimaryKeyInfo(index, logicalType);
                        })
                .collect(Collectors.toList());
    }

    private void validatePrimaryKeyTypes(DataType pkDataType) {
        pkDataType
                .getChildren()
                .forEach(
                        child -> {
                            LogicalType type = child.getLogicalType();
                            if (!ALLOWED_PRIMARY_KEY_TYPES.contains(type.getTypeRoot())) {
                                throw new ValidationException(
                                        String.format(
                                                "Primary key column type '%s' is not supported.",
                                                type));
                            }
                        });
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS_OPTION);
        options.add(INDEX_OPTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        FORMAT_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        DEAD_LETTER_INDEX_OPTION,
                        ALLOW_INSECURE_OPTION,
                        CERTIFICATE_FINGERPRINT_OPTION,
                        SINK_PARALLELISM)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        HOSTS_OPTION,
                        INDEX_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION)
                .collect(Collectors.toSet());
    }
}
