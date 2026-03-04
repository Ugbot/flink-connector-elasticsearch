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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch9AsyncWriter Apache Flink's Async Sink Writer that submits Operations into an
 * Elasticsearch cluster.
 *
 * @param <InputT> type of Operations
 */
public class Elasticsearch9AsyncWriter<InputT> extends AsyncSinkWriter<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch9AsyncWriter.class);

    private final ElasticsearchAsyncClient esClient;

    private boolean close = false;

    private final Counter numRecordsOutErrorsCounter;
    /**
     * A counter to track number of records that are returned by Elasticsearch as failed and then
     * retried by this writer.
     */
    private final Counter numRecordsSendPartialFailureCounter;
    /** A counter to track the number of bulk requests that are sent to Elasticsearch. */
    private final Counter numRequestSubmittedCounter;
    /** A counter to track non-retryable items that were dropped. */
    private final Counter numRecordsDroppedCounter;

    /** Cached serializer instance to avoid Kryo instantiation per record. */
    private final OperationSerializer operationSerializer = new OperationSerializer();

    /** Set of error types from Elasticsearch that indicate a retryable condition. */
    private static final java.util.Set<String> RETRYABLE_ES_ERROR_TYPES =
            java.util.Set.of(
                    "es_rejected_execution_exception",
                    "circuit_breaking_exception",
                    "too_many_requests");

    private static final FatalExceptionClassifier ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    new FatalExceptionClassifier(
                            err ->
                                    err instanceof NoRouteToHostException
                                            || err instanceof ConnectException,
                            err ->
                                    new FlinkRuntimeException(
                                            "Could not connect to Elasticsearch cluster using the provided hosts",
                                            err)),
                    new FatalExceptionClassifier(
                            err -> {
                                if (err instanceof ResponseException) {
                                    int status =
                                            ((ResponseException) err)
                                                    .getResponse()
                                                    .getStatusCode();
                                    // 4xx client errors (except 429) are fatal
                                    return status >= 400 && status < 500 && status != 429;
                                }
                                return false;
                            },
                            err ->
                                    new FlinkRuntimeException(
                                            "Non-retryable Elasticsearch client error", err)));

    public Elasticsearch9AsyncWriter(
            ElementConverter<InputT, Operation> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
            Collection<BufferedRequestState<Operation>> state) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                state);

        this.esClient = networkConfig.createEsClient();
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendPartialFailureCounter =
                metricGroup.counter("numRecordsSendPartialFailure");
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
        this.numRecordsDroppedCounter = metricGroup.counter("numRecordsDropped");
    }

    @Override
    protected void submitRequestEntries(
            List<Operation> requestEntries, Consumer<List<Operation>> requestResult) {
        numRequestSubmittedCounter.inc();
        LOG.debug("submitRequestEntries with {} items", requestEntries.size());

        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Operation operation : requestEntries) {
            br.operations(new BulkOperation(operation.getBulkOperationVariant()));
        }

        esClient.bulk(br.build())
                .whenComplete(
                        (response, error) -> {
                            if (error != null) {
                                handleFailedRequest(requestEntries, requestResult, error);
                            } else if (response.errors()) {
                                handlePartiallyFailedRequest(
                                        requestEntries, requestResult, response);
                            } else {
                                handleSuccessfulRequest(requestResult, response);
                            }
                        });
    }

    private void handleFailedRequest(
            List<Operation> requestEntries,
            Consumer<List<Operation>> requestResult,
            Throwable error) {
        LOG.warn(
                "The BulkRequest of {} operation(s) has failed due to: {}",
                requestEntries.size(),
                error.getMessage());
        LOG.debug("The BulkRequest has failed", error);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        Throwable cause = error.getCause() != null ? error.getCause() : error;
        if (isRetryable(cause)) {
            requestResult.accept(requestEntries);
        } else {
            // Must complete the callback to avoid hanging the pipeline
            requestResult.accept(Collections.emptyList());
            getFatalExceptionCons()
                    .accept(
                            new FlinkRuntimeException(
                                    "Non-retryable Elasticsearch error in bulk request of "
                                            + requestEntries.size()
                                            + " operation(s)",
                                    error));
        }
    }

    private void handlePartiallyFailedRequest(
            List<Operation> requestEntries,
            Consumer<List<Operation>> requestResult,
            BulkResponse response) {
        LOG.debug("The BulkRequest has failed partially. Response: {}", response);
        ArrayList<Operation> retryableItems = new ArrayList<>();
        int droppedCount = 0;

        for (int i = 0; i < response.items().size(); i++) {
            BulkResponseItem item = response.items().get(i);
            if (item.error() != null) {
                if (isItemRetryable(item.status(), item.error().type())) {
                    retryableItems.add(requestEntries.get(i));
                } else {
                    droppedCount++;
                    LOG.warn(
                            "Dropping non-retryable item: index={}, status={}, errorType={}, reason={}",
                            item.index(),
                            item.status(),
                            item.error().type(),
                            item.error().reason());
                }
            }
        }

        int totalFailures = retryableItems.size() + droppedCount;
        numRecordsOutErrorsCounter.inc(totalFailures);
        numRecordsSendPartialFailureCounter.inc(retryableItems.size());
        numRecordsDroppedCounter.inc(droppedCount);
        LOG.info(
                "The BulkRequest with {} operation(s) has {} failure(s) ({} retryable, {} dropped). It took {}ms",
                requestEntries.size(),
                totalFailures,
                retryableItems.size(),
                droppedCount,
                response.took());
        requestResult.accept(retryableItems);
    }

    /**
     * Determines if a bulk item failure is retryable based on HTTP status and error type.
     *
     * <p>Retryable conditions (matching ES7 parity and production patterns):
     *
     * <ul>
     *   <li>HTTP 429 (Too Many Requests)
     *   <li>HTTP 5xx (server errors)
     *   <li>es_rejected_execution_exception (thread pool saturation)
     *   <li>circuit_breaking_exception (memory pressure)
     * </ul>
     */
    private boolean isItemRetryable(int status, String errorType) {
        if (status == 429) {
            return true;
        }
        if (status >= 500 && status <= 599) {
            return true;
        }
        if (errorType != null && RETRYABLE_ES_ERROR_TYPES.contains(errorType)) {
            return true;
        }
        return false;
    }

    private void handleSuccessfulRequest(
            Consumer<List<Operation>> requestResult, BulkResponse response) {
        LOG.debug(
                "The BulkRequest of {} operation(s) completed successfully. It took {}ms",
                response.items().size(),
                response.took());
        requestResult.accept(Collections.emptyList());
    }

    private boolean isRetryable(Throwable error) {
        return !ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER.isFatal(error, getFatalExceptionCons());
    }

    @Override
    protected long getSizeInBytes(Operation requestEntry) {
        return operationSerializer.size(requestEntry);
    }

    @Override
    public void close() {
        if (!close) {
            close = true;
            esClient.shutdown();
        }
    }
}
