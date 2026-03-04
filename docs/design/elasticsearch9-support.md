# Elasticsearch 9 Support - Design Document

## Context

The flink-connector-elasticsearch project needs a new `flink-connector-elasticsearch9` module to support Elasticsearch 9. The module is based on the existing ES8 async module, targeting Flink 1.20 compatibility on the `v3.1` branch.

The ES9 Java client (v9.0.2) introduces a new HTTP transport layer (`Rest5ClientTransport` based on Apache HttpClient 5) replacing the legacy `RestClientTransport` (Apache HttpClient 4). The Bulk API (`BulkRequest`, `BulkResponse`, `BulkOperation`, `BulkOperationVariant`) is **unchanged** between ES8 and ES9, so the core sink logic requires only transport-layer changes.

**Key constraint**: ES9 Java client requires **Java 17+**.

## Architecture

### Transport Layer Change

**ES8 approach** (`RestClientTransport` + Apache HC4):
- `org.elasticsearch.client.RestClient` / `RestClientBuilder`
- `co.elastic.clients.transport.rest_client.RestClientTransport`
- `org.apache.http.HttpHost`, `org.apache.http.auth.*`

**ES9 approach** (`Rest5ClientTransport` + Apache HC5):
- `co.elastic.clients.transport.rest5_client.low_level.Rest5Client` / `Rest5ClientBuilder`
- `co.elastic.clients.transport.rest5_client.Rest5ClientTransport`
- `org.apache.hc.core5.http.HttpHost`, `org.apache.hc.core5.http.Header`

### Authentication Change

ES8 uses `CredentialsProvider` with `UsernamePasswordCredentials`. ES9 uses header-based Basic auth directly:
```java
String cred = Base64.getEncoder().encodeToString(
    (username + ":" + password).getBytes(StandardCharsets.UTF_8));
builder.setDefaultHeaders(new Header[]{
    new BasicHeader("Authorization", "Basic " + cred)
});
```

### SSL/TLS

`Rest5ClientBuilder.setSSLContext(sslContext)` directly on builder (simpler than ES8's callback pattern).

**Limitation**: Hostname verifier is not directly supported on `Rest5ClientBuilder`. SSLContext covers most use cases including certificate fingerprint and custom trust.

## File Changelist

### New Files (flink-connector-elasticsearch9/)

**Main sources:**
- `Elasticsearch9AsyncSink.java` - Rename 8->9
- `Elasticsearch9AsyncSinkBuilder.java` - Rename 8->9, HC5 imports
- `Elasticsearch9AsyncWriter.java` - Rename 8->9
- `Elasticsearch9AsyncSinkSerializer.java` - Rename 8->9
- `NetworkConfig.java` - Major rewrite for Rest5Client + HC5
- `Operation.java` - Identical copy (Bulk API unchanged)
- `OperationSerializer.java` - Identical copy

**Test sources:**
- `Elasticsearch9AsyncSinkBuilderTest.java`
- `Elasticsearch9AsyncSinkITCase.java`
- `Elasticsearch9AsyncSinkSecureITCase.java`
- `Elasticsearch9AsyncWriterITCase.java`
- `ElasticsearchSinkBaseITCase.java`
- `OperationSerializerTest.java`
- `log4j2-test.properties`

### Modified Files
- `pom.xml` (root) - Add module

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Java 17 requirement | Medium | Flink 1.20 supports Java 17; ES9 itself requires it |
| HC5 dependency conflicts | Medium | Module is self-contained |
| Hostname verifier limitation | Low | SSLContext covers most use cases; documented |
