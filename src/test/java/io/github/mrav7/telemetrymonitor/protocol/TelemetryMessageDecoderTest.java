package io.github.mrav7.telemetrymonitor.protocol;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the wire contract: what a protocol v1 message must look like, and the exact reason any
 * other input is rejected.
 *
 * <p>Messages are assembled from raw JSON fragments so that a test can express "this field is
 * absent", "this field is null" and "this field has the wrong JSON type" as genuinely different
 * inputs rather than as variations of the same Java value.
 */
class TelemetryMessageDecoderTest {

    private final TelemetryMessageDecoder decoder = new TelemetryMessageDecoder();

    private static String quoted(String value) {
        return "\"" + value + "\"";
    }

    private static Map<String, String> validFields() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("version", "1");
        fields.put("sourceId", quoted("source-01"));
        fields.put("occurredAt", quoted("2026-09-21T18:15:42.123Z"));
        fields.put("metric", quoted("temperature"));
        fields.put("value", "18.72");
        return fields;
    }

    private static String render(Map<String, String> fields) {
        return fields.entrySet().stream()
                .map(entry -> quoted(entry.getKey()) + ":" + entry.getValue())
                .collect(Collectors.joining(",", "{", "}"));
    }

    private static String valid() {
        return render(validFields());
    }

    private static String without(String field) {
        Map<String, String> fields = validFields();
        fields.remove(field);
        return render(fields);
    }

    private static String with(String field, String rawJsonValue) {
        Map<String, String> fields = validFields();
        fields.put(field, rawJsonValue);
        return render(fields);
    }

    private RejectionReason reasonFor(String json) {
        return assertThrows(MessageRejectedException.class, () -> decoder.decode(json)).reason();
    }

    @Nested
    @DisplayName("a valid protocol v1 message")
    class ValidMessage {

        @Test
        @DisplayName("decodes into the five declared fields")
        void decodesAllFields() throws Exception {
            TelemetryEvent event = decoder.decode(valid());

            assertAll(
                    () -> assertEquals(1, event.version()),
                    () -> assertEquals("source-01", event.sourceId()),
                    () ->
                            assertEquals(
                                    Instant.parse("2026-09-21T18:15:42.123Z"), event.occurredAt()),
                    () -> assertEquals("temperature", event.metric()),
                    () -> assertEquals(18.72, event.value()));
        }

        @Test
        @DisplayName("decodes identically from frame bytes")
        void decodesFromBytes() throws Exception {
            assertEquals(
                    decoder.decode(valid()),
                    decoder.decode(valid().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }

        @ParameterizedTest(name = "accepts the finite value {0}")
        @ValueSource(strings = {"0", "-0.5", "18.72", "-273.15", "1e10", "-1e-10", "42"})
        void acceptsFiniteValues(String rawValue) throws Exception {
            assertEquals(Double.parseDouble(rawValue), decoder.decode(with("value", rawValue)).value());
        }
    }

    @Nested
    @DisplayName("JSON structure")
    class Structure {

        @ParameterizedTest(name = "rejects {0}")
        @ValueSource(strings = {"", "   ", "{", "{\"version\":}", "not json", "{'version':1}"})
        void rejectsMalformedJson(String json) {
            assertEquals(RejectionReason.INVALID_JSON, reasonFor(json));
        }

        @ParameterizedTest(name = "rejects the non-object top-level value {0}")
        @ValueSource(strings = {"[]", "[{\"version\":1}]", "1", "\"text\"", "null", "true"})
        void rejectsNonObjectTopLevel(String json) {
            assertEquals(RejectionReason.INVALID_JSON, reasonFor(json));
        }

        @Test
        @DisplayName("rejects more than one JSON value in a frame")
        void rejectsTrailingContent() {
            assertEquals(RejectionReason.INVALID_JSON, reasonFor(valid() + valid()));
        }

        @Test
        @DisplayName("rejects an unknown property instead of ignoring it")
        void rejectsUnknownProperty() {
            assertEquals(
                    RejectionReason.UNEXPECTED_FIELD, reasonFor(with("unexpected", "123")));
        }

        @Test
        @DisplayName("rejects a repeated property instead of taking the last one")
        void rejectsDuplicateProperty() {
            String duplicated =
                    "{\"version\":1,\"version\":2,\"sourceId\":\"source-01\","
                            + "\"occurredAt\":\"2026-09-21T18:15:42.123Z\","
                            + "\"metric\":\"temperature\",\"value\":18.72}";

            assertEquals(RejectionReason.DUPLICATE_FIELD, reasonFor(duplicated));
        }

        @Test
        @DisplayName("rejects a repeated property even when both occurrences agree")
        void rejectsDuplicatePropertyWithEqualValues() {
            String duplicated =
                    "{\"version\":1,\"sourceId\":\"source-01\",\"sourceId\":\"source-01\","
                            + "\"occurredAt\":\"2026-09-21T18:15:42.123Z\","
                            + "\"metric\":\"temperature\",\"value\":18.72}";

            assertEquals(RejectionReason.DUPLICATE_FIELD, reasonFor(duplicated));
        }
    }

    @Nested
    @DisplayName("required fields")
    class RequiredFields {

        @ParameterizedTest(name = "rejects a message without {0}")
        @ValueSource(strings = {"version", "sourceId", "occurredAt", "metric", "value"})
        void rejectsMissingField(String field) {
            assertEquals(RejectionReason.MISSING_FIELD, reasonFor(without(field)));
        }

        @ParameterizedTest(name = "rejects an explicit null {0}")
        @ValueSource(strings = {"version", "sourceId", "occurredAt", "metric", "value"})
        void rejectsExplicitNull(String field) {
            assertEquals(RejectionReason.INVALID_FIELD_TYPE, reasonFor(with(field, "null")));
        }

        @Test
        @DisplayName("an absent numeric field does not become zero")
        void missingNumericFieldIsNotDefaulted() {
            assertAll(
                    () -> assertEquals(RejectionReason.MISSING_FIELD, reasonFor(without("version"))),
                    () -> assertEquals(RejectionReason.MISSING_FIELD, reasonFor(without("value"))));
        }
    }

    @Nested
    @DisplayName("JSON types")
    class Types {

        @ParameterizedTest(name = "rejects {0} given {1}")
        @CsvSource({
            "version, '\"1\"'",
            "version, '1.0'",
            "version, 'true'",
            "version, '{}'",
            "version, '[]'",
            "sourceId, '1'",
            "sourceId, 'true'",
            "sourceId, '[]'",
            "occurredAt, '1'",
            "occurredAt, '{}'",
            "metric, '1'",
            "metric, 'false'",
            "value, '\"18.72\"'",
            "value, 'true'",
            "value, '{}'",
            "value, '[]'"
        })
        void rejectsWrongJsonType(String field, String rawJsonValue) {
            assertEquals(RejectionReason.INVALID_FIELD_TYPE, reasonFor(with(field, rawJsonValue)));
        }

        @Test
        @DisplayName("a quoted number is not coerced into a number")
        void doesNotCoerceStringsToNumbers() {
            assertEquals(RejectionReason.INVALID_FIELD_TYPE, reasonFor(with("version", "\"1\"")));
            assertEquals(RejectionReason.INVALID_FIELD_TYPE, reasonFor(with("value", "\"18.72\"")));
        }
    }

    @Nested
    @DisplayName("protocol version")
    class Version {

        @ParameterizedTest(name = "rejects version {0}")
        @ValueSource(strings = {"0", "2", "-1", "99"})
        void rejectsUnsupportedVersions(String version) {
            assertEquals(RejectionReason.UNSUPPORTED_VERSION, reasonFor(with("version", version)));
        }

        @Test
        @DisplayName("rejects a version far outside the supported range")
        void rejectsOutOfRangeVersion() {
            assertEquals(
                    RejectionReason.UNSUPPORTED_VERSION,
                    reasonFor(with("version", "99999999999999999999")));
        }
    }

    @Nested
    @DisplayName("identifiers")
    class Identifiers {

        @ParameterizedTest(name = "accepts the sourceId {0}")
        @ValueSource(strings = {"a", "source-01", "sensor_2", "processing.node.a", "A1-b_2.c"})
        void acceptsValidSourceIds(String sourceId) throws Exception {
            assertEquals(sourceId, decoder.decode(with("sourceId", quoted(sourceId))).sourceId());
        }

        @Test
        @DisplayName("accepts a 64-character sourceId and rejects 65")
        void enforcesSourceIdLengthBoundary() throws Exception {
            String atLimit = "s".repeat(64);
            assertEquals(atLimit, decoder.decode(with("sourceId", quoted(atLimit))).sourceId());

            assertEquals(
                    RejectionReason.INVALID_SOURCE_ID,
                    reasonFor(with("sourceId", quoted("s".repeat(65)))));
        }

        @ParameterizedTest(name = "rejects the sourceId {0}")
        @ValueSource(strings = {"", " ", "with space", "sensor/1", "sensor:1", "sensor@1", "é"})
        void rejectsInvalidSourceIds(String sourceId) {
            assertEquals(
                    RejectionReason.INVALID_SOURCE_ID, reasonFor(with("sourceId", quoted(sourceId))));
        }

        @ParameterizedTest(name = "accepts the metric {0}")
        @ValueSource(strings = {"t", "temperature", "queue.depth", "cpu_load", "a-b.c_1"})
        void acceptsValidMetrics(String metric) throws Exception {
            assertEquals(metric, decoder.decode(with("metric", quoted(metric))).metric());
        }

        @Test
        @DisplayName("accepts a 64-character metric and rejects 65")
        void enforcesMetricLengthBoundary() throws Exception {
            String atLimit = "m".repeat(64);
            assertEquals(atLimit, decoder.decode(with("metric", quoted(atLimit))).metric());

            assertEquals(
                    RejectionReason.INVALID_METRIC,
                    reasonFor(with("metric", quoted("m".repeat(65)))));
        }

        @ParameterizedTest(name = "rejects the metric {0}")
        @ValueSource(strings = {"", " ", "with space", "cpu%", "a+b"})
        void rejectsInvalidMetrics(String metric) {
            assertEquals(RejectionReason.INVALID_METRIC, reasonFor(with("metric", quoted(metric))));
        }

        @Test
        @DisplayName("identifiers are not trimmed or normalised before validation")
        void doesNotRepairIdentifiers() {
            assertEquals(
                    RejectionReason.INVALID_SOURCE_ID,
                    reasonFor(with("sourceId", quoted(" source-01 "))));
        }
    }

    @Nested
    @DisplayName("occurredAt")
    class OccurredAt {

        @ParameterizedTest(name = "accepts {0}")
        @ValueSource(
                strings = {
                    "2026-09-21T18:15:42.123Z",
                    "2026-09-21T18:15:42Z",
                    "1970-01-01T00:00:00Z"
                })
        void acceptsIsoInstants(String timestamp) throws Exception {
            assertEquals(
                    Instant.parse(timestamp),
                    decoder.decode(with("occurredAt", quoted(timestamp))).occurredAt());
        }

        @ParameterizedTest(name = "rejects {0}")
        @ValueSource(
                strings = {
                    "",
                    "not-a-timestamp",
                    "2026-09-21",
                    "18:15:42",
                    "2026-13-01T00:00:00Z",
                    "1758478542"
                })
        void rejectsInvalidTimestamps(String timestamp) {
            assertEquals(
                    RejectionReason.INVALID_TIMESTAMP,
                    reasonFor(with("occurredAt", quoted(timestamp))));
        }
    }

    @Nested
    @DisplayName("value")
    class Value {

        @Test
        @DisplayName("rejects a literal NaN or Infinity as malformed JSON")
        void rejectsNonNumericLiterals() {
            assertAll(
                    () -> assertEquals(RejectionReason.INVALID_JSON, reasonFor(with("value", "NaN"))),
                    () ->
                            assertEquals(
                                    RejectionReason.INVALID_JSON, reasonFor(with("value", "Infinity"))),
                    () ->
                            assertEquals(
                                    RejectionReason.INVALID_JSON,
                                    reasonFor(with("value", "-Infinity"))));
        }

        @ParameterizedTest(name = "rejects {0} because it is not a finite double")
        @ValueSource(strings = {"1e400", "-1e400", "1e309"})
        void rejectsValuesThatOverflowToInfinity(String rawValue) {
            assertEquals(RejectionReason.INVALID_VALUE, reasonFor(with("value", rawValue)));
        }

        @Test
        @DisplayName("every decoded event carries a finite value")
        void decodedValueIsAlwaysFinite() throws Exception {
            assertEquals(true, Double.isFinite(decoder.decode(valid()).value()));
        }
    }

    @Test
    @DisplayName("validation order is stable: version is checked before identifiers")
    void reportsTheFirstSemanticFailureDeterministically() {
        Map<String, String> fields = validFields();
        fields.put("version", "7");
        fields.put("sourceId", quoted("bad id"));

        assertEquals(RejectionReason.UNSUPPORTED_VERSION, reasonFor(render(fields)));
    }

    @Test
    @DisplayName("no rejection reason requires reading the exception message")
    void everyRejectionExposesAReason() {
        assertEquals(RejectionReason.INVALID_JSON, reasonFor("{"));
        assertEquals(RejectionReason.MISSING_FIELD, reasonFor(without("metric")));
        assertEquals(RejectionReason.INVALID_METRIC, reasonFor(with("metric", quoted("a b"))));
    }
}
