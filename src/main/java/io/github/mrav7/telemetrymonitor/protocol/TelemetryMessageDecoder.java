package io.github.mrav7.telemetrymonitor.protocol;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Turns one frame into a validated {@link TelemetryEvent}.
 *
 * <p>Decoding uses Jackson's streaming parser rather than data binding. Binding untrusted JSON onto
 * a record of primitives would make an absent {@code version} indistinguishable from {@code 0} and
 * an absent {@code value} indistinguishable from {@code 0.0}; inspecting tokens keeps "absent",
 * "null", "wrong type" and "invalid value" as four distinct, separately reported outcomes.
 *
 * <p>The strictness below is deliberate rather than inherited from parser defaults:
 *
 * <ul>
 *   <li>the top-level value must be an object, and nothing may follow it in the frame;
 *   <li>unknown properties are rejected instead of ignored;
 *   <li>a repeated property is rejected instead of silently taking the last occurrence;
 *   <li>scalars are never coerced, so {@code "1"} is not accepted for {@code version};
 *   <li>an explicit {@code null} is a type error, not an absent value.
 * </ul>
 *
 * <p>Instances are immutable and safe to share across threads.
 */
public final class TelemetryMessageDecoder {

    /** The only protocol version this monitor accepts. */
    public static final int SUPPORTED_VERSION = 1;

    /** Contract shared by {@code sourceId} and {@code metric}. */
    public static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_.-]{1,64}");

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_SOURCE_ID = "sourceId";
    private static final String FIELD_OCCURRED_AT = "occurredAt";
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_VALUE = "value";

    private final JsonFactory jsonFactory;

    public TelemetryMessageDecoder() {
        this.jsonFactory =
                JsonFactory.builder()
                        // Every lenient read feature is switched off explicitly, so the strict
                        // contract survives a future change to Jackson's defaults. Duplicate
                        // detection is handled below instead of by the parser, so that duplicates
                        // report DUPLICATE_FIELD rather than a generic parse failure.
                        .disable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
                        .disable(JsonReadFeature.ALLOW_YAML_COMMENTS)
                        .disable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
                        .disable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
                        .disable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                        .disable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
                        .disable(JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS)
                        .disable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
                        .disable(JsonReadFeature.ALLOW_MISSING_VALUES)
                        .disable(JsonReadFeature.ALLOW_TRAILING_COMMA)
                        .build();
    }

    /** Decodes frame bytes: strict UTF-8, then JSON, then semantic validation. */
    public TelemetryEvent decode(byte[] frame) throws MessageRejectedException {
        return decode(StrictUtf8Decoder.decode(frame));
    }

    /** Decodes already-decoded frame text. */
    public TelemetryEvent decode(String json) throws MessageRejectedException {
        Long version = null;
        String sourceId = null;
        String occurredAt = null;
        String metric = null;
        Double value = null;

        try (JsonParser parser = jsonFactory.createParser(json)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new MessageRejectedException(
                        RejectionReason.INVALID_JSON, "top-level value must be a JSON object");
            }

            Set<String> seen = new HashSet<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                if (!seen.add(field)) {
                    throw new MessageRejectedException(
                            RejectionReason.DUPLICATE_FIELD, "'" + field + "' appears more than once");
                }
                JsonToken token = parser.nextToken();
                switch (field) {
                    case FIELD_VERSION -> version = integerField(parser, token, field);
                    case FIELD_SOURCE_ID -> sourceId = stringField(parser, token, field);
                    case FIELD_OCCURRED_AT -> occurredAt = stringField(parser, token, field);
                    case FIELD_METRIC -> metric = stringField(parser, token, field);
                    case FIELD_VALUE -> value = numberField(parser, token, field);
                    default ->
                            throw new MessageRejectedException(
                                    RejectionReason.UNEXPECTED_FIELD,
                                    "'" + field + "' is not part of the protocol");
                }
            }

            if (parser.nextToken() != null) {
                throw new MessageRejectedException(
                        RejectionReason.INVALID_JSON, "frame contains more than one JSON value");
            }
        } catch (IOException e) {
            // Covers malformed JSON; Jackson's parse failures are IOExceptions.
            throw new MessageRejectedException(
                    RejectionReason.INVALID_JSON, "frame is not well-formed JSON", e);
        }

        return validate(version, sourceId, occurredAt, metric, value);
    }

    private static TelemetryEvent validate(
            Long version, String sourceId, String occurredAt, String metric, Double value)
            throws MessageRejectedException {

        requirePresent(version, FIELD_VERSION);
        requirePresent(sourceId, FIELD_SOURCE_ID);
        requirePresent(occurredAt, FIELD_OCCURRED_AT);
        requirePresent(metric, FIELD_METRIC);
        requirePresent(value, FIELD_VALUE);

        if (version != SUPPORTED_VERSION) {
            throw new MessageRejectedException(
                    RejectionReason.UNSUPPORTED_VERSION,
                    "only version " + SUPPORTED_VERSION + " is supported, but was " + version);
        }
        if (!IDENTIFIER.matcher(sourceId).matches()) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_SOURCE_ID,
                    "must match " + IDENTIFIER.pattern());
        }
        if (!IDENTIFIER.matcher(metric).matches()) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_METRIC, "must match " + IDENTIFIER.pattern());
        }

        Instant parsedOccurredAt;
        try {
            parsedOccurredAt = Instant.parse(occurredAt);
        } catch (DateTimeParseException e) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_TIMESTAMP, "must be an ISO-8601 instant", e);
        }

        if (!Double.isFinite(value)) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_VALUE, "must be finite, but was " + value);
        }

        return new TelemetryEvent(
                Math.toIntExact(version), sourceId, parsedOccurredAt, metric, value);
    }

    private static void requirePresent(Object fieldValue, String field)
            throws MessageRejectedException {
        if (fieldValue == null) {
            throw new MessageRejectedException(
                    RejectionReason.MISSING_FIELD, "'" + field + "' is required");
        }
    }

    private static Long integerField(JsonParser parser, JsonToken token, String field)
            throws MessageRejectedException {
        if (token != JsonToken.VALUE_NUMBER_INT) {
            throw invalidType(field, token, "an integer");
        }
        try {
            return parser.getLongValue();
        } catch (IOException e) {
            // Beyond long range, so certainly not the supported version.
            throw new MessageRejectedException(
                    RejectionReason.UNSUPPORTED_VERSION, "'" + field + "' is out of range", e);
        }
    }

    private static String stringField(JsonParser parser, JsonToken token, String field)
            throws MessageRejectedException {
        if (token != JsonToken.VALUE_STRING) {
            throw invalidType(field, token, "a string");
        }
        try {
            return parser.getText();
        } catch (IOException e) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_JSON, "'" + field + "' could not be read", e);
        }
    }

    private static Double numberField(JsonParser parser, JsonToken token, String field)
            throws MessageRejectedException {
        if (token != JsonToken.VALUE_NUMBER_FLOAT && token != JsonToken.VALUE_NUMBER_INT) {
            throw invalidType(field, token, "a number");
        }
        try {
            return parser.getDoubleValue();
        } catch (IOException e) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_VALUE, "'" + field + "' is not a usable number", e);
        }
    }

    private static MessageRejectedException invalidType(
            String field, JsonToken token, String expected) {
        String actual = token == JsonToken.VALUE_NULL ? "null" : String.valueOf(token);
        return new MessageRejectedException(
                RejectionReason.INVALID_FIELD_TYPE,
                "'" + field + "' must be " + expected + ", but was " + actual);
    }
}
