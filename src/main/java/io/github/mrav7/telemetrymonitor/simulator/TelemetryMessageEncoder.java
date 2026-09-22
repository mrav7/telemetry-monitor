package io.github.mrav7.telemetrymonitor.simulator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.mrav7.telemetrymonitor.protocol.TelemetryMessageDecoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

/** Encodes protocol-v1 frames and the simulator's fixed malformed fixtures. */
public final class TelemetryMessageEncoder {

    private static final byte LF = (byte) '\n';

    private final ObjectMapper objectMapper = new ObjectMapper();

    public byte[] encode(
            String sourceId, Instant occurredAt, String metric, double value) throws IOException {
        ObjectNode message = validBaseline(sourceId, occurredAt, metric, value);
        return framed(objectMapper.writeValueAsBytes(message));
    }

    public byte[] encodeMalformed(
            MalformedCase malformedCase,
            String sourceId,
            Instant occurredAt,
            String metric,
            double value)
            throws IOException {
        return switch (malformedCase) {
            case BROKEN_JSON ->
                    brokenJsonFrame(validBaseline(sourceId, occurredAt, metric, value));
            case MISSING_FIELD -> {
                ObjectNode message = validBaseline(sourceId, occurredAt, metric, value);
                message.remove("metric");
                yield framed(objectMapper.writeValueAsBytes(message));
            }
            case UNSUPPORTED_VERSION -> {
                ObjectNode message = validBaseline(sourceId, occurredAt, metric, value);
                message.put("version", TelemetryMessageDecoder.SUPPORTED_VERSION + 1);
                yield framed(objectMapper.writeValueAsBytes(message));
            }
            case INVALID_SOURCE_ID -> {
                ObjectNode message = validBaseline(sourceId, occurredAt, metric, value);
                message.put("sourceId", "invalid source");
                yield framed(objectMapper.writeValueAsBytes(message));
            }
            case INVALID_TIMESTAMP -> {
                ObjectNode message = validBaseline(sourceId, occurredAt, metric, value);
                message.put("occurredAt", "not-an-instant");
                yield framed(objectMapper.writeValueAsBytes(message));
            }
            case INVALID_VALUE -> invalidValueFrame(sourceId, occurredAt, metric);
        };
    }

    private ObjectNode validBaseline(
            String sourceId, Instant occurredAt, String metric, double value) {
        ObjectNode message = objectMapper.createObjectNode();
        message.put("version", TelemetryMessageDecoder.SUPPORTED_VERSION);
        message.put("sourceId", sourceId);
        message.put("occurredAt", occurredAt.toString());
        message.put("metric", metric);
        message.put("value", value);
        return message;
    }

    private byte[] invalidValueFrame(String sourceId, Instant occurredAt, String metric)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JsonGenerator json = objectMapper.createGenerator(bytes)) {
            json.writeStartObject();
            json.writeNumberField("version", TelemetryMessageDecoder.SUPPORTED_VERSION);
            json.writeStringField("sourceId", sourceId);
            json.writeStringField("occurredAt", occurredAt.toString());
            json.writeStringField("metric", metric);
            json.writeFieldName("value");
            json.writeNumber("1e309");
            json.writeEndObject();
        }
        return framed(bytes.toByteArray());
    }

    private byte[] brokenJsonFrame(ObjectNode validBaseline) throws IOException {
        byte[] validJson = objectMapper.writeValueAsBytes(validBaseline);
        byte[] missingClosingBrace = new byte[validJson.length - 1];
        System.arraycopy(validJson, 0, missingClosingBrace, 0, missingClosingBrace.length);
        return framed(missingClosingBrace);
    }

    private static byte[] framed(byte[] json) {
        byte[] frame = new byte[json.length + 1];
        System.arraycopy(json, 0, frame, 0, json.length);
        frame[json.length] = LF;
        return frame;
    }
}
