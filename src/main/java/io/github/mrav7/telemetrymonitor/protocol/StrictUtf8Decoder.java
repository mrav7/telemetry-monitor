package io.github.mrav7.telemetrymonitor.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Decodes frame bytes as UTF-8, reporting malformed input instead of substituting replacement
 * characters.
 *
 * <p>{@code new String(bytes, UTF_8)} would silently turn invalid bytes into U+FFFD, which would
 * let corrupt input reach the JSON parser as plausible-looking text. A reporting decoder is used
 * so that invalid UTF-8 is a distinct, observable rejection.
 */
public final class StrictUtf8Decoder {

    private StrictUtf8Decoder() {
        // Static entry point only.
    }

    /**
     * @throws MessageRejectedException with {@link RejectionReason#INVALID_UTF8} if the bytes are
     *     not well-formed UTF-8
     */
    public static String decode(byte[] frame) throws MessageRejectedException {
        // CharsetDecoder is stateful, so a fresh one is used per frame.
        CharsetDecoder decoder =
                StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(frame)).toString();
        } catch (CharacterCodingException e) {
            throw new MessageRejectedException(
                    RejectionReason.INVALID_UTF8, "frame is not valid UTF-8", e);
        }
    }
}
