package io.github.mrav7.telemetrymonitor.protocol;

import java.io.InputStream;

/**
 * An {@link InputStream} that hands out at most a fixed number of bytes per read.
 *
 * <p>Real TCP reads return whatever has arrived, which is rarely a whole message. This stream makes
 * that fragmentation explicit and repeatable, including the single-byte case, so framing cannot
 * accidentally depend on one read returning one frame.
 */
final class ChunkedInputStream extends InputStream {

    private final byte[] data;
    private final int chunkSize;
    private int position;

    ChunkedInputStream(byte[] data, int chunkSize) {
        this.data = data.clone();
        this.chunkSize = chunkSize;
    }

    @Override
    public int read() {
        return position < data.length ? data[position++] & 0xFF : -1;
    }

    @Override
    public int read(byte[] destination, int offset, int length) {
        if (position >= data.length) {
            return -1;
        }
        int count = Math.min(Math.min(length, chunkSize), data.length - position);
        System.arraycopy(data, position, destination, offset, count);
        position += count;
        return count;
    }

    @Override
    public int available() {
        return data.length - position;
    }

    /** Bytes the reader has not consumed. */
    int remaining() {
        return data.length - position;
    }
}
