package org.apache.rocketmq.common;

import java.nio.ByteBuffer;

public class EncodeResult {
    private ByteBuffer buffer;
    private int batchSize;

    public EncodeResult(ByteBuffer buffer, int batchSize) {
        this.buffer = buffer;
        this.batchSize = batchSize;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
