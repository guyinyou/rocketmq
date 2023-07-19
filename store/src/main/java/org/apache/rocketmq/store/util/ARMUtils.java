package org.apache.rocketmq.store.util;

import io.netty.buffer.ByteBuf;
import org.apache.rocketmq.common.EncodeResult;
import org.apache.rocketmq.common.message.MessageExtBatch;

public class ARMUtils {
    private native static long encode(byte[] inBytes, long outAddr, int topicLen, int batchPropLen, int sysFlag,
        int queueId, long bornTimestamp);

    public static EncodeResult encode(final MessageExtBatch messageExtBatch, byte[] batchPropData, ByteBuf byteBuf) {
        byteBuf.clear();
        byte[] topicBytes = messageExtBatch.getTopic().getBytes();
        byteBuf.writeBytes(topicBytes);
        byteBuf.writeBytes(batchPropData);
        messageExtBatch.getBornHostBytes(byteBuf);
        messageExtBatch.getStoreHostBytes(byteBuf);
        int paramSize = byteBuf.writerIndex();
        long ret = encode(messageExtBatch.getBody(), byteBuf.memoryAddress(),
            topicBytes.length, batchPropData.length, messageExtBatch.getSysFlag(), messageExtBatch.getQueueId(),
            messageExtBatch.getBornTimestamp());
        int batchSize = (int) (ret);
        int out_size = (int) (ret >>> 32);
        return new EncodeResult(byteBuf.nioBuffer(paramSize, out_size), batchSize);
    }

    private static final boolean enableYitianOptimized;
    private static final boolean enableX86Optimized;

    public static boolean isEnableYitianOptimized() {
        return enableYitianOptimized;
    }

    public static boolean isEnableX86Optimized() {
        return enableX86Optimized;
    }

    static {
        enableYitianOptimized = Boolean.parseBoolean(System.getenv("ARM_OPT"));
        enableX86Optimized = Boolean.parseBoolean(System.getenv("X86_OPT"));
        System.out.println("enableYitianOptimized: " + enableYitianOptimized);
        System.out.println("enableX86Optimized: " + enableX86Optimized);
        if (enableYitianOptimized) {
            System.load("/home/admin/so/libarmutils.so");
        }
    }
}
