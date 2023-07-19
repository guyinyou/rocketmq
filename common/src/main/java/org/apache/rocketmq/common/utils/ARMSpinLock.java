package org.apache.rocketmq.common.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import sun.nio.ch.DirectBuffer;

public class ARMSpinLock {
    private static final ArrayList<ByteBuffer> memBlocks = new ArrayList<>();
    private static final int memBlockSize = 1024 * 1024;
    private static final int cacheLineSize = 64;

    static {
        System.load("/home/admin/so/libarmspinlock.so");
        mallocMemBlock();
    }

    private static ByteBuffer mallocMemBlock() {
        ByteBuffer block = ByteBuffer.allocateDirect(memBlockSize);
        memBlocks.add(block);
        return block;
    }

    private final long addr;

    public ARMSpinLock() {
        synchronized (memBlocks) {
            ByteBuffer lastMemBlock = memBlocks.get(memBlocks.size() - 1);
            if (!lastMemBlock.hasRemaining()) {
                lastMemBlock = mallocMemBlock();
            }
            addr = ((DirectBuffer) lastMemBlock).address() + lastMemBlock.position();
            lastMemBlock.position(lastMemBlock.position() + cacheLineSize);
        }
    }

    public void lock() {
        boolean flag = this.lock(addr);
        while (!flag){
            Thread.yield();
            flag = this.lock(addr);
        }
    }

    public void unlock() {
        this.unlock(addr);
    }

    private native boolean lock(long addr);

    private native void unlock(long addr);
}
