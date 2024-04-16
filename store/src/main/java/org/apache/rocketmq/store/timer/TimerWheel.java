/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TimerWheel {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final String TIMER_WHEEL_FILE_NAME = "timerwheel";
    public static final int BLANK = -1, IGNORE = -2;
    public final int slotsTotal;
    public final int precisionMs;
    private String fileName;
    private final RandomAccessFile randomAccessFile;
    //    private final FileChannel fileChannel;
//    private final MappedByteBuffer mappedByteBuffer;
    private final ByteBuffer byteBuffer;
    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return byteBuffer.duplicate();
        }
    };
    private final int wheelLength;

    public TimerWheel(String fileName, int slotsTotal, int precisionMs) throws IOException {
        this(fileName, -1, slotsTotal, precisionMs);
    }

    public TimerWheel(String fileName, long flag, int slotsTotal, int precisionMs) throws IOException {
        this.fileName = fileName;
        this.slotsTotal = slotsTotal;
        this.precisionMs = precisionMs;
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;

        String currentFileName = selectSnapshotByFlag(flag);
        File file = new File(currentFileName);
        UtilAll.ensureDirOK(file.getParent());

        try {
            randomAccessFile = new RandomAccessFile(currentFileName, "rw");
            if (file.exists() && randomAccessFile.length() != 0 &&
                randomAccessFile.length() != wheelLength) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s",
                    randomAccessFile.length(), wheelLength));
            }
            randomAccessFile.setLength(wheelLength);
            FileChannel fileChannel = randomAccessFile.getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
            assert wheelLength == mappedByteBuffer.remaining();
            this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);
            this.byteBuffer.put(mappedByteBuffer);

            // unmap mappedByteBuffer
            UtilAll.cleanBuffer(mappedByteBuffer);
            fileChannel.close();
        } catch (FileNotFoundException e) {
            log.error("create file channel " + currentFileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + currentFileName + " Failed. ", e);
            throw e;
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean flush) {
        if (flush) {
            try {
                this.flush();
            } catch (IOException e) {
                log.error("Shutdown error in timer wheel", e);
            }
        }
        // unmap mappedByteBuffer
//        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.byteBuffer);
    }

    public void flush() throws IOException {
        this.flush(-1);
    }

    public void flush(long timeStamp) throws IOException {
        ByteBuffer bf = localBuffer.get();
        bf.position(0);
        bf.limit(wheelLength);

        String fileName = selectSnapshotByFlag(timeStamp);
        RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
        mappedByteBuffer.position(0);
        mappedByteBuffer.put(bf);
        mappedByteBuffer.force();
    }

    public Slot getSlot(long timeMs) {
        Slot slot = getRawSlot(timeMs);
        if (slot.timeMs != timeMs / precisionMs * precisionMs) {
            return new Slot(-1, -1, -1);
        }
        return slot;
    }

    //testable
    public Slot getRawSlot(long timeMs) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        return new Slot(localBuffer.get().getLong() * precisionMs,
            localBuffer.get().getLong(), localBuffer.get().getLong(), localBuffer.get().getInt(), localBuffer.get().getInt());
    }

    public int getSlotIndex(long timeMs) {
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    public void putSlot(long timeMs, long firstPos, long lastPos) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // To be compatible with previous version.
        // The previous version's precision is fixed at 1000ms and it store timeMs / 1000 in slot.
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
    }

    public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        localBuffer.get().putInt(num);
        localBuffer.get().putInt(magic);
    }

    public void reviseSlot(long timeMs, long firstPos, long lastPos, boolean force) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);

        if (timeMs / precisionMs != localBuffer.get().getLong()) {
            if (force) {
                putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
        } else {
            if (IGNORE != firstPos) {
                localBuffer.get().putLong(firstPos);
            } else {
                localBuffer.get().getLong();
            }
            if (IGNORE != lastPos) {
                localBuffer.get().putLong(lastPos);
            }
        }
    }

    //check the timerwheel to see if its stored offset > maxOffset in timerlog
    public long checkPhyPos(long timeStartMs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }
            long first = localBuffer.get().getLong();
            long last = localBuffer.get().getLong();
            if (last > maxOffset) {
                if (first < minFirst) {
                    minFirst = first;
                }
            }
        }
        return minFirst;
    }

    public long getNum(long timeMs) {
        return getSlot(timeMs).num;
    }

    public long getAllNum(long timeStartMs) {
        int allNum = 0;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs == localBuffer.get().getLong()) {
                localBuffer.get().getLong(); //first pos
                localBuffer.get().getLong(); //last pos
                allNum = allNum + localBuffer.get().getInt();
            }
        }
        return allNum;
    }

    private String selectSnapshotByFlag(long flag) {
        if (flag < 0) {
            return this.fileName;
        }
        return this.fileName + "." + flag;
    }

    public void cleanExpiredSnapshot() {
        File dir = new File(this.fileName).getParentFile();
        File[] files = dir.listFiles();
        File tmpFile = null;
        long tmpFlag = -1;
        if (files != null) {
            for (File f : files) {
                String tmpFileName = f.getName();
                if (tmpFileName.startsWith(TIMER_WHEEL_FILE_NAME + ".")) {
                    long flag = UtilAll.asLong(tmpFileName.substring(TIMER_WHEEL_FILE_NAME.length() + 1), -1);
                    if (flag > tmpFlag) {
                        if (tmpFile != null) {
                            UtilAll.deleteFile(tmpFile);
                        }
                        tmpFile = f;
                        tmpFlag = flag;
                    } else if (tmpFile != null) {
                        UtilAll.deleteFile(f);
                    }
                }
            }
        }
    }
}
