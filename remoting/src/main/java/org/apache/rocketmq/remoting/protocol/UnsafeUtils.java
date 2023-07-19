package org.apache.rocketmq.remoting.protocol;

import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import sun.misc.Unsafe;

public class UnsafeUtils {
    public static final int BYTE_ARRAY_BASE_OFFSET;

    public final static Unsafe theUnsafe;

    static {
        Unsafe theUnsafeInstance = null;
        try {
            // 获取theUnsafe
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            theUnsafeInstance = (Unsafe) field.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        theUnsafe = theUnsafeInstance;
        BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    }

    public static int parseUnsignedInt(byte[] bytes) {
        int ret = 0;
        for (byte b : bytes) {
            ret = ret * 10 + b - '0';
        }
        return ret;
    }

    public static long parseUnsignedLong(byte[] bytes) {
        long ret = 0L;
        for (byte b : bytes) {
            ret = ret * 10 + b - '0';
        }
        return ret;
    }

    public static void writeUnsignedInt(ByteBuf byteBuf, int num) {
        if (num == 0) {
            byteBuf.writeByte('0');
            return;
        }
        byte[] tmpBytes = new byte[10];
        int ptr = 9;
        while (num > 0) {
            tmpBytes[ptr--] = (byte) ((num % 10) + '0');
            num /= 10;
        }
        ptr++;
        byteBuf.writeBytes(tmpBytes, ptr, 10 - ptr);
    }

    public static void writeUnsignedLong(ByteBuf byteBuf, long num) {
        if (num == 0) {
            byteBuf.writeByte('0');
            return;
        }
        byte[] tmpBytes = new byte[10];
        int ptr = 9;
        while (num > 0) {
            tmpBytes[ptr--] = (byte) ((num % 10) + '0');
            num /= 10;
        }
        ptr++;
        byteBuf.writeBytes(tmpBytes, ptr, 10 - ptr);
    }

    public static void writeJsonStr(ByteBuf byteBuf, byte[] bytes) {
        int ptr = 0;
        int markPtr = 0;
        for (; ptr < bytes.length; ptr++) {
            byte c = bytes[ptr];
            if (c == '"' || c == '\\') {
                byteBuf.writeBytes(bytes, markPtr, ptr - markPtr);
                byteBuf.writeByte((byte) '\\');
                markPtr = ptr;
            }
        }
        if (ptr > markPtr) {
            byteBuf.writeBytes(bytes, markPtr, ptr - markPtr);
        }
    }
}
