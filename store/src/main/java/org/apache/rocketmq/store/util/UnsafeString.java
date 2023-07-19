package org.apache.rocketmq.store.util;

import java.lang.reflect.Field;
import java.util.Arrays;
import sun.misc.Unsafe;

public class UnsafeString {
    private static final String javaVersion;
    private static final boolean isJava11;
    private static final int valueFieldOffset_String;
    private static final int hashFieldOffset_String;
    private static Unsafe theUnsafe;

    private static final String[] constStringCache;
    private static final int CACHE_SIZE = 64;
    private static final int TABLE_MASK = CACHE_SIZE - 1;

    static {
        theUnsafe = UnsafeUtils.theUnsafe;
        javaVersion = System.getProperty("java.version");
        System.out.println("javaVersion=" + javaVersion);
        isJava11 = javaVersion.contains("11.");

        Field f = null;
        Field f2 = null;
        try {
            f = String.class.getDeclaredField("value");
            f.setAccessible(true);

            f2 = String.class.getDeclaredField("hash");
            f2.setAccessible(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

        valueFieldOffset_String = (int) theUnsafe.objectFieldOffset(f);
        System.out.println("offset:" + valueFieldOffset_String);

        hashFieldOffset_String = (int) theUnsafe.objectFieldOffset(f2);
        System.out.println("offset:" + hashFieldOffset_String);

        constStringCache = new String[CACHE_SIZE];
    }

    private static boolean isJava11() {
        return isJava11;
    }

    public static String getConst(byte[] bytes) {
        int hash = 0;
        for (byte v : bytes) {
            hash = 31 * hash + (v & 0xff);
        }
        return getConst(hash, bytes);
    }

    public static String getConst(int hash, byte[] bytes) {
        int idx = hash & TABLE_MASK;
        String cached = constStringCache[idx];
        if (cached != null) {
            byte[] cacheBytes = unsafeGetBytes(cached);
            if (hash == cached.hashCode() && Arrays.equals(bytes, cacheBytes)) {
                return cached;
            }
            return genString(bytes, hash);
        }
        cached = genString(bytes, hash).intern();
        constStringCache[idx] = cached;
        return cached;
    }

    public static String genString(byte[] bytes) {
        if (isJava11()) {
            String base = new String();
            try {
                theUnsafe.putObjectVolatile(base, valueFieldOffset_String, bytes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return base;
        }
        return new String(bytes);
    }

    public static String genString(byte[] bytes, int hash) {
        if (isJava11()) {
            String base = new String();
            try {
                theUnsafe.putObjectVolatile(base, valueFieldOffset_String, bytes);
                theUnsafe.putIntVolatile(base, hashFieldOffset_String, hash);
            } catch (Exception e) {
                e.printStackTrace();
            }
            base.hashCode();
            return base;
        }
        return new String(bytes);
    }

    public static byte[] unsafeGetBytes(String string) {
        return (byte[]) theUnsafe.getObjectVolatile(string, valueFieldOffset_String);
    }

}
