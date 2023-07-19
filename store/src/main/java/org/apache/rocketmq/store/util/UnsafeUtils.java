package org.apache.rocketmq.store.util;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import sun.misc.Unsafe;

public class UnsafeUtils {
    private static final int arrayFieldOffset_CopyOnWriteArrayList;
    public final static Unsafe theUnsafe;
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

        Field f = null;
        Unsafe theUnsafeInstance = null;
        try {
            // 获取theUnsafe
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            theUnsafeInstance = (Unsafe) field.get(null);

            f = CopyOnWriteArrayList.class.getDeclaredField("array");
        } catch (Exception e) {
            e.printStackTrace();
        }
        theUnsafe = theUnsafeInstance;
        arrayFieldOffset_CopyOnWriteArrayList = (int) theUnsafe.objectFieldOffset(f);
    }

    public static Object unsafeGetLastElement(CopyOnWriteArrayList copyOnWriteArrayList) {
        Object[] objects = (Object[]) theUnsafe.getObjectVolatile(copyOnWriteArrayList, arrayFieldOffset_CopyOnWriteArrayList);
        if (objects == null) {
            return null;
        }
        return objects.length == 0 ? null : objects[objects.length - 1];
    }

    public static Map<String, String> fastString2messageProperties(final String properties) {
        if (properties != null) {
            byte[] propertiesBytes = UnsafeString.unsafeGetBytes(properties);
            return fastString2messageProperties(propertiesBytes, 0, propertiesBytes.length);
        }
        return null;
    }

    public static Map<String, String> fastString2messageProperties(byte[] propertiesBytes, int offset, int len) {
        Map<String, String> map = new HashMap<>();
        int index = 0;
        int hash = 0;
        for (int i = offset; i < len; i++) {
            byte v = propertiesBytes[i];
            if (v == '\u0001') {
                byte[] keyBytes = Arrays.copyOfRange(propertiesBytes, index, i);
                String keyStr = UnsafeString.getConst(hash, keyBytes);
                index = ++i;
                for (; i < len; i++) {
                    byte tv = propertiesBytes[i];
                    if (tv == '\u0002') {
                        break;
                    }
                }
                byte[] valBytes = Arrays.copyOfRange(propertiesBytes, index, i);
                String valStr = UnsafeString.genString(valBytes);

                hash = 0;
                index = i + 1;
                map.put(keyStr, valStr);
                continue;
            }
            hash = 31 * hash + (v & 0xff);
        }
        return map;
    }

    public static String fastBuildIndexKey(String topic, String key) {
        byte[] topicBytes = UnsafeString.unsafeGetBytes(topic);
        byte[] keyBytes = UnsafeString.unsafeGetBytes(key);
        byte[] newStringBytes = new byte[topicBytes.length + keyBytes.length + 1];
        int ptr = 0;
        for (byte v : topicBytes) {
            newStringBytes[ptr++] = v;
        }
        newStringBytes[ptr++] = '#';
        int hash = 31 * topic.hashCode() + ('#' & 0xff);
        for (byte v : keyBytes) {
            newStringBytes[ptr++] = v;
            hash = 31 * topic.hashCode() + (v & 0xff);
        }
        return UnsafeString.genString(newStringBytes, hash);
    }
}
