package org.apache.rocketmq.remoting.protocol;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

public class AsyncStringHashMap extends HashMap<String, String> {
    private final int CONSUMER_GROUP = -308835287;
    private final int TOPIC = 110546223;
    private final int QUEUE_ID = 655172108;
    private final int QUEUE_OFFSET = -1025599228;
    private final int MAX_MSG_BYTES = 722278126;
    private final int MAX_MSG_NUMS = -1777462582;
    private final int SYS_FLAG = -1738734631;
    private final int COMMIT_OFFSET = -846236502;
    private final int SUSPEND_TIMEOUT_MILLIS = -262186965;
    private final int SUBSCRIPTION = 341203229;
    private final int SUB_VERSION = -1130008968;
    private final int EXPRESSION_TYPE = -1318549006;
    private final int REQUEST_SOURCE = 1398923146;
    private final int PROXY_FROWARD_CLIENT_ID = 314434871;
    private final int LO = 3459;
    private final int NS = 3525;
    private final int NSD = 109375;
    private final int B_NAME = 93878765;
    private final int O_WAY = 3424288;
    private byte[] jsonData = null;

    public void setJsonData(byte[] jsonData) {
        this.jsonData = jsonData;
    }

    class StringView {
        int pos;
        int len;

        public StringView(int pos, int len) {
            this.pos = pos;
            this.len = len;
        }
    }

    public void doDefaultInit() {
        if (jsonData == null) {
            return;
        }
        int dataLen = jsonData.length;
        byte[] data = jsonData;
        boolean isKey = true;
        StringView key = null;
        for (int i = 0; i < dataLen; i++) {
            byte c = data[i];
            switch (c) {
                case '"': {
                    int headIndex = i;
                    int endIndex = i + 1;
                    //加速到字符串结束
                    for (; endIndex < dataLen; endIndex++) {
                        byte tc = data[endIndex];
                        if (tc == '\\') {
                            endIndex++;
                            continue;
                        }
                        if (tc == '"') {
                            break;
                        }
                    }
                    int size = endIndex - headIndex - 1;
                    StringView str = new StringView(headIndex + 1, size);
                    i = endIndex;

                    if (isKey) {
                        key = str;
                    } else {
                        int valLen = size;
                        int charPos = headIndex + 1;
                        int realLen = 0;
                        byte[] valData = new byte[valLen];
                        for (int o = charPos; o < charPos + valLen; o++) {
                            byte tmpChar = data[o];
                            if (tmpChar == '\\' && o < charPos + valLen - 5 && data[++o] == 'u') {
                                int c1 = data[++o] - '0';
                                int c2 = data[++o] - '0';
                                int c3 = data[++o] - '0';
                                int c4 = data[++o] - '0';
                                tmpChar = (byte) ((c1 << 12 | c2 << 8 | c3 << 4 | c4));
                            }
                            valData[realLen++] = tmpChar;
                        }
                        if (valLen != realLen) {
                            valData = Arrays.copyOf(valData, realLen);
                            valLen = realLen;
                        }

                        int keyLen = key.len;
                        charPos = key.pos;
                        realLen = 0;
                        byte[] keyData = new byte[keyLen];
                        for (int o = charPos; o < charPos + keyLen; o++) {
                            byte tmpChar = data[o];
                            if (tmpChar == '\\' && o < charPos + keyLen - 5 && data[++o] == 'u') {
                                int c1 = data[++o] - '0';
                                int c2 = data[++o] - '0';
                                int c3 = data[++o] - '0';
                                int c4 = data[++o] - '0';
                                tmpChar = (byte) ((c1 << 12 | c2 << 8 | c3 << 4 | c4));
                            }
                            keyData[realLen++] = tmpChar;
                        }
                        if (keyLen != realLen) {
                            keyData = Arrays.copyOf(keyData, realLen);
                            keyLen = realLen;
                        }
                        super.put(UnsafeString.getConst(keyData), UnsafeString.genString(valData));
                    }
                }
                break;
                case ':':
                case ',':
                    isKey = !isKey;
                    break;
            }
        }
        this.jsonData = null;
    }

    public SendMessageRequestHeader doSendMessageHeaderInit() {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        if (jsonData != null) {
            int dataLen = jsonData.length;
            byte[] data = jsonData;
            boolean isKey = true;
            StringView key = null;
            for (int i = 0; i < dataLen; i++) {
                byte c = data[i];
                switch (c) {
                    case '"': {
                        int headIndex = i;
                        int endIndex = i + 1;
                        //加速到字符串结束
                        for (; endIndex < dataLen; endIndex++) {
                            byte tc = data[endIndex];
                            if (tc == '\\') {
                                endIndex++;
                                continue;
                            }
                            if (tc == '"') {
                                break;
                            }
                        }
                        int size = endIndex - headIndex - 1;
                        StringView str = new StringView(headIndex + 1, size);
                        i = endIndex;

                        if (isKey) {
                            key = str;
                        } else {
                            int valLen = size;
                            int charPos = headIndex + 1;
                            int realLen = 0;
                            byte[] valData = new byte[valLen];
                            for (int o = charPos; o < charPos + valLen; o++) {
                                byte tmpChar = data[o];
                                if (tmpChar == '\\' && o < charPos + valLen - 5 && data[++o] == 'u') {
                                    int c1 = data[++o] - '0';
                                    int c2 = data[++o] - '0';
                                    int c3 = data[++o] - '0';
                                    int c4 = data[++o] - '0';
                                    tmpChar = (byte) ((c1 << 12 | c2 << 8 | c3 << 4 | c4));
                                }
                                valData[realLen++] = tmpChar;
                            }
                            if (valLen != realLen) {
                                valData = Arrays.copyOf(valData, realLen);
                                valLen = realLen;
                            }

                            int keyLen = key.len;
                            charPos = key.pos;
                            // 可能为直接解码字段
                            if (keyLen == 1) {
                                switch (data[charPos]) {
                                    case 'a':
                                        header.setProducerGroup(UnsafeString.genString(valData));
                                        continue;
                                    case 'b':
                                        header.setTopic(UnsafeString.genString(valData));
                                        continue;
                                    case 'c':
                                        header.setDefaultTopic(UnsafeString.genString(valData));
                                        continue;
                                    case 'd':
                                        header.setDefaultTopicQueueNums(UnsafeUtils.parseUnsignedInt(valData));
                                        continue;
                                    case 'e':
                                        header.setQueueId(UnsafeUtils.parseUnsignedInt(valData));
                                        continue;
                                    case 'f':
                                        header.setSysFlag(UnsafeUtils.parseUnsignedInt(valData));
                                        continue;
                                    case 'g':
                                        header.setBornTimestamp(UnsafeUtils.parseUnsignedLong(valData));
                                        continue;
                                    case 'h':
                                        header.setFlag(UnsafeUtils.parseUnsignedInt(valData));
                                        continue;
                                    case 'i':
                                        header.setProperties(UnsafeString.genString(valData));
                                        continue;
                                    case 'j':
                                        header.setReconsumeTimes(UnsafeUtils.parseUnsignedInt(valData));
                                        continue;
                                    case 'k':
                                        // 无关大小写，默认数据正常情况下，true为4个字节，false为5个字节
                                        header.setUnitMode(valData.length == 4);
                                        continue;
                                    case 'l':
                                        header.setMaxReconsumeTimes(UnsafeUtils.parseUnsignedInt(valData));
                                        continue;
                                    case 'm':
                                        header.setBatch(valData.length == 4);
                                        continue;
                                    case 'n':
                                        header.setBname(UnsafeString.genString(valData));
                                        continue;
                                }
                            }
                            realLen = 0;
                            byte[] keyData = new byte[keyLen];
                            for (int o = charPos; o < charPos + keyLen; o++) {
                                byte tmpChar = data[o];
                                if (tmpChar == '\\' && o < charPos + keyLen - 5 && data[++o] == 'u') {
                                    int c1 = data[++o] - '0';
                                    int c2 = data[++o] - '0';
                                    int c3 = data[++o] - '0';
                                    int c4 = data[++o] - '0';
                                    tmpChar = (byte) ((c1 << 12 | c2 << 8 | c3 << 4 | c4));
                                }
                                keyData[realLen++] = tmpChar;
                            }
                            if (keyLen != realLen) {
                                keyData = Arrays.copyOf(keyData, realLen);
                                keyLen = realLen;
                            }
                            super.put(UnsafeString.getConst(keyData), UnsafeString.genString(valData));
                        }
                    }
                    break;
                    case ':':
                    case ',':
                        isKey = !isKey;
                        break;
                }
            }
            this.jsonData = null;
        }
        return header;
    }

    public PullMessageRequestHeader doPullMessageHeaderInit() {
        PullMessageRequestHeader header = new PullMessageRequestHeader();
        if (jsonData != null) {
            int dataLen = jsonData.length;
            byte[] data = jsonData;
            boolean isKey = true;
            StringView key = null;
            for (int i = 0; i < dataLen; i++) {
                byte c = data[i];
                switch (c) {
                    case '"': {
                        int headIndex = i;
                        int endIndex = i + 1;
                        //加速到字符串结束
                        for (; endIndex < dataLen; endIndex++) {
                            byte tc = data[endIndex];
                            if (tc == '\\') {
                                endIndex++;
                                continue;
                            }
                            if (tc == '"') {
                                break;
                            }
                        }
                        int size = endIndex - headIndex - 1;
                        StringView str = new StringView(headIndex + 1, size);
                        i = endIndex;

                        if (isKey) {
                            key = str;
                        } else {
                            int valLen = size;
                            int charPos = headIndex + 1;
                            int realLen = 0;
                            byte[] valData = new byte[valLen];
                            for (int o = charPos; o < charPos + valLen; o++) {
                                byte tmpChar = data[o];
                                if (tmpChar == '\\' && o < charPos + valLen - 5 && data[++o] == 'u') {
                                    int c1 = data[++o] - '0';
                                    int c2 = data[++o] - '0';
                                    int c3 = data[++o] - '0';
                                    int c4 = data[++o] - '0';
                                    tmpChar = (byte) ((c1 << 12 | c2 << 8 | c3 << 4 | c4));
                                }
                                valData[realLen++] = tmpChar;
                            }
                            if (valLen != realLen) {
                                valData = Arrays.copyOf(valData, realLen);
                                valLen = realLen;
                            }

                            int keyLen = key.len;
                            charPos = key.pos;
                            // 可能为直接解码字段
                            int keyHash = 0;
                            for (int o = key.pos; o < key.pos + key.len; o++) {
                                keyHash = 31 * keyHash + (data[o] & 0xff);
                            }
                            switch (keyHash) {
                                case CONSUMER_GROUP:
                                    header.setConsumerGroup(UnsafeString.genString(valData));
                                    continue;
                                case TOPIC:
                                    header.setTopic(UnsafeString.genString(valData));
                                    continue;
                                case QUEUE_ID:
                                    header.setQueueId(UnsafeUtils.parseUnsignedInt(valData));
                                    continue;
                                case QUEUE_OFFSET:
                                    header.setQueueOffset(UnsafeUtils.parseUnsignedLong(valData));
                                    continue;
                                case MAX_MSG_BYTES:
                                    header.setMaxMsgBytes(UnsafeUtils.parseUnsignedInt(valData));
                                    continue;
                                case MAX_MSG_NUMS:
                                    header.setMaxMsgNums(UnsafeUtils.parseUnsignedInt(valData));
                                    continue;
                                case SYS_FLAG:
                                    header.setSysFlag(UnsafeUtils.parseUnsignedInt(valData));
                                    continue;
                                case COMMIT_OFFSET:
                                    header.setCommitOffset(UnsafeUtils.parseUnsignedLong(valData));
                                    continue;
                                case SUSPEND_TIMEOUT_MILLIS:
                                    header.setSuspendTimeoutMillis(UnsafeUtils.parseUnsignedLong(valData));
                                    continue;
                                case SUBSCRIPTION:
                                    header.setSubscription(UnsafeString.genString(valData));
                                    continue;
                                case SUB_VERSION:
                                    header.setSubVersion(UnsafeUtils.parseUnsignedLong(valData));
                                    continue;
                                case EXPRESSION_TYPE:
                                    header.setExpressionType(UnsafeString.genString(valData));
                                    continue;
                                case REQUEST_SOURCE:
                                    header.setRequestSource(UnsafeUtils.parseUnsignedInt(valData));
                                    continue;
                                case PROXY_FROWARD_CLIENT_ID:
                                    header.setProxyFrowardClientId(UnsafeString.genString(valData));
                                    continue;
                                case LO:
                                    // 无关大小写，默认数据正常情况下，true为4个字节，false为5个字节
                                    header.setLo(valData.length == 4);
                                    continue;
                                case NS:
                                    header.setNs(UnsafeString.genString(valData));
                                    continue;
                                case NSD:
                                    header.setNsd(valData.length == 4);
                                    continue;
                                case B_NAME:
                                    header.setBname(UnsafeString.genString(valData));
                                    continue;
                                case O_WAY:
                                    header.setOway(valData.length == 4);
                                    continue;
                            }

                            realLen = 0;
                            byte[] keyData = new byte[keyLen];
                            for (int o = charPos; o < charPos + keyLen; o++) {
                                byte tmpChar = data[o];
                                if (tmpChar == '\\' && o < charPos + keyLen - 5 && data[++o] == 'u') {
                                    int c1 = data[++o] - '0';
                                    int c2 = data[++o] - '0';
                                    int c3 = data[++o] - '0';
                                    int c4 = data[++o] - '0';
                                    tmpChar = (byte) ((c1 << 12 | c2 << 8 | c3 << 4 | c4));
                                }
                                keyData[realLen++] = tmpChar;
                            }
                            if (keyLen != realLen) {
                                keyData = Arrays.copyOf(keyData, realLen);
                                keyLen = realLen;
                            }
                            super.put(UnsafeString.getConst(keyData), UnsafeString.genString(valData));
                        }
                    }
                    break;
                    case ':':
                    case ',':
                        isKey = !isKey;
                        break;
                }
            }
            this.jsonData = null;
        }
        return header;
    }
}
