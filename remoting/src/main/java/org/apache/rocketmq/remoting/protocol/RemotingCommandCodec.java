package org.apache.rocketmq.remoting.protocol;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class RemotingCommandCodec {

    private native static void decodeRemotingCommand(final long headerBufferAddr, int headerLenfinal,
        long retBufferAddr);

    public static RemotingCommand fastJsonDecode(final ByteBuf headerBuffer, int headerLen,
        final DirectBuffer directBuffer) {
        int readerIndex = headerBuffer.readerIndex();
        long headerBufferAddr = headerBuffer.memoryAddress() + readerIndex;
        long retBufferAddr = directBuffer.address();
        decodeRemotingCommand(headerBufferAddr, headerLen, retBufferAddr);
        headerBuffer.readerIndex(readerIndex + headerLen);

        RemotingCommand cmd = new RemotingCommand();
        {
            Unsafe unsafe = UnsafeUtils.theUnsafe;
            cmd.setCode(unsafe.getShort(retBufferAddr));
            // LanguageCode language
            cmd.setLanguage(LanguageCode.valueOf(unsafe.getByte(retBufferAddr + 2)));
            // int version(~32767)
            cmd.setVersion(unsafe.getShort(retBufferAddr + 3));
            // int opaque
            cmd.setOpaque(unsafe.getInt(retBufferAddr + 5));
            // int flag
            cmd.setFlag(unsafe.getInt(retBufferAddr + 9));

            int remarkLen = unsafe.getInt(retBufferAddr + 13);
            if (remarkLen > 0) {
                int remarkPosition = unsafe.getInt(retBufferAddr + 17);
                byte[] remarkBytes = new byte[remarkLen];
                unsafe.copyMemory(null, headerBufferAddr + remarkPosition, remarkBytes, UnsafeUtils.BYTE_ARRAY_BASE_OFFSET, remarkLen);
                cmd.setRemark(UnsafeString.genString(remarkBytes));
            }

            AsyncStringHashMap extFields = new AsyncStringHashMap();
            int extFieldsLen = unsafe.getInt(retBufferAddr + 21);
            if (extFieldsLen > 0) {
                int extFieldsPosition = unsafe.getInt(retBufferAddr + 25);
                byte[] extFieldsBytes = new byte[extFieldsLen];
                unsafe.copyMemory(null, headerBufferAddr + extFieldsPosition, extFieldsBytes, UnsafeUtils.BYTE_ARRAY_BASE_OFFSET, extFieldsLen);
                extFields.setJsonData(extFieldsBytes);

                switch (cmd.getCode()) {
                    case RequestCode.SEND_BATCH_MESSAGE:
                    case RequestCode.SEND_MESSAGE_V2:
                        SendMessageRequestHeader sendMessageRequestHeader = extFields.doSendMessageHeaderInit();
                        cmd.setCustomHeaderInRequest(sendMessageRequestHeader);
                        break;
                    case RequestCode.PULL_MESSAGE:
                        PullMessageRequestHeader pullMessageRequestHeader = extFields.doPullMessageHeaderInit();
                        cmd.setCustomHeaderInRequest(pullMessageRequestHeader);
                        break;
                    default:
                        extFields.doDefaultInit();
                }
            }
            cmd.setExtFields(extFields);
        }

        return cmd;
    }

    static class ByteBufBufferWriter {
        ByteBuf buf;
        byte[] buffer;
        int cap;
        int ptr;

        public ByteBufBufferWriter(ByteBuf buf, int cap) {
            this.buf = buf;
            this.cap = cap;
            this.buffer = new byte[cap];
        }

        public void writeBytes(byte[] bytes) {
            int dataPtr = 0;
            while (dataPtr < bytes.length) {
                int n = Math.min(remain(), bytes.length - dataPtr);
                System.arraycopy(bytes, dataPtr, buffer, ptr, n);
                dataPtr += n;
                ptr += n;
            }
        }

        public void writeByte(byte b) {
            if (ptr >= cap) {
                this.flush();
            }
            buffer[ptr++] = b;
        }

        public void flush() {
            buf.writeBytes(buffer, 0, ptr);
            ptr = 0;
        }

        private int remain() {
            int ret = cap - ptr;
            if (ret == 0) {
                this.flush();
                ret = cap - ptr;
            }
            return ret;
        }
    }

    private static void writeMap(ByteBuf out, Map<String, String> map) {
        if (map != null && !map.isEmpty()) {
            map.forEach((k, v) -> {
                out.writeBytes("\"".getBytes());
                out.writeBytes(UnsafeString.unsafeGetBytes(k));
                out.writeBytes("\":\"".getBytes());
                UnsafeUtils.writeJsonStr(out, UnsafeString.unsafeGetBytes(v));
                out.writeBytes("\",".getBytes());
            });
        }
    }

    public static void fastJsonEncode(ByteBuf out, RemotingCommand cmd) {
        out.writeBytes("{\"code\":".getBytes());
        UnsafeUtils.writeUnsignedInt(out, cmd.getCode());

        CommandCustomHeader customHeader = cmd.readCustomHeader();
        if (customHeader instanceof SendMessageResponseHeader) {
            out.writeBytes(",\"extFields\":{".getBytes());
            ((SendMessageResponseHeader) customHeader).encodeJson(out);
            Map<String, String> extFields = cmd.getExtFields();
            writeMap(out, extFields);
            out.writerIndex(out.writerIndex() - 1);
            out.writeByte('}');
        } else {
            cmd.makeCustomHeaderToNet();
            Map<String, String> extFields = cmd.getExtFields();
            if (extFields != null && !extFields.isEmpty()) {
                out.writeBytes(",\"extFields\":{".getBytes());
                writeMap(out, extFields);
                out.writerIndex(out.writerIndex() - 1);
                out.writeByte('}');
            }
        }

        out.writeBytes(",\"flag\":".getBytes());
        UnsafeUtils.writeUnsignedInt(out, cmd.getFlag());

        out.writeBytes(",\"language\":\"".getBytes());
        out.writeBytes(UnsafeString.unsafeGetBytes(cmd.getLanguage().toString()));

        out.writeBytes("\",\"opaque\":".getBytes());
        UnsafeUtils.writeUnsignedInt(out, cmd.getOpaque());

        String remark = cmd.getRemark();
        if (remark != null) {
            out.writeBytes(",\"remark\":\"".getBytes());
            out.writeBytes(UnsafeString.unsafeGetBytes(remark));
            out.writeByte('\"');
        }

        out.writeBytes(",\"serializeTypeCurrentRPC\":\"".getBytes());
        out.writeBytes(UnsafeString.unsafeGetBytes(cmd.getSerializeTypeCurrentRPC().toString()));

        out.writeBytes("\",\"version\":".getBytes());
        UnsafeUtils.writeUnsignedInt(out, cmd.getVersion());
        out.writeByte('}');
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
            System.load("/home/admin/so/libRemotingCommandDecode.so");
        }
    }
}
