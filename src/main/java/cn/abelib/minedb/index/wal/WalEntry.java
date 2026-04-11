package cn.abelib.minedb.index.wal;

import cn.abelib.minedb.utils.ByteUtils;

import java.nio.ByteBuffer;

/**
 * WAL 日志条目
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class WalEntry {
    /** 操作类型 */
    public enum Type {
        PUT((byte) 1),
        DELETE((byte) 2),
        SYNC((byte) 3),      // 检查点标记
        TX_BEGIN((byte) 4),  // 事务开始
        TX_COMMIT((byte) 5), // 事务提交
        TX_ROLLBACK((byte) 6); // 事务回滚

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static Type fromCode(byte code) {
            for (Type type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown WAL entry type: " + code);
        }
    }

    /** 日志头大小：magic(4) + type(1) + keyLen(4) + valueLen(4) + checksum(4) + sequence(8) = 25 */
    public static final int HEADER_SIZE = 25;
    private static final int MAGIC = 0x57414C00; // "WAL\0"

    private final Type type;
    private final String key;
    private final String value;
    private final long sequence;
    private final int checksum;

    public WalEntry(Type type, String key, String value, long sequence) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.sequence = sequence;
        this.checksum = calculateChecksum();
    }

    private WalEntry(Type type, String key, String value, long sequence, int checksum) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.sequence = sequence;
        this.checksum = checksum;
    }

    /**
     * 计算校验和
     */
    private int calculateChecksum() {
        int sum = type.getCode();
        if (key != null) {
            sum ^= key.hashCode();
        }
        if (value != null) {
            sum ^= value.hashCode();
        }
        sum ^= (int) (sequence ^ (sequence >>> 32));
        return sum;
    }

    /**
     * 验证校验和
     */
    public boolean validate() {
        return checksum == calculateChecksum();
    }

    /**
     * 序列化为 ByteBuffer
     */
    public ByteBuffer toByteBuffer() {
        byte[] keyBytes = key != null ? ByteUtils.getBytesUTF8(key) : new byte[0];
        byte[] valueBytes = value != null ? ByteUtils.getBytesUTF8(value) : new byte[0];

        int totalSize = HEADER_SIZE + keyBytes.length + valueBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // 写入头部
        buffer.putInt(MAGIC);
        buffer.put(type.getCode());
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueBytes.length);
        buffer.putInt(checksum);
        buffer.putLong(sequence);

        // 写入数据
        buffer.put(keyBytes);
        buffer.put(valueBytes);

        buffer.flip();
        return buffer;
    }

    /**
     * 从 ByteBuffer 反序列化
     */
    public static WalEntry fromByteBuffer(ByteBuffer buffer) {
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid WAL entry magic: " + magic);
        }

        byte typeCode = buffer.get();
        Type type = Type.fromCode(typeCode);

        int keyLen = buffer.getInt();
        int valueLen = buffer.getInt();
        int checksum = buffer.getInt();
        long sequence = buffer.getLong();

        String key = null;
        String value = null;

        if (keyLen > 0) {
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            key = ByteUtils.bytes2UTF8(keyBytes);
        }

        if (valueLen > 0) {
            byte[] valueBytes = new byte[valueLen];
            buffer.get(valueBytes);
            value = ByteUtils.bytes2UTF8(valueBytes);
        }

        return new WalEntry(type, key, value, sequence, checksum);
    }

    /**
     * 获取总大小
     */
    public int getSize() {
        int keyLen = key != null ? ByteUtils.getBytesUTF8(key).length : 0;
        int valueLen = value != null ? ByteUtils.getBytesUTF8(value).length : 0;
        return HEADER_SIZE + keyLen + valueLen;
    }

    // Getters
    public Type getType() { return type; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    public long getSequence() { return sequence; }
    public int getChecksum() { return checksum; }
}
