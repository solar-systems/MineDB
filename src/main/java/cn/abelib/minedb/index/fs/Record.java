package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.utils.ByteUtils;
import cn.abelib.minedb.utils.KeyValue;

import java.nio.ByteBuffer;

/**
 * 磁盘记录结构
 *
 * @Author: abel.huang
 * @Date: 2020-11-15 00:17
 */
public class Record {
    private static final byte OVERFLOW_FLAG = 1;

    private int position;
    private int totalLen;
    private int keySize;
    private byte[] key;
    private int valueSize;
    private byte[] value;
    private boolean overflow;
    private long overflowPage;

    public Record() {}

    public Record(KeyValue keyValue, int position) {
        this(keyValue, position, false, 0);
    }

    public Record(KeyValue keyValue, int position, boolean overflow, long overflowPage) {
        this.position = position;
        this.key = ByteUtils.getBytesUTF8(keyValue.getKey());
        this.keySize = key.length;
        this.value = ByteUtils.getBytesUTF8(keyValue.getValue());
        this.valueSize = value.length;
        this.overflow = overflow;
        this.overflowPage = overflowPage;

        this.totalLen = 16 + keySize + 4; // header + key + valueSize
        if (overflow) {
            this.totalLen += 9; // overflow flag + overflow page
        } else {
            this.totalLen += valueSize;
        }
    }

    public Record(ByteBuffer buffer) {
        this.position = buffer.getInt();
        this.totalLen = buffer.getInt();
        this.keySize = buffer.getInt();
        this.key = ByteUtils.toBytes(buffer, keySize);
        this.valueSize = buffer.getInt();

        // Check if overflow
        if (buffer.remaining() > 0 && buffer.get(buffer.position()) == OVERFLOW_FLAG) {
            this.overflow = true;
            buffer.get(); // skip overflow flag
            this.overflowPage = buffer.getLong();
            this.value = new byte[0];
        } else {
            this.overflow = false;
            this.overflowPage = 0;
            this.value = ByteUtils.toBytes(buffer, valueSize);
        }
    }

    public ByteBuffer byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        buffer.putInt(position);
        buffer.putInt(totalLen);
        buffer.putInt(keySize);
        buffer.put(key);
        buffer.putInt(valueSize);
        if (overflow) {
            buffer.put(OVERFLOW_FLAG);
            buffer.putLong(overflowPage);
        } else {
            buffer.put(value);
        }
        buffer.flip();
        return buffer;
    }

    public static int getFixedHeaderSize() {
        return 16; // position(4) + totalLen(4) + keySize(4) + valueSize(4)
    }

    public static int getOverflowOverhead() {
        return 9; // overflow flag(1) + overflow page(8)
    }

    public static int calculateRecordSize(int keySize, int valueSize, boolean overflow) {
        int size = getFixedHeaderSize() + keySize + 4; // header + key + valueSize
        if (overflow) {
            size += getOverflowOverhead();
        } else {
            size += valueSize;
        }
        return size;
    }

    // Getters and Setters
    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }
    public int getTotalLen() { return totalLen; }
    public void setTotalLen(int totalLen) { this.totalLen = totalLen; }
    public int getKeySize() { return keySize; }
    public byte[] getKey() { return key; }
    public int getValueSize() { return valueSize; }
    public byte[] getValue() { return value; }
    public boolean isOverflow() { return overflow; }
    public long getOverflowPage() { return overflowPage; }
}
