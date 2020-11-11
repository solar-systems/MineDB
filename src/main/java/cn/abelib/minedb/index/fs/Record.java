package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.utils.ByteUtils;
import cn.abelib.minedb.utils.KeyValue;

import java.nio.ByteBuffer;

/**
 * @Author: abel.huang
 * @Date: 2020-11-03 23:37
 */
public class Record {
    /**
     * 记录地址
     */
    private int position;
    /**
     * 当前记录总长度
     */
    private int totalLen;
    /**
     * 键长度
     */
    private int keySize;
    /**
     * 键
     */
    private byte[] key;
    /**
     * 值长度
     */
    private int valueSize;
    /**
     * 值
     */
    private byte[] value;

    public Record() {}

    /**
     *
     * @param keyValue
     * @param position
     */
    public Record(KeyValue keyValue, int position) {
        // position + totalLen + keySize + valueSize = 4 * 4 = 16
        this.totalLen = 16;
        this.position = position;
        this.key = ByteUtils.getBytesUTF8(keyValue.getKey());
        this.keySize = key.length;
        this.totalLen += keySize;
        this.value = ByteUtils.getBytesUTF8(keyValue.getValue());
        this.valueSize = value.length;
        this.totalLen += valueSize;
    }

    public Record(ByteBuffer buffer) {
        buffer.flip();
        this.position = buffer.getInt();
        this.totalLen = buffer.getInt();
        this.keySize = buffer.getInt();
        this.key = ByteUtils.toBytes(buffer, keySize);
        this.valueSize = buffer.getInt();
        this.value = ByteUtils.toBytes(buffer, valueSize);
    }


    /**
     * 返回每个记录的ByteBuffer对象
     * @return
     */
    public ByteBuffer byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        buffer.putInt(position);
        buffer.putInt(totalLen);
        buffer.putInt(keySize);
        buffer.put(key);
        buffer.putInt(valueSize);
        buffer.put(value);
        return buffer;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getTotalLen() {
        return totalLen;
    }

    public void setTotalLen(int totalLen) {
        this.totalLen = totalLen;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
