package cn.abelib.minedb.index.fs;

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
     * 当前记录的前一个记录位置
     */
    private int prev;
    /**
     * 当前记录的下一个记录位置
     */
    private int next;
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

    private Record(KeyValue keyValue, int position) {
        this.position = position;
        
    }

    /**
     * 返回每个记录的ByteBuffer对象
     * @return
     */
    public ByteBuffer byteBuffer() {
        return null;
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

    public int getPrev() {
        return prev;
    }

    public void setPrev(int prev) {
        this.prev = prev;
    }

    public int getNext() {
        return next;
    }

    public void setNext(int next) {
        this.next = next;
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
