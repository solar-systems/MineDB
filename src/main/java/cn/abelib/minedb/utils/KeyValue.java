package cn.abelib.minedb.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @Author: abel.huang
 * @Date: 2020-11-03 01:07
 */
public class KeyValue implements Comparable<KeyValue> {
    private String key;
    private String value;

    public KeyValue(String key, String value) {
        this(key);
        this.value = value;
    }

    public KeyValue(String key) {
        this.key = key;
        this.value = StringUtils.EMPTY;
    }

    public KeyValue(KeyValue keyValue) {
        this.key = keyValue.getKey();
        this.value = keyValue.getValue();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public int compareTo(KeyValue keyValue) {
        return this.key.compareTo(keyValue.key);
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
