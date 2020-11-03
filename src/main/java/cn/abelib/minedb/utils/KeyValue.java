package cn.abelib.minedb.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: abel.huang
 * @Date: 2020-11-03 01:07
 */
public class KeyValue implements Comparable<KeyValue> {
    private String key;
    private List<Object> values;

    public KeyValue(String key, Object value) {
        this(key);
        this.values.add(value);
    }

    public KeyValue(String key) {
        this.key = key;
        this.values = new ArrayList<>();
    }

    public KeyValue(KeyValue keyValue) {
        this.key = keyValue.getKey();
        this.values = keyValue.getValues();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public int compareTo(KeyValue keyValue) {
        return this.key.compareTo(keyValue.key);
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key='" + key + '\'' +
                ", values=" + values +
                '}';
    }
}
