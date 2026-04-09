package cn.abelib.minedb.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class KeyValueTest {

    @Test
    public void testCreateKeyValue() {
        KeyValue kv = new KeyValue("key", "value");

        assertEquals("key", kv.getKey());
        assertEquals("value", kv.getValue());
    }

    @Test
    public void testCreateKeyValueWithKeyOnly() {
        KeyValue kv = new KeyValue("key");

        assertEquals("key", kv.getKey());
        assertEquals("", kv.getValue());
    }

    @Test
    public void testCopyConstructor() {
        KeyValue original = new KeyValue("key", "value");
        KeyValue copy = new KeyValue(original);

        assertEquals(original.getKey(), copy.getKey());
        assertEquals(original.getValue(), copy.getValue());
    }

    @Test
    public void testSetters() {
        KeyValue kv = new KeyValue("key", "value");

        kv.setKey("newKey");
        assertEquals("newKey", kv.getKey());

        kv.setValue("newValue");
        assertEquals("newValue", kv.getValue());
    }

    @Test
    public void testCompareTo() {
        KeyValue kv1 = new KeyValue("a", "value1");
        KeyValue kv2 = new KeyValue("b", "value2");
        KeyValue kv3 = new KeyValue("a", "value3");

        assertTrue(kv1.compareTo(kv2) < 0);
        assertTrue(kv2.compareTo(kv1) > 0);
        assertEquals(0, kv1.compareTo(kv3));
    }

    @Test
    public void testCompareToSameKey() {
        KeyValue kv1 = new KeyValue("key", "value1");
        KeyValue kv2 = new KeyValue("key", "value2");

        assertEquals(0, kv1.compareTo(kv2));
    }

    @Test
    public void testToString() {
        KeyValue kv = new KeyValue("key", "value");
        String str = kv.toString();

        assertTrue(str.contains("key"));
        assertTrue(str.contains("value"));
    }
}
