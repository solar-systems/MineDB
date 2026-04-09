package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.utils.KeyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @Author: abel.huang
 * @Date: 2020-11-15 00:17
 */
public class RecordTest {
    private Record record;

    @Before
    public void init() {
        KeyValue keyValue = new KeyValue("foo", "bar");
        record = new Record(keyValue, 1024);
    }

    @Test
    public void recordTest() {
        ByteBuffer buffer = record.byteBuffer();
        // byteBuffer() already flips the buffer, no need to flip again
        Record record1 = new Record(buffer);
        Assert.assertEquals(record1.getPosition(), record.getPosition());
        Assert.assertEquals(record1.getTotalLen(), record.getTotalLen());
        Assert.assertEquals(record1.getKeySize(), record.getKeySize());
        Assert.assertArrayEquals(record1.getKey(), record.getKey());
        Assert.assertEquals(record1.getValueSize(), record.getValueSize());
        Assert.assertArrayEquals(record1.getValue(), record.getValue());
    }

    @Test
    public void testOverflowRecord() {
        KeyValue keyValue = new KeyValue("testkey", "testvalue");
        Record overflowRecord = new Record(keyValue, 2048, true, 4096L);

        Assert.assertTrue(overflowRecord.isOverflow());
        Assert.assertEquals(4096L, overflowRecord.getOverflowPage());

        ByteBuffer buffer = overflowRecord.byteBuffer();
        Record deserialized = new Record(buffer);

        Assert.assertTrue(deserialized.isOverflow());
        Assert.assertEquals(4096L, deserialized.getOverflowPage());
        Assert.assertEquals("testkey", new String(deserialized.getKey(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    public void testCalculateRecordSizeNormal() {
        KeyValue keyValue = new KeyValue("foo", "bar");
        int keySize = "foo".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        int valueSize = "bar".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

        // header(16) + keySize(3) + valueSize(4) + value(3) = 26
        int expected = 16 + keySize + 4 + valueSize;
        int actual = Record.calculateRecordSize(keySize, valueSize, false);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCalculateRecordSizeOverflow() {
        int keySize = 10;
        // overflow: header(16) + keySize(10) + valueSize(4) + overflow flag(1) + overflow page(8) = 39
        int expected = 16 + keySize + 4 + Record.getOverflowOverhead();
        int actual = Record.calculateRecordSize(keySize, 1000, true);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testEmptyKeyValue() {
        KeyValue keyValue = new KeyValue("", "");
        Record emptyRecord = new Record(keyValue, 0);

        Assert.assertEquals(0, emptyRecord.getKeySize());
        Assert.assertEquals(0, emptyRecord.getValueSize());
        // header(16) + keySize(0) + valueSize(4) + value(0) = 20
        Assert.assertEquals(20, emptyRecord.getTotalLen());
    }

    @Test
    public void testLargeKey() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("a");
        }
        String largeKey = sb.toString();
        KeyValue keyValue = new KeyValue(largeKey, "value");
        Record largeRecord = new Record(keyValue, 1024);

        Assert.assertEquals(100, largeRecord.getKeySize());
        ByteBuffer buffer = largeRecord.byteBuffer();
        Record deserialized = new Record(buffer);

        Assert.assertEquals(largeKey, new String(deserialized.getKey(), java.nio.charset.StandardCharsets.UTF_8));
    }
}
