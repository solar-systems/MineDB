package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Record;
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
        buffer.flip();
        Record record1 = new Record(buffer);
        Assert.assertEquals(record1.getPosition(), record.getPosition());
        Assert.assertEquals(record1.getTotalLen(), record.getTotalLen());
        Assert.assertEquals(record1.getKeySize(), record.getKeySize());
        Assert.assertArrayEquals(record1.getKey(), record.getKey());
        Assert.assertEquals(record1.getValueSize(), record.getValueSize());
        Assert.assertArrayEquals(record1.getValue(), record.getValue());
    }
}
