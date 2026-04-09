package cn.abelib.minedb.index;

import cn.abelib.minedb.utils.KeyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class PageUtilsOverflowTest {
    private Configuration conf;

    @Before
    public void init() {
        conf = new Configuration();
    }

    @Test
    public void testNeedOverflowSmallRecord() {
        // Small record should not need overflow
        KeyValue kv = new KeyValue("key", "value");
        Assert.assertFalse(PageUtils.needOverflow(conf, kv));
    }

    @Test
    public void testNeedOverflowMediumRecord() {
        // Medium record that fits in page
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("x");
        }
        KeyValue kv = new KeyValue("key", sb.toString());

        // 1000 chars should still fit in 16KB page
        Assert.assertFalse(PageUtils.needOverflow(conf, kv));
    }

    @Test
    public void testNeedOverflowLargeRecord() {
        // Large record that exceeds page size
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20 * 1024; i++) { // 20KB value
            sb.append("x");
        }
        KeyValue kv = new KeyValue("key", sb.toString());

        Assert.assertTrue(PageUtils.needOverflow(conf, kv));
    }

    @Test
    public void testGetAvailableDataSpace() {
        int available = PageUtils.getAvailableDataSpace(conf);

        // Default: 16KB - 128 bytes header = 16256 bytes
        Assert.assertEquals(16 * 1024 - 128, available);
    }

    @Test
    public void testCalculateOverflowRecordSize() {
        KeyValue kv = new KeyValue("testkey", "largevalue");

        // Overflow record: header + key + valueSize + overflow flag + overflow page
        int size = PageUtils.calculateOverflowRecordSize(kv);
        int keyLen = "testkey".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

        // 16 + 7 + 4 + 9 = 36
        Assert.assertEquals(16 + keyLen + 4 + 9, size);
    }

    @Test
    public void testCalculateOverflowPageCount() {
        // Value that needs multiple overflow pages
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50 * 1024; i++) { // 50KB
            sb.append("x");
        }
        KeyValue kv = new KeyValue("key", sb.toString());

        int pageCount = PageUtils.calculateOverflowPageCount(conf, kv);
        Assert.assertTrue(pageCount >= 2);
    }

    @Test
    public void testNeedOverflowNullKeyValue() {
        Assert.assertFalse(PageUtils.needOverflow(conf, null));
    }

    @Test
    public void testCalculateOverflowPageCountNullKeyValue() {
        Assert.assertEquals(0, PageUtils.calculateOverflowPageCount(conf, null));
    }
}
