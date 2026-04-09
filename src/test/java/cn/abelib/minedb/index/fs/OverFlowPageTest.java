package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class OverFlowPageTest {
    private Configuration conf;

    @Before
    public void init() {
        conf = new Configuration();
    }

    @Test
    public void testCreateOverFlowPage() {
        OverFlowPage page = new OverFlowPage(conf, 1024L);

        Assert.assertEquals(1024L, page.getPosition());
        Assert.assertEquals(-1L, page.getNextPage());
        Assert.assertEquals(0, page.getDataLen());
    }

    @Test
    public void testSetData() {
        OverFlowPage page = new OverFlowPage(conf, 0);
        byte[] data = "Hello World".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        page.setData(data);

        Assert.assertEquals(data.length, page.getDataLen());
        Assert.assertArrayEquals(data, page.getData());
    }

    @Test
    public void testSetDataPartial() {
        OverFlowPage page = new OverFlowPage(conf, 0);
        int availableSpace = OverFlowPage.getAvailableDataSpace(conf);
        byte[] data = new byte[availableSpace / 2];

        page.setData(data);

        Assert.assertEquals(data.length, page.getDataLen());
    }

    @Test
    public void testSerializeDeserialize() {
        OverFlowPage original = new OverFlowPage(conf, 4096L);
        byte[] data = "Test data for serialization".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        original.setData(data);
        original.setNextPage(8192L);

        ByteBuffer buffer = original.byteBuffer();
        OverFlowPage deserialized = new OverFlowPage(conf, buffer);

        Assert.assertEquals(original.getPosition(), deserialized.getPosition());
        Assert.assertEquals(original.getDataLen(), deserialized.getDataLen());
        Assert.assertEquals(original.getNextPage(), deserialized.getNextPage());
        Assert.assertArrayEquals(data, deserialized.getData());
    }

    @Test
    public void testCalculateOverflowPageCount() {
        int availableSpace = OverFlowPage.getAvailableDataSpace(conf);

        // Data that fits in one page
        int count1 = OverFlowPage.calculateOverflowPageCount(conf, availableSpace - 100);
        Assert.assertEquals(1, count1);

        // Data that needs two pages
        int count2 = OverFlowPage.calculateOverflowPageCount(conf, availableSpace + 100);
        Assert.assertEquals(2, count2);

        // Data that needs multiple pages
        int count3 = OverFlowPage.calculateOverflowPageCount(conf, availableSpace * 3 + 1);
        Assert.assertEquals(4, count3);
    }

    @Test
    public void testCreateOverflowPages() {
        int availableSpace = OverFlowPage.getAvailableDataSpace(conf);
        byte[] data = new byte[availableSpace * 2 + 100]; // Needs 3 pages

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        List<OverFlowPage> pages = OverFlowPage.createOverflowPages(conf, data, 4096L);

        Assert.assertEquals(3, pages.size());

        // Verify chain
        Assert.assertEquals(4096L + conf.getPageSize(), pages.get(0).getNextPage());
        Assert.assertEquals(4096L + conf.getPageSize() * 2, pages.get(1).getNextPage());
        Assert.assertEquals(-1L, pages.get(2).getNextPage());
    }

    @Test
    public void testMergeData() {
        byte[] data1 = "Hello ".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] data2 = "World!".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        OverFlowPage page1 = new OverFlowPage(conf, 0);
        page1.setData(data1);

        OverFlowPage page2 = new OverFlowPage(conf, 0);
        page2.setData(data2);

        List<OverFlowPage> pages = List.of(page1, page2);
        byte[] merged = OverFlowPage.mergeData(pages);

        Assert.assertEquals("Hello World!", new String(merged, java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    public void testLargeDataRoundTrip() {
        int availableSpace = OverFlowPage.getAvailableDataSpace(conf);
        byte[] originalData = new byte[availableSpace * 2];

        for (int i = 0; i < originalData.length; i++) {
            originalData[i] = (byte) (i % 256);
        }

        List<OverFlowPage> pages = OverFlowPage.createOverflowPages(conf, originalData, 0);
        byte[] recovered = OverFlowPage.mergeData(pages);

        Assert.assertArrayEquals(originalData, recovered);
    }

    @Test
    public void testGetAvailableDataSpace() {
        int available = OverFlowPage.getAvailableDataSpace(conf);

        // pageSize(16KB) - headerSize(24) = 16360
        Assert.assertEquals(16 * 1024 - 24, available);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDataTooLarge() {
        OverFlowPage page = new OverFlowPage(conf, 0);
        int availableSpace = OverFlowPage.getAvailableDataSpace(conf);
        byte[] tooLargeData = new byte[availableSpace + 1];

        page.setData(tooLargeData);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMagic() {
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        buffer.putLong(0); // position
        buffer.putInt(0x12345678); // wrong magic
        buffer.putInt(0); // dataLen
        buffer.putLong(-1); // nextPage
        buffer.flip();

        new OverFlowPage(conf, buffer);
    }
}
