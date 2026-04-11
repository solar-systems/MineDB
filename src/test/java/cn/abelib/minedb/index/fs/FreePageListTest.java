package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class FreePageListTest {
    private static final String TEST_DB = "test_freepagelist.db";
    private Configuration conf;

    @Before
    public void init() throws IOException {
        cleanup();
        conf = new Configuration();
        conf.setDbName(TEST_DB);
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get(TEST_DB));
    }

    @Test
    public void testCreateFreePageList() {
        FreePageList list = new FreePageList(conf);

        assertEquals(0, list.getFreePageCount());
        assertTrue(list.getNextAllocPosition() > 0);
    }

    @Test
    public void testAllocatePage() {
        FreePageList list = new FreePageList(conf);

        long page1 = list.allocatePage();
        long page2 = list.allocatePage();

        assertTrue(page1 > 0);
        assertTrue(page2 > 0);
        assertTrue(page2 > page1);
        assertEquals(conf.getPageSize(), page2 - page1);
    }

    @Test
    public void testAllocatePages() {
        FreePageList list = new FreePageList(conf);

        List<Long> pages = list.allocatePages(5);

        assertEquals(5, pages.size());
        // Pages should be contiguous
        for (int i = 1; i < pages.size(); i++) {
            assertEquals(conf.getPageSize(), pages.get(i) - pages.get(i - 1));
        }
    }

    @Test
    public void testFreeAndReusePage() {
        FreePageList list = new FreePageList(conf);

        long allocated = list.allocatePage();
        assertEquals(0, list.getFreePageCount());

        list.freePage(allocated);
        assertEquals(1, list.getFreePageCount());

        // Next allocation should reuse the freed page
        long reused = list.allocatePage();
        assertEquals(allocated, reused);
        assertEquals(0, list.getFreePageCount());
    }

    @Test
    public void testFreeMultiplePages() {
        FreePageList list = new FreePageList(conf);

        List<Long> pages = list.allocatePages(3);
        assertEquals(0, list.getFreePageCount());

        list.freePages(pages);
        assertEquals(3, list.getFreePageCount());
    }

    @Test
    public void testSetAndGetFreePages() {
        FreePageList list = new FreePageList(conf);

        List<Long> pages = List.of(1000L, 2000L, 3000L);
        list.setFreePages(pages);

        assertEquals(3, list.getFreePageCount());
        assertEquals(pages, list.getFreePages());
    }

    @Test
    public void testSetNextAllocPosition() {
        FreePageList list = new FreePageList(conf);

        list.setNextAllocPosition(50000L);
        assertEquals(50000L, list.getNextAllocPosition());
    }

    @Test
    public void testClear() {
        FreePageList list = new FreePageList(conf);

        list.allocatePages(5);
        list.freePages(list.allocatePages(3));

        assertEquals(3, list.getFreePageCount());
        list.clear();
        assertEquals(0, list.getFreePageCount());
    }

    @Test
    public void testSerializeDeserialize() {
        FreePageList original = new FreePageList(conf);
        original.setNextAllocPosition(100000L);
        original.freePage(5000L);
        original.freePage(6000L);
        original.freePage(7000L);

        // 序列化
        ByteBuffer buffer = original.byteBuffer();
        // buffer 已经被 flip，可以直接读取

        // 反序列化
        FreePageList restored = FreePageList.fromByteBuffer(conf, original.getDiskPosition(), buffer);

        assertEquals(original.getFreePageCount(), restored.getFreePageCount());
        assertEquals(original.getNextAllocPosition(), restored.getNextAllocPosition());
        assertEquals(original.getFreePages(), restored.getFreePages());
    }

    @Test
    public void testSerializeEmptyList() {
        FreePageList original = new FreePageList(conf);

        // 序列化空列表
        ByteBuffer buffer = original.byteBuffer();
        // buffer 已经被 flip，可以直接读取

        // 反序列化
        FreePageList restored = FreePageList.fromByteBuffer(conf, original.getDiskPosition(), buffer);

        assertEquals(0, restored.getFreePageCount());
        assertTrue(restored.getNextAllocPosition() > 0);
    }

    @Test
    public void testDiskPosition() {
        FreePageList list = new FreePageList(conf);
        assertEquals(conf.getPageSize(), list.getDiskPosition());

        list.setDiskPosition(5000L);
        assertEquals(5000L, list.getDiskPosition());
    }

    @Test
    public void testInvalidMagic() {
        // 创建一个没有正确魔数的 buffer
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        buffer.putInt(0x12345678); // 错误的魔数
        buffer.putInt(0); // count
        buffer.putLong(0); // nextAllocPosition
        buffer.putLong(0); // reserved
        buffer.flip();

        FreePageList restored = FreePageList.fromByteBuffer(conf, conf.getPageSize(), buffer);

        // 应该返回空列表而不是抛出异常
        assertEquals(0, restored.getFreePageCount());
    }

    @Test
    public void testGetConfiguration() {
        FreePageList list = new FreePageList(conf);
        assertEquals(conf, list.getConfiguration());
    }
}
