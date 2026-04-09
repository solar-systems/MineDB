package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class FreePageListTest {
    private Configuration conf;

    @Before
    public void init() {
        conf = new Configuration();
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
}
