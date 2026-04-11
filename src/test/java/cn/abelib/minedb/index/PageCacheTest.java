package cn.abelib.minedb.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 页缓存测试（支持 LRU 淘汰）
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class PageCacheTest {
    private PageCache pageCache;

    @Before
    public void init() {
        pageCache = new PageCache();
    }

    @After
    public void cleanup() {
        if (pageCache != null) {
            pageCache.clear();
        }
    }

    @Test
    public void testPutAndGetDirtyPage() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);

        pageCache.putDirtyPage(node);

        assertEquals(1, pageCache.getDirtyPageCount());
        assertEquals(0, pageCache.getCleanPageCount());

        TreeNode retrieved = pageCache.getPage(1024);
        assertSame(node, retrieved);
    }

    @Test
    public void testPutAndGetCleanPage() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);

        pageCache.putCleanPage(node);

        assertEquals(0, pageCache.getDirtyPageCount());
        assertEquals(1, pageCache.getCleanPageCount());

        TreeNode retrieved = pageCache.getPage(1024);
        assertSame(node, retrieved);
    }

    @Test
    public void testDirtyToCleanTransition() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);

        pageCache.putDirtyPage(node);
        assertEquals(1, pageCache.getDirtyPageCount());

        pageCache.putCleanPage(node);
        assertEquals(0, pageCache.getDirtyPageCount());
        assertEquals(1, pageCache.getCleanPageCount());
    }

    @Test
    public void testClear() throws Exception {
        Configuration conf = new Configuration();

        TreeNode node1 = new TreeNode(conf, true, true, 0);
        node1.setPosition(1024);
        node1.setPageCache(pageCache);
        TreeNode node2 = new TreeNode(conf, true, false, 1);
        node2.setPosition(2048);
        node2.setPageCache(pageCache);

        pageCache.putDirtyPage(node1);
        pageCache.putCleanPage(node2);

        assertEquals(2, pageCache.getTotalPageCount());

        pageCache.clear();

        assertEquals(0, pageCache.getDirtyPageCount());
        assertEquals(0, pageCache.getCleanPageCount());
        assertEquals(0, pageCache.getTotalPageCount());
    }

    @Test
    public void testGetPageNotFound() {
        TreeNode retrieved = pageCache.getPage(99999);
        assertNull(retrieved);
    }

    @Test
    public void testContainsPage() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);

        assertFalse(pageCache.containsPage(1024));

        pageCache.putDirtyPage(node);
        assertTrue(pageCache.containsPage(1024));
    }

    @Test
    public void testRemovePage() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);

        pageCache.putDirtyPage(node);
        assertTrue(pageCache.containsPage(1024));

        pageCache.removePage(1024);
        assertFalse(pageCache.containsPage(1024));
    }

    // ==================== LRU 测试 ====================

    @Test
    public void testLruEviction() throws Exception {
        // 创建容量为 3 的缓存
        pageCache = new PageCache(3);
        Configuration conf = new Configuration();

        // 添加 3 个净页
        for (int i = 0; i < 3; i++) {
            TreeNode node = new TreeNode(conf, true, true, i);
            node.setPosition(i * 1000);
            node.setPageCache(pageCache);
            pageCache.putCleanPage(node);
        }

        assertEquals(3, pageCache.getCleanPageCount());
        assertEquals(0, pageCache.getEvictionCount());

        // 添加第 4 个，应该触发淘汰
        TreeNode node4 = new TreeNode(conf, true, true, 3);
        node4.setPosition(3000);
        node4.setPageCache(pageCache);
        pageCache.putCleanPage(node4);

        assertEquals(3, pageCache.getCleanPageCount()); // 仍然是 3
        assertEquals(1, pageCache.getEvictionCount()); // 淘汰了 1 个

        // 第一个页面应该被淘汰
        assertNull(pageCache.getPage(0));
    }

    @Test
    public void testLruAccessOrder() throws Exception {
        // 创建容量为 3 的缓存
        pageCache = new PageCache(3);
        Configuration conf = new Configuration();

        // 添加 3 个净页
        for (int i = 0; i < 3; i++) {
            TreeNode node = new TreeNode(conf, true, true, i);
            node.setPosition(i * 1000);
            node.setPageCache(pageCache);
            pageCache.putCleanPage(node);
        }

        // 访问第一个页面，使其变为最近使用
        pageCache.getPage(0);

        // 添加第 4 个，应该淘汰第二个（最久未使用）
        TreeNode node4 = new TreeNode(conf, true, true, 3);
        node4.setPosition(3000);
        node4.setPageCache(pageCache);
        pageCache.putCleanPage(node4);

        // 第一个页面应该还在（因为刚被访问过）
        assertNotNull(pageCache.getPage(0));
        // 第二个页面应该被淘汰
        assertNull(pageCache.getPage(1000));
    }

    @Test
    public void testDirtyPageNotEvictedDirectly() throws Exception {
        // 创建容量为 2 的缓存
        pageCache = new PageCache(2);
        Configuration conf = new Configuration();

        // 添加 1 个脏页和 1 个净页
        TreeNode dirtyNode = new TreeNode(conf, true, true, 0);
        dirtyNode.setPosition(0);
        dirtyNode.setPageCache(pageCache);
        pageCache.putDirtyPage(dirtyNode);

        TreeNode cleanNode = new TreeNode(conf, true, true, 1);
        cleanNode.setPosition(1000);
        cleanNode.setPageCache(pageCache);
        pageCache.putCleanPage(cleanNode);

        // 添加第 3 个净页，应该淘汰净页而不是脏页
        TreeNode node3 = new TreeNode(conf, true, true, 2);
        node3.setPosition(2000);
        node3.setPageCache(pageCache);
        pageCache.putCleanPage(node3);

        // 脏页应该还在
        assertNotNull(pageCache.getPage(0));
        assertEquals(1, pageCache.getDirtyPageCount());
    }

    @Test
    public void testCacheStats() throws Exception {
        pageCache = new PageCache(100);
        Configuration conf = new Configuration();

        // 添加页面
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);
        pageCache.putCleanPage(node);

        // 命中
        pageCache.getPage(1024);
        pageCache.getPage(1024);
        assertEquals(2, pageCache.getHitCount());

        // 未命中
        pageCache.getPage(9999);
        assertEquals(1, pageCache.getMissCount());

        // 命中率
        double hitRate = pageCache.getHitRate();
        assertEquals(2.0 / 3.0, hitRate, 0.001);

        // 重置统计
        pageCache.resetStats();
        assertEquals(0, pageCache.getHitCount());
        assertEquals(0, pageCache.getMissCount());
    }

    @Test
    public void testDefaultMaxPages() {
        assertEquals(PageCache.DEFAULT_MAX_PAGES, pageCache.getMaxPages());
    }

    @Test
    public void testCustomMaxPages() {
        PageCache customCache = new PageCache(500);
        assertEquals(500, customCache.getMaxPages());
        customCache.clear();
    }

    @Test
    public void testInvalidMaxPages() {
        // 传入 0 或负数应该使用默认值
        PageCache cache1 = new PageCache(0);
        assertEquals(PageCache.DEFAULT_MAX_PAGES, cache1.getMaxPages());

        PageCache cache2 = new PageCache(-100);
        assertEquals(PageCache.DEFAULT_MAX_PAGES, cache2.getMaxPages());

        cache1.clear();
        cache2.clear();
    }

    @Test
    public void testToString() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);
        node.setPageCache(pageCache);
        pageCache.putCleanPage(node);
        pageCache.getPage(1024);

        String str = pageCache.toString();
        assertTrue(str.contains("maxPages="));
        assertTrue(str.contains("cleanPages=1"));
        assertTrue(str.contains("hitCount=1"));
    }

    @Test
    public void testMultipleEvictions() throws Exception {
        // 创建容量为 2 的缓存
        pageCache = new PageCache(2);
        Configuration conf = new Configuration();

        // 连续添加 10 个页面
        for (int i = 0; i < 10; i++) {
            TreeNode node = new TreeNode(conf, true, true, i);
            node.setPosition(i * 1000);
            node.setPageCache(pageCache);
            pageCache.putCleanPage(node);
        }

        // 应该淘汰了 8 个
        assertEquals(8, pageCache.getEvictionCount());
        assertEquals(2, pageCache.getCleanPageCount());
    }
}
