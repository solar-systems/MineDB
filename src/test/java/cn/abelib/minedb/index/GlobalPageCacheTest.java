package cn.abelib.minedb.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class GlobalPageCacheTest {

    @Before
    public void init() {
        GlobalPageCache.getInstance().clear();
    }

    @After
    public void cleanup() {
        GlobalPageCache.getInstance().clear();
    }

    @Test
    public void testGetInstance() {
        GlobalPageCache instance1 = GlobalPageCache.getInstance();
        GlobalPageCache instance2 = GlobalPageCache.getInstance();

        assertSame(instance1, instance2);
    }

    @Test
    public void testPutAndGetDirtyPage() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);

        GlobalPageCache.putDirtyPage(node);

        assertEquals(1, GlobalPageCache.getInstance().getDirtyPageCount());
        assertEquals(0, GlobalPageCache.getInstance().getCleanPageCount());

        TreeNode retrieved = GlobalPageCache.getPage(1024);
        assertSame(node, retrieved);
    }

    @Test
    public void testPutCleanPage() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);

        GlobalPageCache.putCleanPage(node);

        assertEquals(0, GlobalPageCache.getInstance().getDirtyPageCount());
        assertEquals(1, GlobalPageCache.getInstance().getCleanPageCount());

        TreeNode retrieved = GlobalPageCache.getPage(1024);
        assertSame(node, retrieved);
    }

    @Test
    public void testDirtyToCleanTransition() throws Exception {
        Configuration conf = new Configuration();
        TreeNode node = new TreeNode(conf, true, true, 0);
        node.setPosition(1024);

        GlobalPageCache.putDirtyPage(node);
        assertEquals(1, GlobalPageCache.getInstance().getDirtyPageCount());

        GlobalPageCache.putCleanPage(node);
        assertEquals(0, GlobalPageCache.getInstance().getDirtyPageCount());
        assertEquals(1, GlobalPageCache.getInstance().getCleanPageCount());
    }

    @Test
    public void testClear() throws Exception {
        Configuration conf = new Configuration();

        TreeNode node1 = new TreeNode(conf, true, true, 0);
        node1.setPosition(1024);
        TreeNode node2 = new TreeNode(conf, true, false, 1);
        node2.setPosition(2048);

        GlobalPageCache.putDirtyPage(node1);
        GlobalPageCache.putCleanPage(node2);

        assertEquals(2, GlobalPageCache.getInstance().getTotalPageCount());

        GlobalPageCache.getInstance().clear();

        assertEquals(0, GlobalPageCache.getInstance().getDirtyPageCount());
        assertEquals(0, GlobalPageCache.getInstance().getCleanPageCount());
        assertEquals(0, GlobalPageCache.getInstance().getTotalPageCount());
    }

    @Test
    public void testGetPageNotFound() {
        TreeNode retrieved = GlobalPageCache.getPage(99999);
        assertNull(retrieved);
    }
}
