package cn.abelib.minedb.index;

import cn.abelib.minedb.utils.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class TreeNodeTest {
    private Configuration conf;
    private TreeNode node;
    private PageCache pageCache;

    @Before
    public void init() throws IOException {
        cleanup();
        conf = new Configuration();
        pageCache = new PageCache();
        node = new TreeNode(conf, true, true, 0);
        node.setPageCache(pageCache);
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get("default.db"));
        if (pageCache != null) {
            pageCache.clear();
        }
    }

    @Test
    public void testCreateLeafRootNode() throws IOException {
        assertTrue(node.isLeaf());
        assertTrue(node.isRoot());
        assertEquals(0L, node.getPageNo());
        assertNotNull(node.getKeyValues());
        assertTrue(node.getKeyValues().isEmpty());
        assertNotNull(node.getChildren());
        assertTrue(node.getChildren().isEmpty());
    }

    @Test
    public void testCreateInternalNode() throws IOException {
        TreeNode internalNode = new TreeNode(conf, false, false, 1);

        assertFalse(internalNode.isLeaf());
        assertFalse(internalNode.isRoot());
        assertEquals(1L, internalNode.getPageNo());
    }

    @Test
    public void testSetters() {
        node.setLeaf(false);
        assertFalse(node.isLeaf());

        node.setRoot(false);
        assertFalse(node.isRoot());

        node.setPageNo(100);
        assertEquals(100L, node.getPageNo());

        node.setPosition(2048);
        assertEquals(2048L, node.getPosition());

        node.setDeleted(true);
        assertTrue(node.isDeleted());

        node.setReadFlag(true);
        assertTrue(node.isReadFlag());
    }

    @Test
    public void testKeyValues() {
        KeyValue kv1 = new KeyValue("key1", "value1");
        KeyValue kv2 = new KeyValue("key2", "value2");

        node.getKeyValues().add(kv1);
        node.getKeyValues().add(kv2);

        assertEquals(2, node.getKeyValues().size());
        assertEquals("key1", node.getKeyValues().get(0).getKey());
        assertEquals("key2", node.getKeyValues().get(1).getKey());
    }

    @Test
    public void testParent() throws IOException {
        TreeNode parent = new TreeNode(conf, false, false, 1);
        parent.setPosition(4096);

        node.setParent(parent);

        assertSame(parent, node.getParent());
    }

    @Test
    public void testPreviousNext() throws IOException {
        TreeNode prev = new TreeNode(conf, true, false, 1);
        prev.setPosition(2048);
        TreeNode next = new TreeNode(conf, true, false, 2);
        next.setPosition(4096);

        node.setPrevious(prev);
        node.setNext(next);

        assertSame(prev, node.getPrevious());
        assertSame(next, node.getNext());
    }

    @Test
    public void testTotal() {
        node.setTotal(100);
        assertEquals(100, node.getTotal());

        node.addTotal(50);
        assertEquals(150, node.getTotal());
    }

    @Test
    public void testDirtyFlag() {
        assertFalse(node.isDirty());

        node.setDirty(true);
        assertTrue(node.isDirty());
        assertEquals(1, pageCache.getDirtyPageCount());

        node.setDirty(false);
        assertFalse(node.isDirty());
        assertEquals(1, pageCache.getCleanPageCount());
    }

    @Test
    public void testGetPage() {
        assertNotNull(node.getPage());
    }

    @Test
    public void testGetConfiguration() {
        assertSame(conf, node.getConfiguration());
    }
}
