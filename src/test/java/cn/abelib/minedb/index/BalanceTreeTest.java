package cn.abelib.minedb.index;

import cn.abelib.minedb.utils.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @Author: abel.huang
 * @Date: 2020-10-29 00:33
 */
public class BalanceTreeTest {
    BalanceTree balanceTree;

    @Before
    public void init() throws IOException {
        cleanup();
        balanceTree = new BalanceTree(new Configuration());
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get("default.db"));
    }

    @Test
    public void initTest() {
        System.err.println(balanceTree.getConfiguration());
        System.err.println(balanceTree.getMetaNode());
    }

    @Test
    public void insertTest() throws Exception {
        balanceTree.insert("Hello", "World");
        balanceTree.insert("Foo", "Bar");
        balanceTree.insert("Abel", "Huang");
    }

    @Test
    public void testGet() throws Exception {
        balanceTree.insert("key1", "value1");
        balanceTree.insert("key2", "value2");

        assertEquals("value1", balanceTree.get("key1"));
        assertEquals("value2", balanceTree.get("key2"));
        assertNull(balanceTree.get("nonexistent"));
    }

    @Test
    public void testGetEmptyTree() throws IOException {
        assertNull(balanceTree.get("anykey"));
    }

    @Test
    public void testContains() throws Exception {
        balanceTree.insert("key1", "value1");

        assertTrue(balanceTree.contains("key1"));
        assertFalse(balanceTree.contains("key2"));
    }

    @Test
    public void testRange() throws Exception {
        balanceTree.insert("a", "1");
        balanceTree.insert("b", "2");
        balanceTree.insert("c", "3");
        balanceTree.insert("d", "4");
        balanceTree.insert("e", "5");

        List<KeyValue> result = balanceTree.range("b", "e");

        assertEquals(3, result.size());
        assertEquals("b", result.get(0).getKey());
        assertEquals("c", result.get(1).getKey());
        assertEquals("d", result.get(2).getKey());
    }

    @Test
    public void testRangeEmpty() throws Exception {
        List<KeyValue> result = balanceTree.range("a", "z");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRangeInvalid() throws Exception {
        balanceTree.insert("a", "1");

        List<KeyValue> result = balanceTree.range("z", "a");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPrefix() throws Exception {
        balanceTree.insert("user:1", "alice");
        balanceTree.insert("user:2", "bob");
        balanceTree.insert("user:3", "charlie");
        balanceTree.insert("product:1", "item1");

        List<KeyValue> result = balanceTree.prefix("user:");

        assertEquals(3, result.size());
        assertTrue(result.stream().allMatch(kv -> kv.getKey().startsWith("user:")));
    }

    @Test
    public void testPrefixNoMatch() throws Exception {
        balanceTree.insert("key1", "value1");

        List<KeyValue> result = balanceTree.prefix("xyz:");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDelete() throws Exception {
        balanceTree.insert("key1", "value1");
        balanceTree.insert("key2", "value2");

        assertTrue(balanceTree.delete("key1"));
        assertNull(balanceTree.get("key1"));
        assertEquals("value2", balanceTree.get("key2"));
    }

    @Test
    public void testDeleteNonexistent() throws Exception {
        assertFalse(balanceTree.delete("nonexistent"));
    }

    @Test
    public void testDeleteFromEmptyTree() throws IOException {
        assertFalse(balanceTree.delete("anykey"));
    }

    @Test
    public void testFlush() throws Exception {
        balanceTree.insert("key1", "value1");
        balanceTree.insert("key2", "value2");

        balanceTree.flush(2);

        // No exception means success
    }

    @Test
    public void testFlushEmptyTree() throws IOException {
        balanceTree.flush(0);
        // No exception means success
    }

    @Test
    public void testInsertAndUpdate() throws Exception {
        balanceTree.insert("key1", "value1");
        balanceTree.insert("key1", "value2");

        assertEquals("value2", balanceTree.get("key1"));
    }

    @Test
    public void testMultipleInserts() throws Exception {
        for (int i = 0; i < 100; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, balanceTree.get("key" + i));
        }
    }

    @Test
    public void testGetMetaNode() {
        assertNotNull(balanceTree.getMetaNode());
    }

    @Test
    public void testGetConfiguration() {
        assertNotNull(balanceTree.getConfiguration());
    }

    @Test
    public void testGetRoot() {
        assertNotNull(balanceTree.getRoot());
    }
}
