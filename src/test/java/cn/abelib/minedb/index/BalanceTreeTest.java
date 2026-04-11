package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.FreePageList;
import cn.abelib.minedb.utils.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

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

    // ==================== 删除合并测试 ====================

    /**
     * 测试简单删除 - 少量数据
     */
    @Test
    public void testSimpleDelete() throws Exception {
        // 插入少量数据
        for (int i = 0; i < 20; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        // 验证插入成功
        for (int i = 0; i < 20; i++) {
            assertEquals("value" + i, balanceTree.get("key" + i));
        }

        // 删除一半
        for (int i = 0; i < 10; i++) {
            assertTrue("Delete key" + i + " should succeed", balanceTree.delete("key" + i));
        }

        // 验证删除结果
        for (int i = 0; i < 10; i++) {
            assertNull("key" + i + " should be deleted", balanceTree.get("key" + i));
        }
        for (int i = 10; i < 20; i++) {
            assertEquals("value" + i, balanceTree.get("key" + i));
        }
    }

    /**
     * 测试中等数量插入
     */
    @Test
    public void testMediumInserts() throws Exception {
        int[] sizes = {50, 100, 150, 200, 300, 500};

        for (int size : sizes) {
            cleanup();
            balanceTree = new BalanceTree(new Configuration());

            for (int i = 0; i < size; i++) {
                String key = String.format("key%04d", i);
                String value = "value" + "_".repeat(50) + i;
                balanceTree.insert(key, value);
            }

            for (int i = 0; i < size; i++) {
                String key = String.format("key%04d", i);
                assertNotNull("Key " + i + " should exist", balanceTree.get(key));
            }
        }
    }

    /**
     * 测试删除后节点借用（从右兄弟借用）
     */
    @Test
    public void testDeleteBorrowFromRight() throws Exception {
        for (int i = 0; i < 300; i++) {
            String key = String.format("key%04d", i);
            String value = "value" + "_".repeat(50) + i;
            balanceTree.insert(key, value);
        }

        for (int i = 0; i < 300; i++) {
            assertNotNull("Key " + i + " should exist after insert", balanceTree.get(String.format("key%04d", i)));
        }

        for (int i = 0; i < 200; i++) {
            balanceTree.delete(String.format("key%04d", i));
        }

        for (int i = 200; i < 300; i++) {
            String key = String.format("key%04d", i);
            assertNotNull("Key " + i + " should still exist", balanceTree.get(key));
        }

        for (int i = 0; i < 200; i++) {
            assertNull("Key " + i + " should be deleted", balanceTree.get(String.format("key%04d", i)));
        }
    }

    /**
     * 测试删除后节点借用（从左兄弟借用）
     */
    @Test
    public void testDeleteBorrowFromLeft() throws Exception {
        for (int i = 0; i < 300; i++) {
            String key = String.format("key%04d", i);
            String value = "value" + "_".repeat(50) + i;
            balanceTree.insert(key, value);
        }

        for (int i = 299; i >= 100; i--) {
            balanceTree.delete(String.format("key%04d", i));
        }

        for (int i = 0; i < 100; i++) {
            assertNotNull("Key " + i + " should still exist", balanceTree.get(String.format("key%04d", i)));
        }
    }

    /**
     * 测试删除后节点合并
     */
    @Test
    public void testDeleteNodeMerge() throws Exception {
        for (int i = 0; i < 1000; i++) {
            String key = String.format("key%05d", i);
            String value = "value" + "_".repeat(100) + i;
            balanceTree.insert(key, value);
        }

        for (int i = 0; i < 800; i++) {
            balanceTree.delete(String.format("key%05d", i));
        }

        for (int i = 800; i < 1000; i++) {
            String key = String.format("key%05d", i);
            assertNotNull("Key " + i + " should still exist", balanceTree.get(key));
        }
    }

    /**
     * 测试删除到只剩根节点
     */
    @Test
    public void testDeleteToRoot() throws Exception {
        // 插入数据
        for (int i = 0; i < 100; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        // 删除所有数据
        for (int i = 0; i < 100; i++) {
            assertTrue("Delete key" + i + " should succeed", balanceTree.delete("key" + i));
        }

        // 验证树为空
        assertNull(balanceTree.get("key0"));
        assertNull(balanceTree.get("key99"));

        // 根节点仍然存在
        assertNotNull(balanceTree.getRoot());
        assertTrue(balanceTree.getRoot().getKeyValues().isEmpty());
    }

    /**
     * 测试交替插入删除
     */
    @Test
    public void testAlternatingInsertDelete() throws Exception {
        // 第一轮插入
        for (int i = 0; i < 200; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        // 删除一半
        for (int i = 0; i < 100; i++) {
            balanceTree.delete("key" + i);
        }

        // 再插入新的
        for (int i = 200; i < 300; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        // 验证：前100个不存在，100-199存在，200-299存在
        for (int i = 0; i < 100; i++) {
            assertNull(balanceTree.get("key" + i));
        }
        for (int i = 100; i < 200; i++) {
            assertEquals("value" + i, balanceTree.get("key" + i));
        }
        for (int i = 200; i < 300; i++) {
            assertEquals("value" + i, balanceTree.get("key" + i));
        }
    }

    /**
     * 测试随机删除顺序
     */
    @Test
    public void testRandomDeleteOrder() throws Exception {
        Random random = new Random(42); // 固定种子以便复现
        List<Integer> keys = new ArrayList<>();

        // 插入数据
        for (int i = 0; i < 300; i++) {
            balanceTree.insert("key" + i, "value" + i);
            keys.add(i);
        }

        // 随机打乱删除顺序
        Collections.shuffle(keys, random);

        // 随机删除一半
        for (int i = 0; i < 150; i++) {
            int keyIndex = keys.get(i);
            assertTrue(balanceTree.delete("key" + keyIndex));
        }

        // 验证删除结果
        for (int i = 0; i < 150; i++) {
            assertNull(balanceTree.get("key" + keys.get(i)));
        }
        for (int i = 150; i < 300; i++) {
            assertNotNull(balanceTree.get("key" + keys.get(i)));
        }
    }

    /**
     * 测试删除后范围查询正确性
     */
    @Test
    public void testRangeAfterDelete() throws Exception {
        // 插入数据
        for (int i = 0; i < 200; i++) {
            balanceTree.insert(String.format("k%03d", i), "v" + i);
        }

        // 删除中间部分
        for (int i = 50; i < 150; i++) {
            balanceTree.delete(String.format("k%03d", i));
        }

        // 范围查询
        List<KeyValue> range = balanceTree.range("k000", "k200");
        assertEquals(100, range.size()); // 0-49 和 150-199

        // 验证范围查询结果
        for (KeyValue kv : range) {
            int num = Integer.parseInt(kv.getKey().substring(1));
            assertTrue(num < 50 || num >= 150);
        }
    }

    /**
     * 测试删除后前缀查询正确性
     */
    @Test
    public void testPrefixAfterDelete() throws Exception {
        // 插入不同前缀的数据
        for (int i = 0; i < 50; i++) {
            balanceTree.insert("user:" + i, "u" + i);
            balanceTree.insert("admin:" + i, "a" + i);
            balanceTree.insert("guest:" + i, "g" + i);
        }

        // 删除 admin 前缀的所有数据
        for (int i = 0; i < 50; i++) {
            balanceTree.delete("admin:" + i);
        }

        // 验证前缀查询
        List<KeyValue> userPrefix = balanceTree.prefix("user:");
        assertEquals(50, userPrefix.size());

        List<KeyValue> adminPrefix = balanceTree.prefix("admin:");
        assertEquals(0, adminPrefix.size());

        List<KeyValue> guestPrefix = balanceTree.prefix("guest:");
        assertEquals(50, guestPrefix.size());
    }

    /**
     * 测试大量数据删除后的树结构完整性
     */
    @Test
    public void testTreeIntegrityAfterMassiveDelete() throws Exception {
        int total = 5000;
        for (int i = 0; i < total; i++) {
            balanceTree.insert("key" + String.format("%04d", i), "value" + i);
        }

        for (int i = 0; i < total - 100; i++) {
            assertTrue(balanceTree.delete("key" + String.format("%04d", i)));
        }

        for (int i = total - 100; i < total; i++) {
            String key = "key" + String.format("%04d", i);
            assertEquals("value" + i, balanceTree.get(key));
        }

        assertNotNull(balanceTree.getRoot());
    }

    /**
     * 测试删除后持久化和恢复
     */
    @Test
    public void testPersistenceAfterDelete() throws Exception {
        // 插入数据
        for (int i = 0; i < 200; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        // 删除一半
        for (int i = 0; i < 100; i++) {
            balanceTree.delete("key" + i);
        }

        // 持久化
        balanceTree.flush(100);

        // 创建新的树实例来恢复数据
        BalanceTree restoredTree = new BalanceTree(new Configuration());

        // 验证数据
        for (int i = 0; i < 100; i++) {
            assertNull(restoredTree.get("key" + i));
        }
        for (int i = 100; i < 200; i++) {
            assertEquals("value" + i, restoredTree.get("key" + i));
        }
    }

    /**
     * 测试 FreePageList 在删除操作后的状态
     */
    @Test
    public void testFreePageListAfterDelete() throws Exception {
        FreePageList freePageList = balanceTree.getFreePageList();
        assertNotNull(freePageList);
        int initialCount = freePageList.getFreePageCount();

        // 插入大量数据
        for (int i = 0; i < 500; i++) {
            balanceTree.insert("key" + i, "value" + i);
        }

        // 删除数据后检查空闲页
        for (int i = 0; i < 250; i++) {
            balanceTree.delete("key" + i);
        }

        // 删除后空闲页可能增加（如果释放了溢出页）
        // 注意：普通记录不会立即释放页面，只有溢出页才会
        assertTrue(freePageList.getFreePageCount() >= initialCount);
    }
}
