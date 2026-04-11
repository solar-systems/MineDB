package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.utils.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

/**
 * 批量操作测试
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class BatchOperationTest {
    private Db db;

    @Before
    public void init() throws IOException {
        cleanup();
        Configuration config = new Configuration();
        config.setDbName("test_batch.db");
        config.setAutoFlushEnabled(false); // 禁用自动刷盘以简化测试
        db = new MineDb("test_batch.db", config);
    }

    @After
    public void cleanup() throws IOException {
        if (db != null) {
            db.close();
        }
        Files.deleteIfExists(Paths.get("test_batch.db"));
        Files.deleteIfExists(Paths.get("test_batch.db.wal"));
    }

    // ==================== batchPut(Map) 测试 ====================

    @Test
    public void testBatchPutWithMap() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        data.put("key3", "value3");

        int count = db.batchPut(data);

        assertEquals(3, count);
        assertEquals(3, db.size());
        assertEquals("value1", db.get("key1"));
        assertEquals("value2", db.get("key2"));
        assertEquals("value3", db.get("key3"));
    }

    @Test
    public void testBatchPutEmptyMap() throws Exception {
        int count = db.batchPut(new HashMap<>());
        assertEquals(0, count);
        assertEquals(0, db.size());
    }

    @Test
    public void testBatchPutUpdateExisting() throws Exception {
        db.put("key1", "oldValue");

        Map<String, String> data = new HashMap<>();
        data.put("key1", "newValue");
        data.put("key2", "value2");

        int count = db.batchPut(data);

        assertEquals(2, count);
        assertEquals(2, db.size());
        assertEquals("newValue", db.get("key1"));
        assertEquals("value2", db.get("key2"));
    }

    @Test
    public void testBatchPutWithNullKey() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put(null, "value1");
        data.put("key2", "value2");

        int count = db.batchPut(data);

        assertEquals(1, count);
        assertEquals(1, db.size());
        assertEquals("value2", db.get("key2"));
    }

    @Test
    public void testBatchPutWithNullValue() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("key1", null);
        data.put("key2", "value2");

        int count = db.batchPut(data);

        assertEquals(1, count);
        assertEquals(1, db.size());
    }

    // ==================== batchPut(List) 测试 ====================

    @Test
    public void testBatchPutWithList() throws Exception {
        List<KeyValue> data = Arrays.asList(
                new KeyValue("key1", "value1"),
                new KeyValue("key2", "value2"),
                new KeyValue("key3", "value3")
        );

        int count = db.batchPut(data);

        assertEquals(3, count);
        assertEquals(3, db.size());
    }

    @Test
    public void testBatchPutEmptyList() throws Exception {
        int count = db.batchPut(new ArrayList<>());
        assertEquals(0, count);
    }

    @Test
    public void testBatchPutListWithNulls() throws Exception {
        List<KeyValue> data = Arrays.asList(
                null,
                new KeyValue("key1", "value1"),
                new KeyValue(null, "value2"),
                new KeyValue("key2", null),
                new KeyValue("key3", "value3")
        );

        int count = db.batchPut(data);

        assertEquals(2, count);
        assertEquals(2, db.size());
        assertEquals("value1", db.get("key1"));
        assertEquals("value3", db.get("key3"));
    }

    // ==================== batchGet 测试 ====================

    @Test
    public void testBatchGet() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");

        List<String> keys = Arrays.asList("key1", "key2", "key3");
        Map<String, String> result = db.batchGet(keys);

        assertEquals(3, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
        assertEquals("value3", result.get("key3"));
    }

    @Test
    public void testBatchGetPartialFound() throws Exception {
        db.put("key1", "value1");
        db.put("key3", "value3");

        List<String> keys = Arrays.asList("key1", "key2", "key3");
        Map<String, String> result = db.batchGet(keys);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("key1"));
        assertTrue(result.containsKey("key3"));
        assertFalse(result.containsKey("key2"));
    }

    @Test
    public void testBatchGetEmptyList() throws Exception {
        Map<String, String> result = db.batchGet(new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBatchGetNoneFound() throws Exception {
        List<String> keys = Arrays.asList("notexist1", "notexist2");
        Map<String, String> result = db.batchGet(keys);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testBatchGetWithNullKey() throws Exception {
        db.put("key1", "value1");

        List<String> keys = Arrays.asList(null, "key1");
        Map<String, String> result = db.batchGet(keys);

        assertEquals(1, result.size());
        assertEquals("value1", result.get("key1"));
    }

    // ==================== batchDelete 测试 ====================

    @Test
    public void testBatchDelete() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");

        List<String> keys = Arrays.asList("key1", "key2");
        int count = db.batchDelete(keys);

        assertEquals(2, count);
        assertEquals(1, db.size());
        assertNull(db.get("key1"));
        assertNull(db.get("key2"));
        assertEquals("value3", db.get("key3"));
    }

    @Test
    public void testBatchDeletePartialFound() throws Exception {
        db.put("key1", "value1");
        db.put("key3", "value3");

        List<String> keys = Arrays.asList("key1", "key2", "key3");
        int count = db.batchDelete(keys);

        assertEquals(2, count);
        assertEquals(0, db.size());
    }

    @Test
    public void testBatchDeleteEmptyList() throws Exception {
        int count = db.batchDelete(new ArrayList<>());
        assertEquals(0, count);
    }

    @Test
    public void testBatchDeleteNoneFound() throws Exception {
        List<String> keys = Arrays.asList("notexist1", "notexist2");
        int count = db.batchDelete(keys);

        assertEquals(0, count);
    }

    @Test
    public void testBatchDeleteWithNullKey() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        List<String> keys = Arrays.asList(null, "key1");
        int count = db.batchDelete(keys);

        assertEquals(1, count);
        assertEquals(1, db.size());
    }

    // ==================== 混合操作测试 ====================

    @Test
    public void testMixedBatchOperations() throws Exception {
        // 批量插入
        Map<String, String> putData = new HashMap<>();
        putData.put("key1", "value1");
        putData.put("key2", "value2");
        putData.put("key3", "value3");
        db.batchPut(putData);

        assertEquals(3, db.size());

        // 批量获取
        Map<String, String> getData = db.batchGet(Arrays.asList("key1", "key2", "key3"));
        assertEquals(3, getData.size());

        // 批量删除
        int deleted = db.batchDelete(Arrays.asList("key1", "key2"));
        assertEquals(2, deleted);
        assertEquals(1, db.size());
    }

    @Test
    public void testLargeBatchPut() throws Exception {
        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            data.put("key" + i, "value" + i);
        }

        int count = db.batchPut(data);

        assertEquals(1000, count);
        assertEquals(1000, db.size());
    }

    @Test
    public void testLargeBatchGet() throws Exception {
        // 先插入数据
        for (int i = 0; i < 100; i++) {
            db.put("key" + i, "value" + i);
        }

        // 批量获取
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add("key" + i);
        }

        Map<String, String> result = db.batchGet(keys);

        assertEquals(100, result.size());
    }

    @Test
    public void testLargeBatchDelete() throws Exception {
        // 先插入数据
        for (int i = 0; i < 100; i++) {
            db.put("key" + i, "value" + i);
        }

        assertEquals(100, db.size());

        // 批量删除一半
        List<String> keysToDelete = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            keysToDelete.add("key" + i);
        }

        int deleted = db.batchDelete(keysToDelete);

        assertEquals(50, deleted);
        assertEquals(50, db.size());
    }

    // ==================== 异常测试 ====================

    @Test(expected = IllegalStateException.class)
    public void testBatchPutOnClosedDb() throws Exception {
        db.close();
        db.batchPut(new HashMap<>());
    }

    @Test(expected = IllegalStateException.class)
    public void testBatchGetOnClosedDb() throws Exception {
        db.close();
        db.batchGet(new ArrayList<>());
    }

    @Test(expected = IllegalStateException.class)
    public void testBatchDeleteOnClosedDb() throws Exception {
        db.close();
        db.batchDelete(new ArrayList<>());
    }

    @Test(expected = NullPointerException.class)
    public void testBatchPutNullMap() throws Exception {
        db.batchPut((Map<String, String>) null);
    }

    @Test(expected = NullPointerException.class)
    public void testBatchPutNullList() throws Exception {
        db.batchPut((List<KeyValue>) null);
    }

    @Test(expected = NullPointerException.class)
    public void testBatchGetNullList() throws Exception {
        db.batchGet(null);
    }

    @Test(expected = NullPointerException.class)
    public void testBatchDeleteNullList() throws Exception {
        db.batchDelete(null);
    }
}
