package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.utils.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * 迭代器测试
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class DbIteratorTest {
    private Db db;

    @Before
    public void init() throws IOException {
        cleanup();
        Configuration config = new Configuration();
        config.setDbName("test_iterator.db");
        config.setAutoFlushEnabled(false);
        db = new MineDb("test_iterator.db", config);
    }

    @After
    public void cleanup() throws IOException {
        if (db != null) {
            db.close();
        }
        Files.deleteIfExists(Paths.get("test_iterator.db"));
        Files.deleteIfExists(Paths.get("test_iterator.db.wal"));
    }

    @Test
    public void testEmptyIterator() throws Exception {
        Iterator<KeyValue> it = db.iterator();
        assertFalse(it.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyIteratorNext() throws Exception {
        Iterator<KeyValue> it = db.iterator();
        it.next();
    }

    @Test
    public void testSingleElement() throws Exception {
        db.put("key1", "value1");

        Iterator<KeyValue> it = db.iterator();

        assertTrue(it.hasNext());
        KeyValue kv = it.next();
        assertEquals("key1", kv.getKey());
        assertEquals("value1", kv.getValue());

        assertFalse(it.hasNext());
    }

    @Test
    public void testMultipleElements() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");

        Iterator<KeyValue> it = db.iterator();
        List<String> keys = new ArrayList<>();

        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }

        assertEquals(3, keys.size());
        // 应该按键的字典序排列
        assertEquals("key1", keys.get(0));
        assertEquals("key2", keys.get(1));
        assertEquals("key3", keys.get(2));
    }

    @Test
    public void testIteratorOrder() throws Exception {
        // 故意乱序插入
        db.put("z", "valueZ");
        db.put("a", "valueA");
        db.put("m", "valueM");
        db.put("b", "valueB");

        Iterator<KeyValue> it = db.iterator();
        List<String> keys = new ArrayList<>();

        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }

        // 应该按字典序排列
        assertEquals(4, keys.size());
        assertEquals("a", keys.get(0));
        assertEquals("b", keys.get(1));
        assertEquals("m", keys.get(2));
        assertEquals("z", keys.get(3));
    }

    @Test
    public void testIteratorWithStartKey() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");
        db.put("key4", "value4");
        db.put("key5", "value5");

        // 从 key3 开始
        Iterator<KeyValue> it = db.iterator("key3");
        List<String> keys = new ArrayList<>();

        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }

        assertEquals(3, keys.size());
        assertEquals("key3", keys.get(0));
        assertEquals("key4", keys.get(1));
        assertEquals("key5", keys.get(2));
    }

    @Test
    public void testIteratorWithNonExistentStartKey() throws Exception {
        db.put("key1", "value1");
        db.put("key3", "value3");
        db.put("key5", "value5");

        // key2 不存在，应该从 key3 开始
        Iterator<KeyValue> it = db.iterator("key2");
        List<String> keys = new ArrayList<>();

        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }

        assertEquals(2, keys.size());
        assertEquals("key3", keys.get(0));
        assertEquals("key5", keys.get(1));
    }

    @Test
    public void testIteratorWithStartKeyBeyondEnd() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        // 从 zzz 开始，应该在所有元素之后
        Iterator<KeyValue> it = db.iterator("zzz");

        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorWithNullStartKey() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        // null 表示从头开始
        Iterator<KeyValue> it = db.iterator((String) null);
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }

        assertEquals(2, count);
    }

    @Test
    public void testMultipleIterators() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");

        Iterator<KeyValue> it1 = db.iterator();
        Iterator<KeyValue> it2 = db.iterator();

        // 两个迭代器应该独立遍历
        assertEquals("key1", it1.next().getKey());
        assertEquals("key1", it2.next().getKey());

        // 继续遍历
        assertEquals("key2", it1.next().getKey());
        assertEquals("key2", it2.next().getKey());

        assertEquals("key3", it1.next().getKey());
        assertEquals("key3", it2.next().getKey());
    }

    @Test
    public void testSkip() throws Exception {
        for (int i = 0; i < 100; i++) {
            db.put("key" + String.format("%03d", i), "value" + i);
        }

        DbIterator it = (DbIterator) db.iterator();

        // 跳过 50 个
        long skipped = it.skip(50);
        assertEquals(50, skipped);

        // 应该从第 51 个开始
        KeyValue kv = it.next();
        assertEquals("key050", kv.getKey());
    }

    @Test
    public void testSkipBeyondEnd() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        DbIterator it = (DbIterator) db.iterator();

        // 尝试跳过 100 个，但只有 2 个
        long skipped = it.skip(100);
        assertEquals(2, skipped);

        assertFalse(it.hasNext());
    }

    @Test
    public void testLargeDataset() throws Exception {
        int count = 1000;
        for (int i = 0; i < count; i++) {
            db.put("key" + String.format("%04d", i), "value" + i);
        }

        Iterator<KeyValue> it = db.iterator();
        int actualCount = 0;
        String prevKey = null;

        while (it.hasNext()) {
            KeyValue kv = it.next();
            if (prevKey != null) {
                // 确保有序
                assertTrue(kv.getKey().compareTo(prevKey) > 0);
            }
            prevKey = kv.getKey();
            actualCount++;
        }

        assertEquals(count, actualCount);
    }

    @Test
    public void testIteratorAfterDelete() throws Exception {
        db.put("a", "value1");
        db.put("b", "value2");
        db.put("c", "value3");

        db.delete("b");

        Iterator<KeyValue> it = db.iterator();
        List<String> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }

        assertEquals(2, keys.size());
        assertEquals("a", keys.get(0));
        assertEquals("c", keys.get(1));
    }

    @Test
    public void testIteratorAfterUpdate() throws Exception {
        db.put("a", "value1");
        db.put("b", "oldValue");

        // 更新
        db.put("b", "newValue");

        Iterator<KeyValue> it = db.iterator();
        assertTrue(it.hasNext());

        KeyValue kv1 = it.next();
        assertEquals("a", kv1.getKey());
        assertEquals("value1", kv1.getValue());

        KeyValue kv2 = it.next();
        assertEquals("b", kv2.getKey());
        assertEquals("newValue", kv2.getValue());
    }

    @Test
    public void testIteratorConsistency() throws Exception {
        // 插入数据（使用零填充键确保正确排序）
        for (int i = 0; i < 50; i++) {
            db.put("key" + String.format("%03d", i), "value" + i);
        }

        // 创建迭代器
        Iterator<KeyValue> it = db.iterator();

        // 读取一半
        for (int i = 0; i < 25; i++) {
            assertTrue(it.hasNext());
            KeyValue kv = it.next();
            assertEquals("key" + String.format("%03d", i), kv.getKey());
        }

        // 继续读取剩余
        for (int i = 25; i < 50; i++) {
            assertTrue(it.hasNext());
            KeyValue kv = it.next();
            assertEquals("key" + String.format("%03d", i), kv.getKey());
        }

        assertFalse(it.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorOnClosedDb() throws Exception {
        db.close();
        db.iterator();
    }

    @Test
    public void testForEachLoop() throws Exception {
        db.put("a", "value1");
        db.put("b", "value2");

        List<KeyValue> results = new ArrayList<>();
        Iterator<KeyValue> it = db.iterator();

        while (it.hasNext()) {
            results.add(it.next());
        }

        assertEquals(2, results.size());
        assertEquals("a", results.get(0).getKey());
        assertEquals("b", results.get(1).getKey());
    }
}
