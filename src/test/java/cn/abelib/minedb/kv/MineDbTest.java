package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.Configuration;
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
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class MineDbTest {
    private static final String DB_NAME = "test_minedb.db";
    private MineDb db;

    @Before
    public void init() throws IOException {
        cleanup();
        db = new MineDb(DB_NAME);
    }

    @After
    public void cleanup() throws IOException {
        if (db != null && !db.isClosed()) {
            db.close();
        }
        Files.deleteIfExists(Paths.get(DB_NAME));
    }

    @Test
    public void testPutAndGet() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        assertEquals("value1", db.get("key1"));
        assertEquals("value2", db.get("key2"));
    }

    @Test
    public void testGetNotFound() throws Exception {
        assertNull(db.get("nonexistent"));
    }

    @Test
    public void testPutOverwrite() throws Exception {
        db.put("key1", "value1");
        db.put("key1", "value2");

        assertEquals("value2", db.get("key1"));
        assertEquals(1, db.size());
    }

    @Test
    public void testContains() throws Exception {
        db.put("key1", "value1");

        assertTrue(db.contains("key1"));
        assertFalse(db.contains("key2"));
    }

    @Test
    public void testDelete() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        assertTrue(db.delete("key1"));
        assertNull(db.get("key1"));
        assertEquals(1, db.size());

        assertFalse(db.delete("nonexistent"));
    }

    @Test
    public void testRange() throws Exception {
        db.put("a", "1");
        db.put("b", "2");
        db.put("c", "3");
        db.put("d", "4");
        db.put("e", "5");

        List<KeyValue> result = db.range("b", "e");

        assertEquals(3, result.size());
        assertEquals("b", result.get(0).getKey());
        assertEquals("c", result.get(1).getKey());
        assertEquals("d", result.get(2).getKey());
    }

    @Test
    public void testPrefix() throws Exception {
        db.put("user:1", "alice");
        db.put("user:2", "bob");
        db.put("user:3", "charlie");
        db.put("product:1", "item1");

        List<KeyValue> result = db.prefix("user:");

        assertEquals(3, result.size());
        assertTrue(result.stream().allMatch(kv -> kv.getKey().startsWith("user:")));
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(0, db.size());

        db.put("key1", "value1");
        assertEquals(1, db.size());

        db.put("key2", "value2");
        assertEquals(2, db.size());

        db.delete("key1");
        assertEquals(1, db.size());
    }

    @Test
    public void testSync() throws Exception {
        db.put("key1", "value1");
        db.sync();
        // No exception means success
    }

    @Test
    public void testClear() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        db.clear();

        assertEquals(0, db.size());
        assertNull(db.get("key1"));
    }

    @Test
    public void testClose() throws Exception {
        db.put("key1", "value1");
        db.close();

        assertTrue(db.isClosed());
    }

    @Test(expected = IllegalStateException.class)
    public void testOperationAfterClose() throws Exception {
        db.close();
        db.get("key1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullKey() throws Exception {
        db.put(null, "value");
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullValue() throws Exception {
        db.put("key", null);
    }

    @Test
    public void testCustomConfiguration() throws Exception {
        db.close();
        Files.deleteIfExists(Paths.get(DB_NAME));

        Configuration config = new Configuration(8 * 1024, 64, 512);
        db = new MineDb(DB_NAME, config);

        db.put("key1", "value1");
        assertEquals("value1", db.get("key1"));
    }

    @Test
    public void testGetDbName() {
        assertEquals(DB_NAME, db.getDbName());
    }
}
