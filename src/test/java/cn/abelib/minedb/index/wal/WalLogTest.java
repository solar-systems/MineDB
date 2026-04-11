package cn.abelib.minedb.index.wal;

import cn.abelib.minedb.index.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * WAL 日志测试
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class WalLogTest {
    private Configuration conf;
    private WalLog walLog;

    @Before
    public void init() throws IOException {
        cleanup();
        conf = new Configuration();
        conf.setDbName("test_wal");
        walLog = new WalLog(conf);
    }

    @After
    public void cleanup() throws IOException {
        if (walLog != null) {
            walLog.close();
        }
        Files.deleteIfExists(Paths.get("test_wal.wal"));
    }

    @Test
    public void testLogPut() throws IOException {
        walLog.logPut("key1", "value1");
        walLog.logPut("key2", "value2");

        List<WalEntry> entries = walLog.readAllEntries();
        assertEquals(2, entries.size());

        assertEquals(WalEntry.Type.PUT, entries.get(0).getType());
        assertEquals("key1", entries.get(0).getKey());
        assertEquals("value1", entries.get(0).getValue());
        assertEquals(1, entries.get(0).getSequence());

        assertEquals(WalEntry.Type.PUT, entries.get(1).getType());
        assertEquals("key2", entries.get(1).getKey());
        assertEquals("value2", entries.get(1).getValue());
        assertEquals(2, entries.get(1).getSequence());
    }

    @Test
    public void testLogDelete() throws IOException {
        walLog.logDelete("key1");

        List<WalEntry> entries = walLog.readAllEntries();
        assertEquals(1, entries.size());

        assertEquals(WalEntry.Type.DELETE, entries.get(0).getType());
        assertEquals("key1", entries.get(0).getKey());
        assertNull(entries.get(0).getValue());
    }

    @Test
    public void testLogSync() throws IOException {
        walLog.logPut("key1", "value1");
        walLog.logSync();
        walLog.logPut("key2", "value2");

        List<WalEntry> entries = walLog.readAllEntries();
        assertEquals(3, entries.size());

        assertEquals(WalEntry.Type.PUT, entries.get(0).getType());
        assertEquals(WalEntry.Type.SYNC, entries.get(1).getType());
        assertEquals(WalEntry.Type.PUT, entries.get(2).getType());
    }

    @Test
    public void testChecksumValidation() throws IOException {
        walLog.logPut("key1", "value1");

        List<WalEntry> entries = walLog.readAllEntries();
        assertEquals(1, entries.size());

        WalEntry entry = entries.get(0);
        assertTrue(entry.validate());
    }

    @Test
    public void testSequenceIncrement() throws IOException {
        assertEquals(0, walLog.getSequence());

        walLog.logPut("key1", "value1");
        assertEquals(1, walLog.getSequence());

        walLog.logDelete("key2");
        assertEquals(2, walLog.getSequence());

        walLog.logSync();
        assertEquals(3, walLog.getSequence());
    }

    @Test
    public void testClear() throws IOException {
        walLog.logPut("key1", "value1");
        walLog.logPut("key2", "value2");

        assertEquals(2, walLog.readAllEntries().size());

        walLog.clear();

        assertEquals(0, walLog.readAllEntries().size());
        assertEquals(0, walLog.getSequence());
    }

    @Test
    public void testReopen() throws IOException {
        walLog.logPut("key1", "value1");
        walLog.logPut("key2", "value2");

        long seq = walLog.getSequence();
        walLog.close();

        // 重新打开
        walLog = new WalLog(conf);

        assertEquals(seq, walLog.getSequence());

        List<WalEntry> entries = walLog.readAllEntries();
        assertEquals(2, entries.size());
    }

    @Test
    public void testFileSize() throws IOException {
        assertEquals(0, walLog.getFileSize());

        walLog.logPut("key1", "value1");
        assertTrue(walLog.getFileSize() > 0);
    }

    @Test
    public void testExists() throws IOException {
        assertTrue(walLog.exists());

        walLog.close();
        Files.deleteIfExists(Paths.get("test_wal.wal"));

        walLog = new WalLog(conf);
        assertTrue(walLog.exists());
    }

    @Test
    public void testMultipleOperations() throws IOException {
        // 模拟实际操作序列
        walLog.logPut("user:1", "{\"name\":\"Alice\"}");
        walLog.logPut("user:2", "{\"name\":\"Bob\"}");
        walLog.logDelete("user:1");
        walLog.logPut("user:3", "{\"name\":\"Charlie\"}");
        walLog.logSync();

        List<WalEntry> entries = walLog.readAllEntries();
        assertEquals(5, entries.size());

        // 验证顺序
        assertEquals(WalEntry.Type.PUT, entries.get(0).getType());
        assertEquals("user:1", entries.get(0).getKey());

        assertEquals(WalEntry.Type.PUT, entries.get(1).getType());
        assertEquals("user:2", entries.get(1).getKey());

        assertEquals(WalEntry.Type.DELETE, entries.get(2).getType());
        assertEquals("user:1", entries.get(2).getKey());

        assertEquals(WalEntry.Type.PUT, entries.get(3).getType());
        assertEquals("user:3", entries.get(3).getKey());

        assertEquals(WalEntry.Type.SYNC, entries.get(4).getType());
    }
}
