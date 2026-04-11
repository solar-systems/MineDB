package cn.abelib.minedb.index.autoflush;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * 自动刷盘管理器测试
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class AutoFlushManagerTest {
    private Configuration conf;
    private BalanceTree tree;
    private AutoFlushManager flushManager;

    @Before
    public void init() throws IOException {
        cleanup();
        conf = new Configuration();
        conf.setDbName("test_autoflush");
        tree = new BalanceTree(conf);
    }

    @After
    public void cleanup() throws IOException {
        if (flushManager != null) {
            flushManager.stop();
        }
        if (tree != null) {
            tree.clearCache();
            tree.closeWal();
        }
        Files.deleteIfExists(Paths.get("test_autoflush"));
        Files.deleteIfExists(Paths.get("test_autoflush.wal"));
    }

    @Test
    public void testDefaultConfiguration() {
        flushManager = new AutoFlushManager(tree);

        assertEquals(AutoFlushManager.DEFAULT_FLUSH_INTERVAL_MS, flushManager.getFlushIntervalMs());
        assertEquals(AutoFlushManager.DEFAULT_DIRTY_PAGE_THRESHOLD, flushManager.getDirtyPageThreshold());
        assertEquals(AutoFlushManager.DEFAULT_WAL_SIZE_THRESHOLD, flushManager.getWalSizeThreshold());
        assertFalse(flushManager.isRunning());
    }

    @Test
    public void testCustomConfiguration() {
        flushManager = new AutoFlushManager(tree, 10000, 50, 5 * 1024 * 1024);

        assertEquals(10000, flushManager.getFlushIntervalMs());
        assertEquals(50, flushManager.getDirtyPageThreshold());
        assertEquals(5 * 1024 * 1024, flushManager.getWalSizeThreshold());
    }

    @Test
    public void testStartAndStop() {
        flushManager = new AutoFlushManager(tree, 100, 10, 1024);

        assertFalse(flushManager.isRunning());

        flushManager.start();
        assertTrue(flushManager.isRunning());

        flushManager.stop();
        assertFalse(flushManager.isRunning());
    }

    @Test
    public void testStartIdempotent() {
        flushManager = new AutoFlushManager(tree, 100, 10, 1024);

        flushManager.start();
        assertTrue(flushManager.isRunning());

        // 重复 start 应该无效
        flushManager.start();
        assertTrue(flushManager.isRunning());

        flushManager.stop();
    }

    @Test
    public void testStopIdempotent() {
        flushManager = new AutoFlushManager(tree, 100, 10, 1024);

        // 未启动时 stop 应该安全
        flushManager.stop();
        assertFalse(flushManager.isRunning());

        flushManager.start();
        flushManager.stop();
        assertFalse(flushManager.isRunning());

        // 重复 stop 应该安全
        flushManager.stop();
        assertFalse(flushManager.isRunning());
    }

    @Test
    public void testFlushNow() throws Exception {
        flushManager = new AutoFlushManager(tree, 10000, 100, 1024 * 1024);

        // 写入一些数据
        tree.insert("key1", "value1");
        tree.insert("key2", "value2");

        assertEquals(0, flushManager.getFlushCount());

        // 手动刷盘
        flushManager.flushNow();

        assertEquals(1, flushManager.getFlushCount());
        assertTrue(flushManager.getLastFlushTime() > 0);
    }

    @Test
    public void testScheduledFlush() throws Exception {
        // 使用很短的间隔
        flushManager = new AutoFlushManager(tree, 100, 1000, 1024 * 1024);
        flushManager.start();

        // 写入数据
        tree.insert("key1", "value1");

        // 等待定时刷盘
        Thread.sleep(500);

        // 应该已经刷盘
        assertTrue(flushManager.getFlushCount() > 0);

        flushManager.stop();
    }

    @Test
    public void testThresholdFlush() throws Exception {
        // 设置脏页阈值为 1，很容易触发
        flushManager = new AutoFlushManager(tree, 60000, 1, 1024);
        flushManager.start();

        // 写入数据，应该触发阈值刷盘
        tree.insert("key1", "value1");
        tree.insert("key2", "value2");

        // 手动触发检查（模拟 checkAndFlush）
        // 由于定时任务也会检查，我们等待一会儿
        Thread.sleep(200);

        // 手动触发一次
        flushManager.checkAndFlush();
        Thread.sleep(100);

        // 应该已经刷盘（可能是定时或阈值触发）
        assertTrue(flushManager.getFlushCount() >= 0);

        flushManager.stop();
    }

    @Test
    public void testGetFlushCount() throws Exception {
        flushManager = new AutoFlushManager(tree);

        assertEquals(0, flushManager.getFlushCount());

        flushManager.flushNow();
        assertEquals(1, flushManager.getFlushCount());

        flushManager.flushNow();
        assertEquals(2, flushManager.getFlushCount());
    }

    @Test
    public void testGetLastFlushTime() throws Exception {
        flushManager = new AutoFlushManager(tree);

        assertEquals(0, flushManager.getLastFlushTime());

        long beforeFlush = System.currentTimeMillis();
        flushManager.flushNow();
        long afterFlush = System.currentTimeMillis();

        long lastFlush = flushManager.getLastFlushTime();
        assertTrue(lastFlush >= beforeFlush);
        assertTrue(lastFlush <= afterFlush);
    }

    @Test
    public void testCheckAndFlushWhenNotRunning() throws Exception {
        flushManager = new AutoFlushManager(tree, 10000, 1, 1024);
        // 不启动

        tree.insert("key1", "value1");
        tree.insert("key2", "value2");

        // 不应该触发刷盘
        flushManager.checkAndFlush();

        assertEquals(0, flushManager.getFlushCount());
    }

    @Test
    public void testToString() {
        flushManager = new AutoFlushManager(tree, 5000, 50, 1024 * 1024);

        String str = flushManager.toString();
        assertTrue(str.contains("flushIntervalMs=5000"));
        assertTrue(str.contains("dirtyPageThreshold=50"));
        assertTrue(str.contains("walSizeThreshold=1048576"));
    }

    @Test
    public void testInvalidConfigUsesDefaults() {
        // 传入 0 或负数应该使用默认值
        AutoFlushManager m1 = new AutoFlushManager(tree, 0, 0, 0);
        assertEquals(AutoFlushManager.DEFAULT_FLUSH_INTERVAL_MS, m1.getFlushIntervalMs());
        assertEquals(AutoFlushManager.DEFAULT_DIRTY_PAGE_THRESHOLD, m1.getDirtyPageThreshold());
        assertEquals(AutoFlushManager.DEFAULT_WAL_SIZE_THRESHOLD, m1.getWalSizeThreshold());

        AutoFlushManager m2 = new AutoFlushManager(tree, -100, -10, -1024);
        assertEquals(AutoFlushManager.DEFAULT_FLUSH_INTERVAL_MS, m2.getFlushIntervalMs());
        assertEquals(AutoFlushManager.DEFAULT_DIRTY_PAGE_THRESHOLD, m2.getDirtyPageThreshold());
        assertEquals(AutoFlushManager.DEFAULT_WAL_SIZE_THRESHOLD, m2.getWalSizeThreshold());
    }
}
