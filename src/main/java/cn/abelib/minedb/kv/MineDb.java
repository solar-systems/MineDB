package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.autoflush.AutoFlushManager;
import cn.abelib.minedb.utils.KeyValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MineDB KV 存储实现
 *
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:00
 */
public class MineDb implements Db {

    public static final int DEFAULT_PAGE_SIZE = 16 * 1024;
    public static final int DEFAULT_HEADER_SIZE = 128;
    public static final int DEFAULT_CHILDREN_SIZE = 1024;

    private final String dbName;
    private final Configuration configuration;
    private BalanceTree tree;
    private AutoFlushManager autoFlushManager;
    private TransactionManager transactionManager;
    private final AtomicLong size;
    private volatile boolean closed = false;

    public MineDb(String dbName) throws IOException {
        this(dbName, new Configuration(DEFAULT_PAGE_SIZE, DEFAULT_HEADER_SIZE, DEFAULT_CHILDREN_SIZE));
    }

    public MineDb(String dbName, Configuration configuration) throws IOException {
        Objects.requireNonNull(dbName, "Database name cannot be null");
        Objects.requireNonNull(configuration, "Configuration cannot be null");

        this.dbName = dbName;
        this.configuration = configuration;
        this.configuration.setDbName(dbName);
        this.tree = new BalanceTree(configuration);

        long savedCount = tree.getMetaNode() != null ? tree.getMetaNode().getEntryCount() : 0;
        this.size = new AtomicLong(savedCount);

        // 初始化自动刷盘
        if (configuration.isAutoFlushEnabled()) {
            this.autoFlushManager = new AutoFlushManager(
                    tree,
                    configuration.getAutoFlushIntervalMs(),
                    configuration.getDirtyPageThreshold(),
                    configuration.getWalSizeThreshold()
            );
            this.autoFlushManager.start();
        }

        // 初始化事务管理器
        this.transactionManager = new TransactionManager(tree, tree.getWalLog());
    }

    @Override
    public void put(String key, String value) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        boolean exists = tree.contains(key);
        tree.insert(key, value);

        if (!exists) {
            size.incrementAndGet();
        }

        // 检查是否需要自动刷盘
        checkAutoFlush();
    }

    @Override
    public int batchPut(Map<String, String> keyValues) throws Exception {
        checkClosed();
        Objects.requireNonNull(keyValues, "KeyValues cannot be null");

        if (keyValues.isEmpty()) {
            return 0;
        }

        int count = 0;
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null || value == null) {
                continue;
            }

            boolean exists = tree.contains(key);
            tree.insert(key, value);

            if (!exists) {
                size.incrementAndGet();
            }
            count++;
        }

        // 批量操作后只检查一次自动刷盘
        checkAutoFlush();

        return count;
    }

    @Override
    public int batchPut(List<KeyValue> keyValues) throws Exception {
        checkClosed();
        Objects.requireNonNull(keyValues, "KeyValues cannot be null");

        if (keyValues.isEmpty()) {
            return 0;
        }

        int count = 0;
        for (KeyValue kv : keyValues) {
            if (kv == null || kv.getKey() == null || kv.getValue() == null) {
                continue;
            }

            boolean exists = tree.contains(kv.getKey());
            tree.insert(kv.getKey(), kv.getValue());

            if (!exists) {
                size.incrementAndGet();
            }
            count++;
        }

        // 批量操作后只检查一次自动刷盘
        checkAutoFlush();

        return count;
    }

    @Override
    public String get(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        return tree.get(key);
    }

    @Override
    public Map<String, String> batchGet(List<String> keys) throws Exception {
        checkClosed();
        Objects.requireNonNull(keys, "Keys cannot be null");

        Map<String, String> result = new HashMap<>();
        if (keys.isEmpty()) {
            return result;
        }

        for (String key : keys) {
            if (key == null) {
                continue;
            }
            String value = tree.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }

        return result;
    }

    @Override
    public boolean delete(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        boolean deleted = tree.delete(key);
        if (deleted) {
            size.decrementAndGet();
        }

        // 检查是否需要自动刷盘
        checkAutoFlush();

        return deleted;
    }

    @Override
    public int batchDelete(List<String> keys) throws Exception {
        checkClosed();
        Objects.requireNonNull(keys, "Keys cannot be null");

        if (keys.isEmpty()) {
            return 0;
        }

        int count = 0;
        for (String key : keys) {
            if (key == null) {
                continue;
            }

            boolean deleted = tree.delete(key);
            if (deleted) {
                size.decrementAndGet();
                count++;
            }
        }

        // 批量操作后只检查一次自动刷盘
        checkAutoFlush();

        return count;
    }

    @Override
    public boolean contains(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        return tree.contains(key);
    }

    @Override
    public List<KeyValue> range(String start, String end) throws Exception {
        checkClosed();
        Objects.requireNonNull(start, "Start key cannot be null");
        Objects.requireNonNull(end, "End key cannot be null");

        return tree.range(start, end);
    }

    @Override
    public List<KeyValue> prefix(String prefix) throws Exception {
        checkClosed();
        Objects.requireNonNull(prefix, "Prefix cannot be null");

        return tree.prefix(prefix);
    }

    @Override
    public long size() {
        return size.get();
    }

    @Override
    public void clear() throws Exception {
        checkClosed();

        Path dbPath = Paths.get(dbName);
        if (Files.exists(dbPath)) {
            Files.delete(dbPath);
        }

        this.tree = new BalanceTree(configuration);
        size.set(0);
    }

    @Override
    public void sync() throws Exception {
        checkClosed();

        tree.flush(size.get());
    }

    @Override
    public Iterator<KeyValue> iterator() throws Exception {
        checkClosed();
        return new DbIterator(tree);
    }

    @Override
    public Iterator<KeyValue> iterator(String startKey) throws Exception {
        checkClosed();
        return new DbIterator(tree, startKey);
    }

    @Override
    public Transaction beginTransaction() throws Exception {
        checkClosed();
        return transactionManager.beginTransaction();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        // 停止自动刷盘
        if (autoFlushManager != null) {
            autoFlushManager.stop();
        }

        try {
            tree.flush(size.get());
        } catch (Exception e) {
            System.err.println("Warning: Failed to flush data during close: " + e.getMessage());
        } finally {
            tree.clearCache();
            tree.closeWal();
        }
    }

    /**
     * 检查是否需要自动刷盘
     */
    private void checkAutoFlush() {
        if (autoFlushManager != null) {
            autoFlushManager.checkAndFlush();
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
    }

    public String getDbName() {
        return dbName;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public BalanceTree getTree() {
        return tree;
    }

    public AutoFlushManager getAutoFlushManager() {
        return autoFlushManager;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return "MineDb{" +
                "dbName='" + dbName + '\'' +
                ", size=" + size.get() +
                ", closed=" + closed +
                '}';
    }
}
