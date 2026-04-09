package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.GlobalPageCache;
import cn.abelib.minedb.utils.KeyValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
    }

    @Override
    public String get(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        return tree.get(key);
    }

    @Override
    public boolean delete(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        boolean deleted = tree.delete(key);
        if (deleted) {
            size.decrementAndGet();
        }
        return deleted;
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
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        try {
            tree.flush(size.get());
        } catch (Exception e) {
            System.err.println("Warning: Failed to flush data during close: " + e.getMessage());
        } finally {
            GlobalPageCache.getInstance().clear();
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
