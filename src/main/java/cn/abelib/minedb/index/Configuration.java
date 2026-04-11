package cn.abelib.minedb.index;

import cn.abelib.minedb.index.autoflush.AutoFlushManager;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: abel.huang
 * @Date: 2020-10-23 23:57
 */
public class Configuration {
    /**
     * 单个页大小， 默认16kB
     */
    private int pageSize;
    /**
     * 每个页头部大小，
     * 会有部分空缺, 由配置文件决定
     */
    private int headerSize;
    /**
     * 实际范围是[headerSize, headerSize + childrenSize]
     * 子节点指针域大小，每个指针都是8个字节
     * 可以支持的最大孩子数 = childrenSize / 8
     */
    private int childrenSize;

    /** 页缓存最大页数 */
    private int cacheSize;

    /** 自动刷盘间隔（毫秒） */
    private long autoFlushIntervalMs;

    /** 脏页阈值，超过此数量触发自动刷盘 */
    private int dirtyPageThreshold;

    /** WAL 大小阈值（字节），超过此大小触发自动刷盘 */
    private long walSizeThreshold;

    /** 是否启用自动刷盘 */
    private boolean autoFlushEnabled;

    private String dbName;

    public Configuration() {
        this(16 * 1024, 128, 1024);
    }

    public Configuration(int pageSize, int headerSize, int childrenSize) {
        this(pageSize, headerSize, childrenSize, PageCache.DEFAULT_MAX_PAGES);
    }

    public Configuration(int pageSize, int headerSize, int childrenSize, int cacheSize) {
        this(pageSize, headerSize, childrenSize, cacheSize, true);
    }

    public Configuration(int pageSize, int headerSize, int childrenSize, int cacheSize, boolean autoFlushEnabled) {
        this.pageSize = pageSize;
        this.headerSize = headerSize;
        this.childrenSize = childrenSize;
        this.cacheSize = cacheSize;
        this.autoFlushEnabled = autoFlushEnabled;
        this.autoFlushIntervalMs = AutoFlushManager.DEFAULT_FLUSH_INTERVAL_MS;
        this.dirtyPageThreshold = AutoFlushManager.DEFAULT_DIRTY_PAGE_THRESHOLD;
        this.walSizeThreshold = AutoFlushManager.DEFAULT_WAL_SIZE_THRESHOLD;
        this.dbName = "default.db";
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    }

    public int getChildrenSize() {
        return childrenSize;
    }

    public void setChildrenSize(int childrenSize) {
        this.childrenSize = childrenSize;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Path getPath() {
        return Paths.get(getDbName());
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getAutoFlushIntervalMs() {
        return autoFlushIntervalMs;
    }

    public void setAutoFlushIntervalMs(long autoFlushIntervalMs) {
        this.autoFlushIntervalMs = autoFlushIntervalMs;
    }

    public int getDirtyPageThreshold() {
        return dirtyPageThreshold;
    }

    public void setDirtyPageThreshold(int dirtyPageThreshold) {
        this.dirtyPageThreshold = dirtyPageThreshold;
    }

    public long getWalSizeThreshold() {
        return walSizeThreshold;
    }

    public void setWalSizeThreshold(long walSizeThreshold) {
        this.walSizeThreshold = walSizeThreshold;
    }

    public boolean isAutoFlushEnabled() {
        return autoFlushEnabled;
    }

    public void setAutoFlushEnabled(boolean autoFlushEnabled) {
        this.autoFlushEnabled = autoFlushEnabled;
    }

}


