package cn.abelib.minedb.index.autoflush;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.PageCache;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 自动刷盘管理器
 *
 * <p>提供两种自动刷盘策略：
 * <ul>
 *   <li>定时刷盘：按固定时间间隔自动刷盘</li>
 *   <li>阈值刷盘：当脏页数量达到阈值时触发刷盘</li>
 * </ul>
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class AutoFlushManager {
    /** 默认刷盘间隔（毫秒）: 5秒 */
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 5000;

    /** 默认脏页阈值 */
    public static final int DEFAULT_DIRTY_PAGE_THRESHOLD = 100;

    /** 默认 WAL 大小阈值（字节）: 10MB */
    public static final long DEFAULT_WAL_SIZE_THRESHOLD = 10 * 1024 * 1024;

    private final BalanceTree tree;
    private final long flushIntervalMs;
    private final int dirtyPageThreshold;
    private final long walSizeThreshold;

    private ScheduledExecutorService scheduler;
    private final AtomicBoolean running;
    private final AtomicLong lastFlushTime;
    private final AtomicLong flushCount;
    private final ReentrantLock flushLock;

    /**
     * 使用默认配置创建
     *
     * @param tree B+ 树实例
     */
    public AutoFlushManager(BalanceTree tree) {
        this(tree, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_DIRTY_PAGE_THRESHOLD, DEFAULT_WAL_SIZE_THRESHOLD);
    }

    /**
     * 使用自定义配置创建
     *
     * @param tree B+ 树实例
     * @param flushIntervalMs 刷盘间隔（毫秒）
     * @param dirtyPageThreshold 脏页阈值
     * @param walSizeThreshold WAL 大小阈值（字节）
     */
    public AutoFlushManager(BalanceTree tree, long flushIntervalMs, int dirtyPageThreshold, long walSizeThreshold) {
        this.tree = tree;
        this.flushIntervalMs = flushIntervalMs > 0 ? flushIntervalMs : DEFAULT_FLUSH_INTERVAL_MS;
        this.dirtyPageThreshold = dirtyPageThreshold > 0 ? dirtyPageThreshold : DEFAULT_DIRTY_PAGE_THRESHOLD;
        this.walSizeThreshold = walSizeThreshold > 0 ? walSizeThreshold : DEFAULT_WAL_SIZE_THRESHOLD;
        this.running = new AtomicBoolean(false);
        this.lastFlushTime = new AtomicLong(0);
        this.flushCount = new AtomicLong(0);
        this.flushLock = new ReentrantLock();
    }

    /**
     * 启动自动刷盘
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "MineDB-AutoFlush");
                t.setDaemon(true);
                return t;
            });

            scheduler.scheduleAtFixedRate(
                    this::autoFlush,
                    flushIntervalMs,
                    flushIntervalMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    /**
     * 停止自动刷盘
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 检查是否需要刷盘（写操作后调用）
     */
    public void checkAndFlush() {
        if (!running.get()) {
            return;
        }

        PageCache pageCache = tree.getPageCache();
        if (pageCache == null) {
            return;
        }

        // 检查脏页阈值
        int dirtyPageCount = pageCache.getDirtyPageCount();
        if (dirtyPageCount >= dirtyPageThreshold) {
            triggerFlush("dirty page threshold: " + dirtyPageCount);
            return;
        }

        // 检查 WAL 大小阈值
        if (tree.getWalLog() != null) {
            try {
                long walSize = tree.getWalLog().getFileSize();
                if (walSize >= walSizeThreshold) {
                    triggerFlush("WAL size threshold: " + walSize);
                }
            } catch (IOException e) {
                // 忽略检查错误
            }
        }
    }

    /**
     * 触发刷盘
     */
    private void triggerFlush(String reason) {
        if (flushLock.tryLock()) {
            try {
                doFlush(reason);
            } finally {
                flushLock.unlock();
            }
        }
        // 如果已经在刷盘，跳过本次
    }

    /**
     * 定时刷盘任务
     */
    private void autoFlush() {
        if (!running.get()) {
            return;
        }

        if (flushLock.tryLock()) {
            try {
                // 检查是否有脏页
                PageCache pageCache = tree.getPageCache();
                if (pageCache != null && pageCache.getDirtyPageCount() > 0) {
                    doFlush("scheduled");
                }
            } catch (Exception e) {
                System.err.println("Auto flush error: " + e.getMessage());
            } finally {
                flushLock.unlock();
            }
        }
    }

    /**
     * 执行刷盘
     */
    private void doFlush(String reason) {
        try {
            long startTime = System.currentTimeMillis();

            // 获取当前条目数
            long entryCount = tree.getMetaNode() != null ? tree.getMetaNode().getEntryCount() : 0;

            // 执行刷盘
            tree.flush(entryCount);

            lastFlushTime.set(System.currentTimeMillis());
            flushCount.incrementAndGet();

            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Auto flush completed (" + reason + ") in " + duration + "ms");

        } catch (IOException e) {
            System.err.println("Auto flush failed: " + e.getMessage());
        }
    }

    /**
     * 手动触发刷盘
     */
    public void flushNow() throws IOException {
        flushLock.lock();
        try {
            doFlush("manual");
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * 获取刷盘间隔
     */
    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    /**
     * 获取脏页阈值
     */
    public int getDirtyPageThreshold() {
        return dirtyPageThreshold;
    }

    /**
     * 获取 WAL 大小阈值
     */
    public long getWalSizeThreshold() {
        return walSizeThreshold;
    }

    /**
     * 获取上次刷盘时间
     */
    public long getLastFlushTime() {
        return lastFlushTime.get();
    }

    /**
     * 获取刷盘次数
     */
    public long getFlushCount() {
        return flushCount.get();
    }

    /**
     * 是否正在运行
     */
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public String toString() {
        return "AutoFlushManager{" +
                "flushIntervalMs=" + flushIntervalMs +
                ", dirtyPageThreshold=" + dirtyPageThreshold +
                ", walSizeThreshold=" + walSizeThreshold +
                ", running=" + running.get() +
                ", flushCount=" + flushCount.get() +
                '}';
    }
}
