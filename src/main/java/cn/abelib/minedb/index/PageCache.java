package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 页缓存（支持 LRU 淘汰）
 *
 * <p>每个数据库实例拥有独立的页缓存，支持脏页和净页管理。
 * 当缓存达到上限时，使用 LRU 算法淘汰最久未使用的净页。
 *
 * <p>淘汰策略：
 * <ul>
 *   <li>脏页不能直接淘汰，需要先刷盘转为净页</li>
 *   <li>优先淘汰净页中的 LRU 页面</li>
 *   <li>访问页面时自动更新访问顺序</li>
 * </ul>
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class PageCache {
    /** 默认最大缓存页数 */
    public static final int DEFAULT_MAX_PAGES = 1000;

    /** 脏页缓存：已修改但未持久化 */
    private final Map<Long, TreeNode> dirtyPages;
    /** 净页缓存：与磁盘一致，使用 LRU 淘汰 */
    private final LinkedHashMap<Long, TreeNode> cleanPages;
    /** 读写锁保护缓存操作 */
    private final ReadWriteLock lock;
    /** 最大缓存页数 */
    private final int maxPages;
    /** 淘汰计数器 */
    private long evictionCount;
    /** 命中计数器 */
    private long hitCount;
    /** 未命中计数器 */
    private long missCount;

    /**
     * 使用默认缓存大小创建
     */
    public PageCache() {
        this(DEFAULT_MAX_PAGES);
    }

    /**
     * 指定缓存大小创建
     *
     * @param maxPages 最大缓存页数
     */
    public PageCache(int maxPages) {
        this.maxPages = maxPages > 0 ? maxPages : DEFAULT_MAX_PAGES;
        this.dirtyPages = new HashMap<>();
        // accessOrder=true 表示按访问顺序排序
        this.cleanPages = new LinkedHashMap<>(16, 0.75f, true);
        this.lock = new ReentrantReadWriteLock();
        this.evictionCount = 0;
        this.hitCount = 0;
        this.missCount = 0;
    }

    /**
     * 获取指定位置的页面（更新访问顺序）
     */
    public TreeNode getPage(long position) {
        lock.writeLock().lock();
        try {
            // 先查脏页
            TreeNode node = dirtyPages.get(position);
            if (node != null) {
                hitCount++;
                return node;
            }

            // 再查净页（LinkedHashMap 会自动更新访问顺序）
            node = cleanPages.get(position);
            if (node != null) {
                hitCount++;
                return node;
            }

            missCount++;
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 添加脏页到缓存
     */
    public void putDirtyPage(TreeNode node) {
        lock.writeLock().lock();
        try {
            long position = node.getPosition();

            // 从净页移除（如果存在）
            cleanPages.remove(position);

            // 添加到脏页
            dirtyPages.put(position, node);

            // 检查是否需要淘汰
            evictIfNeeded();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 添加净页到缓存
     */
    public void putCleanPage(TreeNode node) {
        lock.writeLock().lock();
        try {
            long position = node.getPosition();

            // 从脏页移除（如果存在）
            dirtyPages.remove(position);

            // 添加到净页
            cleanPages.put(position, node);

            // 检查是否需要淘汰
            evictIfNeeded();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 检查并执行淘汰
     */
    private void evictIfNeeded() {
        // 已经持有写锁
        while (getTotalPageCountInternal() > maxPages) {
            // 优先淘汰净页
            if (!cleanPages.isEmpty()) {
                evictOneCleanPage();
            } else if (!dirtyPages.isEmpty()) {
                // 净页为空，需要先刷盘一个脏页再淘汰
                evictOneDirtyPage();
            } else {
                break;
            }
        }
    }

    /**
     * 淘汰一个最久未使用的净页
     */
    private void evictOneCleanPage() {
        // LinkedHashMap 的迭代顺序就是访问顺序（LRU）
        Iterator<Map.Entry<Long, TreeNode>> iterator = cleanPages.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<Long, TreeNode> entry = iterator.next();
            iterator.remove();
            evictionCount++;
        }
    }

    /**
     * 淘汰一个脏页（先刷盘再移除）
     */
    private void evictOneDirtyPage() {
        Iterator<Map.Entry<Long, TreeNode>> iterator = dirtyPages.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<Long, TreeNode> entry = iterator.next();
            TreeNode node = entry.getValue();

            try {
                // 刷盘
                node.setPage(new Page(node));
                PageLoader.writePage(node);
            } catch (IOException e) {
                // 刷盘失败，不移除
                System.err.println("Warning: Failed to flush dirty page during eviction: " + e.getMessage());
                return;
            }

            iterator.remove();
            evictionCount++;
        }
    }

    /**
     * 刷新所有脏页到磁盘
     */
    public void flushDirtyPages() throws IOException {
        lock.writeLock().lock();
        try {
            List<TreeNode> pagesToFlush = new ArrayList<>(dirtyPages.values());

            for (TreeNode node : pagesToFlush) {
                node.setPage(new Page(node));
                PageLoader.writePage(node);
                cleanPages.put(node.getPosition(), node);
                dirtyPages.remove(node.getPosition());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取脏页数量
     */
    public int getDirtyPageCount() {
        lock.readLock().lock();
        try {
            return dirtyPages.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取净页数量
     */
    public int getCleanPageCount() {
        lock.readLock().lock();
        try {
            return cleanPages.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取总页数
     */
    public int getTotalPageCount() {
        lock.readLock().lock();
        try {
            return getTotalPageCountInternal();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取总页数（内部方法，不获取锁）
     */
    private int getTotalPageCountInternal() {
        return dirtyPages.size() + cleanPages.size();
    }

    /**
     * 清空缓存
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            dirtyPages.clear();
            cleanPages.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取所有脏页
     */
    public Collection<TreeNode> getDirtyPages() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(dirtyPages.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取所有净页
     */
    public Collection<TreeNode> getCleanPages() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(cleanPages.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 移除指定位置的页面
     */
    public void removePage(long position) {
        lock.writeLock().lock();
        try {
            dirtyPages.remove(position);
            cleanPages.remove(position);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 检查页面是否存在于缓存中
     */
    public boolean containsPage(long position) {
        lock.readLock().lock();
        try {
            return dirtyPages.containsKey(position) || cleanPages.containsKey(position);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取最大缓存页数
     */
    public int getMaxPages() {
        return maxPages;
    }

    /**
     * 获取淘汰次数
     */
    public long getEvictionCount() {
        lock.readLock().lock();
        try {
            return evictionCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取命中次数
     */
    public long getHitCount() {
        lock.readLock().lock();
        try {
            return hitCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取未命中次数
     */
    public long getMissCount() {
        lock.readLock().lock();
        try {
            return missCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取命中率
     */
    public double getHitRate() {
        lock.readLock().lock();
        try {
            long total = hitCount + missCount;
            if (total == 0) {
                return 0.0;
            }
            return (double) hitCount / total;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 重置统计信息
     */
    public void resetStats() {
        lock.writeLock().lock();
        try {
            evictionCount = 0;
            hitCount = 0;
            missCount = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "PageCache{" +
                    "maxPages=" + maxPages +
                    ", dirtyPages=" + dirtyPages.size() +
                    ", cleanPages=" + cleanPages.size() +
                    ", evictionCount=" + evictionCount +
                    ", hitCount=" + hitCount +
                    ", missCount=" + missCount +
                    ", hitRate=" + String.format("%.2f%%", getHitRate() * 100) +
                    '}';
        } finally {
            lock.readLock().unlock();
        }
    }
}
