package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 全局页缓存
 *
 * <p>已废弃：请使用 {@link PageCache} 代替。每个数据库实例应该有独立的页缓存。
 *
 * @author abel.huang
 * @date 2020-11-07 15:36
 * @deprecated 使用 {@link PageCache} 代替，每个数据库实例有独立的缓存
 * @see PageCache
 */
@Deprecated
public class GlobalPageCache {
    private static final GlobalPageCache INSTANCE = new GlobalPageCache();

    private final ConcurrentMap<Long, TreeNode> dirtyPages;
    private final ConcurrentMap<Long, TreeNode> cleanPages;
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();

    private GlobalPageCache() {
        dirtyPages = Maps.newConcurrentMap();
        cleanPages = Maps.newConcurrentMap();
    }

    public static GlobalPageCache getInstance() {
        return INSTANCE;
    }

    public static TreeNode getPage(long position) {
        INSTANCE.flushLock.readLock().lock();
        try {
            TreeNode node = INSTANCE.dirtyPages.get(position);
            if (node != null) {
                return node;
            }
            return INSTANCE.cleanPages.get(position);
        } finally {
            INSTANCE.flushLock.readLock().unlock();
        }
    }

    public static void putDirtyPage(TreeNode node) {
        INSTANCE.flushLock.readLock().lock();
        try {
            long position = node.getPosition();
            INSTANCE.cleanPages.remove(position);
            INSTANCE.dirtyPages.put(position, node);
        } finally {
            INSTANCE.flushLock.readLock().unlock();
        }
    }

    public static void putCleanPage(TreeNode node) {
        INSTANCE.flushLock.readLock().lock();
        try {
            long position = node.getPosition();
            INSTANCE.dirtyPages.remove(position);
            INSTANCE.cleanPages.put(position, node);
        } finally {
            INSTANCE.flushLock.readLock().unlock();
        }
    }

    /**
     * 刷新所有脏页到磁盘
     * 使用写锁确保刷新过程中不会有新的页面操作
     */
    public void flushDirtyPages() throws IOException {
        flushLock.writeLock().lock();
        try {
            // 复制当前脏页列表，避免迭代时并发修改
            List<TreeNode> pagesToFlush = new ArrayList<>(dirtyPages.values());

            for (TreeNode node : pagesToFlush) {
                node.setPage(new Page(node));
                PageLoader.writePage(node);
                // 移动到净页
                cleanPages.put(node.getPosition(), node);
                dirtyPages.remove(node.getPosition());
            }
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    public int getDirtyPageCount() {
        flushLock.readLock().lock();
        try {
            return dirtyPages.size();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public int getCleanPageCount() {
        flushLock.readLock().lock();
        try {
            return cleanPages.size();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public int getTotalPageCount() {
        flushLock.readLock().lock();
        try {
            return dirtyPages.size() + cleanPages.size();
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public void clear() {
        flushLock.writeLock().lock();
        try {
            dirtyPages.clear();
            cleanPages.clear();
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    public Collection<TreeNode> getDirtyPages() {
        flushLock.readLock().lock();
        try {
            return new ArrayList<>(dirtyPages.values());
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public Collection<TreeNode> getCleanPages() {
        flushLock.readLock().lock();
        try {
            return new ArrayList<>(cleanPages.values());
        } finally {
            flushLock.readLock().unlock();
        }
    }

    /**
     * 移除指定位置的页面
     */
    public void removePage(long position) {
        flushLock.writeLock().lock();
        try {
            dirtyPages.remove(position);
            cleanPages.remove(position);
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    /**
     * 检查页面是否存在于缓存中
     */
    public boolean containsPage(long position) {
        flushLock.readLock().lock();
        try {
            return dirtyPages.containsKey(position) || cleanPages.containsKey(position);
        } finally {
            flushLock.readLock().unlock();
        }
    }
}
