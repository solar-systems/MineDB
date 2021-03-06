package cn.abelib.minedb.index;

import com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;

/**
 * @Author: abel.huang
 * @Date: 2020-11-05 01:54
 * 全局页缓存
 */
public class GlobalPageCache {
    private static ConcurrentMap<Long, TreeNode> dirtyPages;
    private static ConcurrentMap<Long, TreeNode> cleanPages;

    static {
        dirtyPages = Maps.newConcurrentMap();
        cleanPages = Maps.newConcurrentMap();
    }

    /**
     * 基于页码获得已经在缓存中的页
     * @param pageNo
     * @return
     */
    public static TreeNode getPage(long pageNo) {
        return dirtyPages.get(pageNo);
    }

    public static void putDirtyPage(TreeNode node) {
        long pageNo = node.getPageNo();
        cleanPages.remove(pageNo);
        dirtyPages.put(pageNo, node);
    }

    public static void putCleanPage(TreeNode node) {
        long pageNo = node.getPageNo();
        dirtyPages.remove(pageNo);
        cleanPages.put(pageNo, node);
    }
}
