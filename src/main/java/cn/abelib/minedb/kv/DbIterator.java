package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.TreeNode;
import cn.abelib.minedb.utils.KeyValue;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 数据库迭代器
 *
 * <p>遍历 B+ 树中的所有键值对，利用叶子节点的链表结构实现高效遍历。
 *
 * <p>遍历顺序：按键的字典序从小到大
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class DbIterator implements Iterator<KeyValue> {
    private final BalanceTree tree;
    private TreeNode currentNode;
    private int currentIndex;
    private KeyValue next;
    private boolean initialized;

    /**
     * 创建迭代器，从头开始遍历
     *
     * @param tree B+ 树实例
     */
    public DbIterator(BalanceTree tree) {
        this(tree, null);
    }

    /**
     * 创建迭代器，从指定键开始遍历
     *
     * @param tree B+ 树实例
     * @param startKey 起始键（包含），null 表示从头开始
     */
    public DbIterator(BalanceTree tree, String startKey) {
        this.tree = tree;
        this.currentNode = null;
        this.currentIndex = 0;
        this.next = null;
        this.initialized = false;

        // 初始化定位
        if (startKey != null) {
            initWithStartKey(startKey);
        }
    }

    /**
     * 从指定键开始初始化
     */
    private void initWithStartKey(String startKey) {
        initialized = true;

        try {
            // 找到包含 startKey 的叶子节点
            TreeNode leaf = tree.findLeafNodeByKey(startKey);
            if (leaf == null) {
                return;
            }

            currentNode = leaf;
            List<KeyValue> keyValues = currentNode.getKeyValues();

            // 二分查找定位
            int left = 0;
            int right = keyValues.size();

            while (left < right) {
                int mid = (left + right) / 2;
                KeyValue kv = keyValues.get(mid);
                if (kv.getKey().compareTo(startKey) < 0) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            currentIndex = left;
            findNext();
        } catch (Exception e) {
            // 初始化失败
            next = null;
        }
    }

    /**
     * 初始化到第一个叶子节点
     */
    private void initToFirstLeaf() {
        initialized = true;

        try {
            // 从根节点找到最左边的叶子节点
            TreeNode node = tree.getRoot();
            if (node == null) {
                return;
            }

            while (!node.getChildren().isEmpty()) {
                node = node.getChildren().get(0);
            }

            currentNode = node;
            currentIndex = 0;
            findNext();
        } catch (Exception e) {
            // 初始化失败
            next = null;
        }
    }

    /**
     * 查找下一个元素
     */
    private void findNext() {
        next = null;

        while (currentNode != null) {
            List<KeyValue> keyValues = currentNode.getKeyValues();

            if (currentIndex < keyValues.size()) {
                // 当前节点还有元素
                next = keyValues.get(currentIndex);
                currentIndex++;
                return;
            }

            // 当前节点遍历完毕，移动到下一个叶子节点
            try {
                currentNode = currentNode.getNext();
            } catch (Exception e) {
                // 加载下一个节点失败，停止遍历
                currentNode = null;
            }
            currentIndex = 0;
        }
    }

    @Override
    public boolean hasNext() {
        if (!initialized) {
            initToFirstLeaf();
        }
        return next != null;
    }

    @Override
    public KeyValue next() {
        if (!initialized) {
            initToFirstLeaf();
        }

        if (next == null) {
            throw new NoSuchElementException("No more elements");
        }

        KeyValue result = next;
        findNext();
        return result;
    }

    /**
     * 跳过指定数量的元素
     *
     * @param n 要跳过的数量
     * @return 实际跳过的数量
     */
    public long skip(long n) {
        if (!initialized) {
            initToFirstLeaf();
        }

        long skipped = 0;
        while (skipped < n && hasNext()) {
            next();
            skipped++;
        }
        return skipped;
    }

    /**
     * 获取当前迭代位置（仅用于调试）
     */
    public String getCurrentPosition() {
        if (currentNode == null) {
            return "end";
        }
        return "node=" + currentNode.getPageNo() + ", index=" + currentIndex;
    }
}
