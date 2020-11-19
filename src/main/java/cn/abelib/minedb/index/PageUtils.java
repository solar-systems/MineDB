package cn.abelib.minedb.index;

import cn.abelib.minedb.utils.KeyValue;

import java.util.List;

/**
 * @Author: abel.huang
 * @Date: 2020-11-07 15:36
 */
public class PageUtils {

    /**
     * 找到entry应该所处于的位置
     * @param entry
     * @param keyValues
     * @return
     */
    public static int binarySearchForIndex(KeyValue entry, List<KeyValue> keyValues) {
        if (keyValues.size() <= 0) {
            return 0;
        }
        int left = 0;
        int right = keyValues.size() - 1;
        int mid;
        int index = -1;
        // 如果小于第一个元素，那位置就在第一个位置
        if (entry.compareTo(keyValues.get(left)) < 0) {
            return 0;
        }
        // 如果大于最后一个元素，那位置就在集合最后一个位置
        if (entry.compareTo(keyValues.get(right)) >= 0) {
            return keyValues.size();
        }
        while (left <= right) {
            mid = left + (right - left) / 2;
            if (entry.compareTo(keyValues.get(mid)) < 0 && entry.compareTo(keyValues.get(mid - 1)) >= 0) {
                index = mid;
                break;
            } else if (entry.compareTo(keyValues.get(mid)) >= 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return index;
    }

    /**
     * 标准的二分搜索算法，没有查找到则返回-1
     * @param entry
     * @param keyValues
     * @return
     */
    public static int binarySearch(KeyValue entry, List<KeyValue> keyValues) {
        int left = 0;
        int right = keyValues.size() - 1;
        int mid;
        int index = -1;
        while (left <= right) {
            mid = left + (right - left) / 2;
            if (entry.compareTo(keyValues.get(mid)) == 0) {
                index = mid;
                break;
            } else if (entry.compareTo(keyValues.get(mid)) >= 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return index;
    }

    /**
     * todo
     * 找到TreeNode的中间节点
     * @param curr
     * @return
     */
    public static int midIndex(TreeNode curr) {
        return 0;
    }

    /**
     * todo 判断叶子节点是否已经满了
     * @param node
     * @return
     */
    public static boolean isFull(TreeNode node) {
        NodeType type = nodeType(node);
        if (type == NodeType.SPECIAL_NODE) {
            // 需要考虑指针域和值域溢出

        } else if (type == NodeType.ROOT_NODE) {
            // 需要考虑指针域溢出

        } else if (type == NodeType.LEAF_NODE) {
            // 需要考虑值域溢出

        } else if (type == NodeType.INDEX_NODE) {
            // 需要考虑指针域溢出

        }
        return false;
    }

    public static NodeType nodeType(TreeNode node) {
        boolean isRoot = node.isRoot();
        boolean isLeaf = node.isLeaf();

        if (isRoot && isLeaf) {
            return NodeType.SPECIAL_NODE;
        } else if (isRoot && !isLeaf) {
            return NodeType.ROOT_NODE;
        } else if (!isRoot && isLeaf) {
            return NodeType.LEAF_NODE;
        } else {
            return NodeType.INDEX_NODE;
        }
    }

    protected enum NodeType {
        /**
         * 根节点
         */
        ROOT_NODE,
        /**
         * 叶节点
         */
        LEAF_NODE,
        /**
         * 索引节点
         */
        INDEX_NODE,
        /**
         * 唯一根节点
         */
        SPECIAL_NODE
    }
}
