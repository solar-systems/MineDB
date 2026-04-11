package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.OverFlowPage;
import cn.abelib.minedb.index.fs.Record;
import cn.abelib.minedb.utils.ByteUtils;
import cn.abelib.minedb.utils.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * 页面工具类
 *
 * @Author: abel.huang
 * @Date: 2020-11-07 15:36
 */
public class PageUtils {

    /**
     * 找到entry应该所处于的位置
     * 在 B+ 树内部节点中，返回应该进入的子节点索引
     * 如果 entry < keys[0]，返回 0
     * 如果 keys[i] <= entry < keys[i+1]，返回 i+1
     * 如果 entry >= keys[last]，返回 keys.size()
     */
    public static int binarySearchForIndex(KeyValue entry, List<KeyValue> keyValues) {
        if (keyValues.isEmpty()) {
            return 0;
        }

        int left = 0;
        int right = keyValues.size() - 1;

        // 如果小于第一个键，进入第一个子节点
        if (entry.compareTo(keyValues.get(left)) < 0) {
            return 0;
        }

        // 如果大于等于最后一个键，进入最后一个子节点
        if (entry.compareTo(keyValues.get(right)) >= 0) {
            return keyValues.size();
        }

        // 二分查找：找到第一个大于 entry 的键的位置
        // 返回该位置作为子节点索引
        int result = keyValues.size();
        while (left <= right) {
            int mid = left + (right - left) / 2;
            int cmp = entry.compareTo(keyValues.get(mid));

            if (cmp < 0) {
                // entry 小于 keys[mid]，可能在这个位置
                result = mid;
                right = mid - 1;
            } else {
                // entry 大于等于 keys[mid]，继续向右搜索
                left = mid + 1;
            }
        }

        return result;
    }

    /**
     * 标准的二分搜索算法，没有查找到则返回-1
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
     * 找到TreeNode的中间节点索引
     */
    public static int midIndex(TreeNode curr) {
        int size = curr.getKeyValues().size();
        if (size <= 0) {
            return 0;
        }
        return size / 2;
    }

    /**
     * 判断叶子节点是否已经满了
     */
    public static boolean isFull(TreeNode node) {
        if (node == null) {
            return false;
        }

        try {
            NodeType type = nodeType(node);
            Configuration conf = node.getConfiguration();
            int usedSpace = calculateUsedSpace(node);
            int pageSize = conf.getPageSize();
            int headerSize = conf.getHeaderSize();
            int availableSpace = pageSize - headerSize;

            switch (type) {
                case SPECIAL_NODE:
                    // 唯一根节点，需要考虑键值域和指针域
                    return usedSpace >= availableSpace * 0.75;
                case ROOT_NODE:
                case INDEX_NODE:
                    // 根节点和索引节点，主要考虑指针域
                    int childCount = node.getChildren().size();
                    int maxChildren = conf.getChildrenSize() / 8;
                    return childCount >= maxChildren;
                case LEAF_NODE:
                    // 叶子节点，考虑数据域
                    return usedSpace >= availableSpace;
                default:
                    return false;
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 计算记录的磁盘大小
     */
    public static int calculateRecordSize(KeyValue keyValue) {
        if (keyValue == null) {
            return 0;
        }
        byte[] keyBytes = ByteUtils.getBytesUTF8(keyValue.getKey());
        byte[] valueBytes = ByteUtils.getBytesUTF8(keyValue.getValue());
        return Record.calculateRecordSize(keyBytes.length, valueBytes.length, false);
    }

    /**
     * 计算节点已使用的空间
     */
    public static int calculateUsedSpace(TreeNode node) {
        if (node == null) {
            return 0;
        }

        int totalSize = 0;
        List<KeyValue> keyValues = node.getKeyValues();

        for (KeyValue kv : keyValues) {
            totalSize += calculateRecordSize(kv);
        }

        // 添加指针域（非叶子节点）
        try {
            if (!node.isLeaf() && node.getChildren() != null) {
                totalSize += node.getChildren().size() * 8;
            }
        } catch (IOException e) {
            // 忽略IO异常
        }

        return totalSize;
    }

    /**
     * 获取页面可用数据空间
     */
    public static int getAvailableDataSpace(Configuration conf) {
        return conf.getPageSize() - conf.getHeaderSize();
    }

    /**
     * 判断记录是否需要溢出存储
     */
    public static boolean needOverflow(Configuration conf, KeyValue keyValue) {
        if (keyValue == null) {
            return false;
        }

        byte[] keyBytes = ByteUtils.getBytesUTF8(keyValue.getKey());
        byte[] valueBytes = ByteUtils.getBytesUTF8(keyValue.getValue());

        // 计算记录大小
        int recordSize = Record.calculateRecordSize(keyBytes.length, valueBytes.length, false);

        // 获取可用空间
        int availableSpace = getAvailableDataSpace(conf);

        return recordSize > availableSpace;
    }

    /**
     * 计算溢出记录的磁盘大小
     * 溢出记录不存储完整value，只存储溢出标记和溢出页指针
     */
    public static int calculateOverflowRecordSize(KeyValue keyValue) {
        if (keyValue == null) {
            return 0;
        }
        byte[] keyBytes = ByteUtils.getBytesUTF8(keyValue.getKey());
        // 溢出记录：header + key + valueSize + overflow flag + overflow page
        return Record.calculateRecordSize(keyBytes.length, 0, true);
    }

    /**
     * 计算存储指定KeyValue需要的溢出页数量
     */
    public static int calculateOverflowPageCount(Configuration conf, KeyValue keyValue) {
        if (keyValue == null) {
            return 0;
        }

        byte[] valueBytes = ByteUtils.getBytesUTF8(keyValue.getValue());
        return OverFlowPage.calculateOverflowPageCount(conf, valueBytes.length);
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
        /** 根节点 */
        ROOT_NODE,
        /** 叶节点 */
        LEAF_NODE,
        /** 索引节点 */
        INDEX_NODE,
        /** 唯一根节点 */
        SPECIAL_NODE
    }
}
