package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import cn.abelib.minedb.utils.KeyValue;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * B+树索引
 *
 * @author abel.huang
 * @version 1.0
 * @date 2020-10-29 00:33
 */
public class BalanceTree {
    private MetaNode metaNode;
    private TreeNode root;
    private Configuration configuration;
    private AtomicLong pageNo;

    /** 读写锁，支持并发读，写操作互斥 */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** 最小填充因子，节点键值对数量少于此比例时需要合并 */
    private static final double MIN_FILL_FACTOR = 0.25;

    public BalanceTree() {
        this.configuration = new Configuration();
    }

    public BalanceTree(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.init();
    }

    public void insert(String key, String value) throws Exception {
        if (Objects.isNull(metaNode) || Objects.isNull(root)) {
            throw new IllegalArgumentException("Invalid meta data");
        } else if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("Invalid blank key/value");
        }

        lock.writeLock().lock();
        try {
            KeyValue entry = new KeyValue(key, value);
            if (getRootInternal().getChildren().isEmpty() && !PageUtils.isFull(getRootInternal())) {
                insert(entry, getRootInternal());
            } else {
                TreeNode curr = getRootInternal();
                while (!curr.getChildren().isEmpty()) {
                    curr = curr.getChildren().get(PageUtils.binarySearchForIndex(entry, curr.getKeyValues()));
                }
                insert(entry, curr);
                if (PageUtils.isFull(curr)) {
                    split(curr);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void split(TreeNode node) throws IOException {
        int mid = PageUtils.midIndex(node);
        TreeNode middleNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
        TreeNode rightNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());

        rightNode.setKeys(new ArrayList<>(node.getKeyValues().subList(mid, node.getKeyValues().size())));
        rightNode.setParent(middleNode);

        middleNode.getKeyValues().add(new KeyValue(node.getKeyValues().get(mid)));
        middleNode.getChildren().add(rightNode);

        node.setKeys(new ArrayList<>(node.getKeyValues().subList(0, mid)));

        split(node.getParent(), node, middleNode, true);
    }

    private void split(TreeNode curr, TreeNode prev, TreeNode insertingNode, boolean first) throws IOException {
        if (curr == null) {
            this.root = insertingNode;
            int indexForPrev = PageUtils.binarySearchForIndex(prev.getKeyValues().get(0), insertingNode.getKeyValues());
            prev.setParent(insertingNode);
            insertingNode.getChildren().add(indexForPrev, prev);
            if (first) {
                if (indexForPrev == 0) {
                    insertingNode.getChildren().get(0).setNext(insertingNode.getChildren().get(1));
                    insertingNode.getChildren().get(1).setPrevious(insertingNode.getChildren().get(0));
                } else {
                    insertingNode.getChildren().get(indexForPrev - 1).setNext(insertingNode.getChildren().get(indexForPrev));
                    insertingNode.getChildren().get(indexForPrev + 1).setPrevious(insertingNode.getChildren().get(indexForPrev));
                }
            }
        } else {
            promote(insertingNode, curr);
            if (PageUtils.isFull(curr)) {
                int mid = PageUtils.midIndex(curr);
                TreeNode middleNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
                TreeNode rightNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());

                rightNode.setKeys(new ArrayList<>((curr.getKeyValues().subList(mid + 1, curr.getKeyValues().size()))));
                rightNode.setParent(middleNode);

                middleNode.getKeyValues().add(curr.getKeyValues().get(mid));
                middleNode.getChildren().add(rightNode);

                List<TreeNode> childrenOfCurr = curr.getChildren();
                List<TreeNode> childrenOfRight = new ArrayList<>();
                int lastChildOfLeft = childrenOfCurr.size() - 1;

                for (int i = childrenOfCurr.size() - 1; i >= 0; i--) {
                    List<KeyValue> keyValues = childrenOfCurr.get(i).getKeyValues();
                    if (middleNode.getKeyValues().get(0).compareTo(keyValues.get(0)) <= 0) {
                        childrenOfCurr.get(i).setParent(rightNode);
                        childrenOfRight.add(0, childrenOfCurr.get(i));
                        lastChildOfLeft--;
                    } else {
                        break;
                    }
                }
                rightNode.setChildren(childrenOfRight);

                curr.getChildren().subList(lastChildOfLeft + 1, childrenOfCurr.size()).clear();
                curr.getKeyValues().subList(mid, curr.getKeyValues().size()).clear();

                split(curr.getParent(), curr, middleNode, false);
            }
        }
    }

    private void promote(TreeNode mergeFrom, TreeNode mergeInto) throws IOException {
        KeyValue keyValue = mergeFrom.getKeyValues().get(0);
        TreeNode childNode = mergeFrom.getChildren().get(0);

        int index = PageUtils.binarySearchForIndex(keyValue, mergeInto.getKeyValues());
        int childIndex = index;
        if (keyValue.compareTo(childNode.getKeyValues().get(0)) <= 0) {
            childIndex = index + 1;
        }

        childNode.setParent(mergeInto);
        mergeInto.getChildren().add(childIndex, childNode);
        mergeInto.getKeyValues().add(index, keyValue);

        if (!mergeInto.getChildren().isEmpty() && mergeInto.getChildren().get(0).getChildren().isEmpty()) {
            if (mergeInto.getChildren().size() - 1 != childIndex && mergeInto.getChildren().get(childIndex + 1).getPrevious() == null) {
                mergeInto.getChildren().get(childIndex + 1).setPrevious(mergeInto.getChildren().get(childIndex));
                mergeInto.getChildren().get(childIndex).setNext(mergeInto.getChildren().get(childIndex + 1));
            } else if (childIndex != 0 && mergeInto.getChildren().get(childIndex - 1).getNext() == null) {
                mergeInto.getChildren().get(childIndex).setPrevious(mergeInto.getChildren().get(childIndex - 1));
                mergeInto.getChildren().get(childIndex - 1).setNext(mergeInto.getChildren().get(childIndex));
            } else {
                mergeInto.getChildren().get(childIndex).setNext(mergeInto.getChildren().get(childIndex - 1).getNext());
                mergeInto.getChildren().get(childIndex).getNext().setPrevious(mergeInto.getChildren().get(childIndex));
                mergeInto.getChildren().get(childIndex - 1).setNext(mergeInto.getChildren().get(childIndex));
                mergeInto.getChildren().get(childIndex).setPrevious(mergeInto.getChildren().get(childIndex - 1));
            }
        }
    }

    private void insert(KeyValue entry, TreeNode node) {
        int index = PageUtils.binarySearchForIndex(entry, getRootInternal().getKeyValues());
        if (index != 0 && node.getKeyValues().get(index - 1).getKey().equals(entry.getKey())) {
            node.getKeyValues().get(index - 1).setValue(entry.getValue());
        } else {
            node.getKeyValues().add(index, entry);
        }
        node.setDirty(true);
    }

    private void init() throws IOException {
        if (Objects.isNull(this.metaNode)) {
            if (PageLoader.existsMeta(configuration.getPath())) {
                this.metaNode = PageLoader.readMeta(configuration);
                this.root = loadRootFromDisk();
            } else {
                this.metaNode = new MetaNode(configuration);
                PageLoader.writeMeta(metaNode);
                this.root = new TreeNode(configuration, true, true, 0L);
                this.root.setPosition(metaNode.getRootPosition());
            }
            this.pageNo = new AtomicLong(this.metaNode.getNextPage());
        }
        if (Objects.isNull(this.root)) {
            this.root = new TreeNode(configuration, true, true, 0L);
        }
    }

    private TreeNode loadRootFromDisk() throws IOException {
        long rootPos = metaNode.getRootPosition();
        long fileSize = configuration.getPath().toFile().length();

        if (rootPos <= 0 || fileSize < rootPos + 100) {
            TreeNode newRoot = new TreeNode(configuration, true, true, 0L);
            newRoot.setPosition(metaNode.getRootPosition());
            return newRoot;
        }

        try {
            Page rootPage = PageLoader.loadPage(configuration, rootPos);
            TreeNode root = rootPage.getNode();
            root.setConfiguration(configuration);
            root.setPage(rootPage);
            return root;
        } catch (Exception e) {
            TreeNode newRoot = new TreeNode(configuration, true, true, 0L);
            newRoot.setPosition(metaNode.getRootPosition());
            return newRoot;
        }
    }

    public void flush(long entryCount) throws IOException {
        lock.writeLock().lock();
        try {
            metaNode.setEntryCount(entryCount);
            metaNode.setNextPage(pageNo.get());

            if (root != null) {
                if (root.getPosition() <= 0) {
                    root.setPosition(metaNode.getRootPosition());
                }
                metaNode.setRootPosition(root.getPosition());
                root.setDirty(true);
            }

            GlobalPageCache.getInstance().flushDirtyPages();
            PageLoader.updateMeta(metaNode);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ==================== 查询操作 ====================

    public String get(String key) throws IOException {
        if (StringUtils.isBlank(key) || root == null) {
            return null;
        }

        lock.readLock().lock();
        try {
            if (getRootInternal().getKeyValues().isEmpty()) {
                return null;
            }

            KeyValue entry = new KeyValue(key);
            TreeNode leaf = findLeafNode(entry);

            if (leaf == null) {
                return null;
            }

            int index = PageUtils.binarySearch(entry, leaf.getKeyValues());
            if (index >= 0) {
                return leaf.getKeyValues().get(index).getValue();
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean contains(String key) throws IOException {
        return get(key) != null;
    }

    public List<KeyValue> range(String start, String end) throws IOException {
        List<KeyValue> result = new ArrayList<>();
        if (StringUtils.isBlank(start) || StringUtils.isBlank(end) || root == null) {
            return result;
        }

        lock.readLock().lock();
        try {
            if (getRootInternal().getKeyValues().isEmpty()) {
                return result;
            }

            KeyValue startEntry = new KeyValue(start);
            KeyValue endEntry = new KeyValue(end);

            if (startEntry.compareTo(endEntry) >= 0) {
                return result;
            }

            TreeNode leaf = findLeafNode(startEntry);

            while (leaf != null) {
                for (KeyValue kv : leaf.getKeyValues()) {
                    if (kv.compareTo(endEntry) >= 0) {
                        return result;
                    }
                    if (kv.compareTo(startEntry) >= 0) {
                        result.add(kv);
                    }
                }
                leaf = leaf.getNext();
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<KeyValue> prefix(String prefix) throws IOException {
        List<KeyValue> result = new ArrayList<>();
        if (StringUtils.isBlank(prefix) || root == null) {
            return result;
        }

        lock.readLock().lock();
        try {
            if (getRootInternal().getKeyValues().isEmpty()) {
                return result;
            }

            KeyValue prefixEntry = new KeyValue(prefix);
            TreeNode leaf = findLeafNode(prefixEntry);

            while (leaf != null) {
                for (KeyValue kv : leaf.getKeyValues()) {
                    String key = kv.getKey();
                    if (key.compareTo(prefix) < 0) {
                        continue;
                    }
                    if (!key.startsWith(prefix)) {
                        return result;
                    }
                    result.add(kv);
                }
                leaf = leaf.getNext();
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private TreeNode findLeafNode(KeyValue entry) throws IOException {
        TreeNode node = getRootInternal();
        if (node == null) {
            return null;
        }
        while (node != null && !node.getChildren().isEmpty()) {
            int childIndex = PageUtils.binarySearchForIndex(entry, node.getKeyValues());
            node = node.getChildren().get(childIndex);
        }
        return node;
    }

    // ==================== 删除操作（带节点合并） ====================

    public boolean delete(String key) throws IOException {
        if (StringUtils.isBlank(key) || root == null) {
            return false;
        }

        lock.writeLock().lock();
        try {
            if (getRootInternal().getKeyValues().isEmpty()) {
                return false;
            }

            KeyValue entry = new KeyValue(key);
            TreeNode leaf = findLeafNode(entry);

            if (leaf == null) {
                return false;
            }

            int index = PageUtils.binarySearch(entry, leaf.getKeyValues());
            if (index < 0) {
                return false;
            }

            // 删除键值对
            leaf.getKeyValues().remove(index);
            leaf.setDirty(true);

            // 检查是否需要再平衡
            if (!leaf.isRoot()) {
                rebalanceAfterDelete(leaf);
            }

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 删除后重新平衡树结构
     */
    private void rebalanceAfterDelete(TreeNode node) throws IOException {
        // 特殊节点（根节点同时是叶子节点）不需要平衡
        if (node.isRoot() && node.isLeaf()) {
            return;
        }

        // 计算最小键值对数量
        int minKeys = getMinKeys(node);

        // 如果节点键值对数量充足，不需要平衡
        if (node.getKeyValues().size() >= minKeys) {
            return;
        }

        TreeNode parent = node.getParent();
        if (parent == null) {
            // 如果是根节点且只有一个子节点，将子节点提升为根
            if (!node.isLeaf() && node.getChildren().size() == 1) {
                TreeNode newRoot = node.getChildren().get(0);
                newRoot.setRoot(true);
                newRoot.setParent(null);
                this.root = newRoot;
                newRoot.setDirty(true);
            }
            return;
        }

        // 尝试从兄弟节点借用或合并
        TreeNode leftSibling = getLeftSibling(node);
        TreeNode rightSibling = getRightSibling(node);

        if (leftSibling != null && leftSibling.getKeyValues().size() > minKeys) {
            // 从左兄弟借用
            borrowFromLeft(node, leftSibling, parent);
        } else if (rightSibling != null && rightSibling.getKeyValues().size() > minKeys) {
            // 从右兄弟借用
            borrowFromRight(node, rightSibling, parent);
        } else if (leftSibling != null) {
            // 与左兄弟合并
            mergeWithLeft(node, leftSibling, parent);
        } else if (rightSibling != null) {
            // 与右兄弟合并
            mergeWithRight(node, rightSibling, parent);
        }
    }

    /**
     * 获取节点最小键值对数量
     */
    private int getMinKeys(TreeNode node) {
        // 对于B+树，非根节点至少要有 ⌈m/2⌉ - 1 个键
        // 简化处理：使用最小填充因子
        int maxKeys = configuration.getPageSize() / 100; // 粗略估算
        return Math.max(1, (int) (maxKeys * MIN_FILL_FACTOR));
    }

    /**
     * 获取左兄弟节点
     */
    private TreeNode getLeftSibling(TreeNode node) throws IOException {
        TreeNode parent = node.getParent();
        if (parent == null) {
            return null;
        }

        int index = parent.getChildren().indexOf(node);
        if (index > 0) {
            return parent.getChildren().get(index - 1);
        }
        return null;
    }

    /**
     * 获取右兄弟节点
     */
    private TreeNode getRightSibling(TreeNode node) throws IOException {
        TreeNode parent = node.getParent();
        if (parent == null) {
            return null;
        }

        int index = parent.getChildren().indexOf(node);
        if (index >= 0 && index < parent.getChildren().size() - 1) {
            return parent.getChildren().get(index + 1);
        }
        return null;
    }

    /**
     * 从左兄弟借用一个键
     */
    private void borrowFromLeft(TreeNode node, TreeNode leftSibling, TreeNode parent) throws IOException {
        int nodeIndex = parent.getChildren().indexOf(node);

        if (node.isLeaf()) {
            // 叶子节点：直接移动最后一个键值对
            KeyValue borrowed = leftSibling.getKeyValues().remove(leftSibling.getKeyValues().size() - 1);
            node.getKeyValues().add(0, borrowed);

            // 更新父节点的分隔键
            parent.getKeyValues().set(nodeIndex - 1, new KeyValue(node.getKeyValues().get(0)));
        } else {
            // 内部节点：需要处理子节点
            KeyValue parentKey = parent.getKeyValues().get(nodeIndex - 1);
            parent.getKeyValues().set(nodeIndex - 1, leftSibling.getKeyValues().remove(leftSibling.getKeyValues().size() - 1));
            node.getKeyValues().add(0, parentKey);

            // 移动子节点
            if (!leftSibling.getChildren().isEmpty()) {
                TreeNode borrowedChild = leftSibling.getChildren().remove(leftSibling.getChildren().size() - 1);
                borrowedChild.setParent(node);
                node.getChildren().add(0, borrowedChild);
            }
        }

        node.setDirty(true);
        leftSibling.setDirty(true);
        parent.setDirty(true);
    }

    /**
     * 从右兄弟借用一个键
     */
    private void borrowFromRight(TreeNode node, TreeNode rightSibling, TreeNode parent) throws IOException {
        int nodeIndex = parent.getChildren().indexOf(node);

        if (node.isLeaf()) {
            // 叶子节点：直接移动第一个键值对
            KeyValue borrowed = rightSibling.getKeyValues().remove(0);
            node.getKeyValues().add(borrowed);

            // 更新父节点的分隔键
            parent.getKeyValues().set(nodeIndex, new KeyValue(rightSibling.getKeyValues().get(0)));
        } else {
            // 内部节点：需要处理子节点
            KeyValue parentKey = parent.getKeyValues().get(nodeIndex);
            parent.getKeyValues().set(nodeIndex, rightSibling.getKeyValues().remove(0));
            node.getKeyValues().add(parentKey);

            // 移动子节点
            if (!rightSibling.getChildren().isEmpty()) {
                TreeNode borrowedChild = rightSibling.getChildren().remove(0);
                borrowedChild.setParent(node);
                node.getChildren().add(borrowedChild);
            }
        }

        node.setDirty(true);
        rightSibling.setDirty(true);
        parent.setDirty(true);
    }

    /**
     * 与左兄弟合并
     */
    private void mergeWithLeft(TreeNode node, TreeNode leftSibling, TreeNode parent) throws IOException {
        int nodeIndex = parent.getChildren().indexOf(node);

        if (node.isLeaf()) {
            // 叶子节点合并：合并所有键值对
            leftSibling.getKeyValues().addAll(node.getKeyValues());

            // 更新链表指针
            leftSibling.setNext(node.getNext());
            if (node.getNext() != null) {
                node.getNext().setPrevious(leftSibling);
            }
        } else {
            // 内部节点合并：需要加入父节点的分隔键
            KeyValue parentKey = parent.getKeyValues().get(nodeIndex - 1);
            leftSibling.getKeyValues().add(parentKey);
            leftSibling.getKeyValues().addAll(node.getKeyValues());

            // 移动所有子节点
            for (TreeNode child : node.getChildren()) {
                child.setParent(leftSibling);
                leftSibling.getChildren().add(child);
            }
        }

        // 从父节点移除合并的键和子节点
        parent.getKeyValues().remove(nodeIndex - 1);
        parent.getChildren().remove(nodeIndex);
        node.setDeleted(true);

        leftSibling.setDirty(true);
        parent.setDirty(true);

        // 递归检查父节点是否需要平衡
        if (!parent.isRoot()) {
            rebalanceAfterDelete(parent);
        } else if (parent.getKeyValues().isEmpty()) {
            // 根节点为空，左兄弟成为新根
            leftSibling.setRoot(true);
            leftSibling.setParent(null);
            this.root = leftSibling;
            leftSibling.setDirty(true);
        }
    }

    /**
     * 与右兄弟合并
     */
    private void mergeWithRight(TreeNode node, TreeNode rightSibling, TreeNode parent) throws IOException {
        int nodeIndex = parent.getChildren().indexOf(node);

        if (node.isLeaf()) {
            // 叶子节点合并：合并所有键值对
            node.getKeyValues().addAll(rightSibling.getKeyValues());

            // 更新链表指针
            node.setNext(rightSibling.getNext());
            if (rightSibling.getNext() != null) {
                rightSibling.getNext().setPrevious(node);
            }
        } else {
            // 内部节点合并：需要加入父节点的分隔键
            KeyValue parentKey = parent.getKeyValues().get(nodeIndex);
            node.getKeyValues().add(parentKey);
            node.getKeyValues().addAll(rightSibling.getKeyValues());

            // 移动所有子节点
            for (TreeNode child : rightSibling.getChildren()) {
                child.setParent(node);
                node.getChildren().add(child);
            }
        }

        // 从父节点移除合并的键和子节点
        parent.getKeyValues().remove(nodeIndex);
        parent.getChildren().remove(nodeIndex + 1);
        rightSibling.setDeleted(true);

        node.setDirty(true);
        parent.setDirty(true);

        // 递归检查父节点是否需要平衡
        if (!parent.isRoot()) {
            rebalanceAfterDelete(parent);
        } else if (parent.getKeyValues().isEmpty()) {
            // 根节点为空，node成为新根
            node.setRoot(true);
            node.setParent(null);
            this.root = node;
            node.setDirty(true);
        }
    }

    /**
     * 获取内部根节点（不加锁）
     */
    private TreeNode getRootInternal() {
        return this.root;
    }

    public MetaNode getMetaNode() { return metaNode; }
    public Configuration getConfiguration() { return configuration; }

    /**
     * 获取根节点（线程安全）
     */
    public TreeNode getRoot() {
        lock.readLock().lock();
        try {
            return this.root;
        } finally {
            lock.readLock().unlock();
        }
    }
}
