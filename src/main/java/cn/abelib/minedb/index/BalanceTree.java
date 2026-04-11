package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.FreePageList;
import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import cn.abelib.minedb.index.fs.Record;
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

    /** 空闲页链表，管理溢出页空间 */
    private FreePageList freePageList;

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
        TreeNode parent = node.getParent();

        // 保存分裂前的信息用于调试
        int originalSize = node.getKeyValues().size();

        // 创建右节点
        TreeNode rightNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());

        // 右节点包含 [mid, size) 的键值对
        rightNode.setKeys(new ArrayList<>(node.getKeyValues().subList(mid, node.getKeyValues().size())));
        rightNode.setParent(parent);

        // 左节点（原节点）保留 [0, mid) 的键值对
        // 注意：必须创建新列表，因为 subList 是视图
        List<KeyValue> leftKeys = new ArrayList<>(node.getKeyValues().subList(0, mid));
        node.setKeys(leftKeys);

        // 确保节点属性正确
        node.setLeaf(true);
        rightNode.setLeaf(true);

        // 更新叶子节点链表指针
        rightNode.setNext(node.getNext());
        rightNode.setPrevious(node);
        if (node.getNext() != null) {
            node.getNext().setPrevious(rightNode);
        }
        node.setNext(rightNode);

        // 创建提升节点，包含分隔键和右节点
        TreeNode promoteNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
        // 分隔键是右节点的第一个键
        KeyValue separatorKey = new KeyValue(rightNode.getKeyValues().get(0));
        promoteNode.getKeyValues().add(separatorKey);
        promoteNode.getChildren().add(rightNode);

        // 处理提升
        insertIntoParent(parent, node, promoteNode);
    }

    /**
     * 将分裂后的新节点插入到父节点中
     */
    private void insertIntoParent(TreeNode parent, TreeNode leftNode, TreeNode promoteNode) throws IOException {
        if (parent == null) {
            // 没有父节点，创建新根
            TreeNode newRoot = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
            newRoot.setLeaf(false);
            newRoot.setRoot(true);

            // 添加键值
            newRoot.getKeyValues().add(new KeyValue(promoteNode.getKeyValues().get(0)));

            // 添加子节点：leftNode 和 promoteNode 的子节点（右节点）
            newRoot.getChildren().add(leftNode);
            newRoot.getChildren().add(promoteNode.getChildren().get(0));

            leftNode.setParent(newRoot);
            leftNode.setRoot(false);
            promoteNode.getChildren().get(0).setParent(newRoot);

            this.root = newRoot;
        } else {
            // 找到 leftNode 在父节点中的位置
            int leftIndex = -1;
            for (int i = 0; i < parent.getChildren().size(); i++) {
                if (parent.getChildren().get(i) == leftNode) {
                    leftIndex = i;
                    break;
                }
            }

            if (leftIndex < 0) {
                // leftNode 不在父节点的子节点列表中，这是一个错误情况
                // 可能是因为 leftNode 的键值已经被修改，无法通过引用匹配
                // 尝试通过键值范围找到正确的位置
                KeyValue leftFirstKey = leftNode.getKeyValues().isEmpty() ? null : leftNode.getKeyValues().get(0);
                if (leftFirstKey != null) {
                    for (int i = 0; i < parent.getChildren().size(); i++) {
                        TreeNode child = parent.getChildren().get(i);
                        if (!child.getKeyValues().isEmpty() &&
                            child.getKeyValues().get(0).compareTo(leftFirstKey) == 0) {
                            leftIndex = i;
                            break;
                        }
                    }
                }
                if (leftIndex < 0) {
                    // 仍然找不到，根据键值确定插入位置
                    leftIndex = PageUtils.binarySearchForIndex(promoteNode.getKeyValues().get(0), parent.getKeyValues());
                }
            }

            // 插入分隔键
            KeyValue separatorKey = new KeyValue(promoteNode.getKeyValues().get(0));
            if (leftIndex >= parent.getKeyValues().size()) {
                parent.getKeyValues().add(separatorKey);
            } else {
                parent.getKeyValues().add(leftIndex, separatorKey);
            }

            // 插入右节点到 leftNode 后面
            TreeNode rightNode = promoteNode.getChildren().get(0);
            rightNode.setParent(parent);
            if (leftIndex + 1 >= parent.getChildren().size()) {
                parent.getChildren().add(rightNode);
            } else {
                parent.getChildren().add(leftIndex + 1, rightNode);
            }

            parent.setDirty(true);

            // 检查父节点是否需要分裂
            if (PageUtils.isFull(parent)) {
                splitInternalNode(parent);
            }
        }
    }

    /**
     * 分裂内部节点（非叶子节点）
     */
    private void splitInternalNode(TreeNode node) throws IOException {
        int mid = PageUtils.midIndex(node);
        TreeNode parent = node.getParent();

        // 分隔键
        KeyValue separatorKey = node.getKeyValues().get(mid);

        // 创建右节点
        TreeNode rightNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
        rightNode.setLeaf(false);
        rightNode.setParent(parent);

        // 右节点包含键 [mid+1, size)
        rightNode.setKeys(new ArrayList<>(node.getKeyValues().subList(mid + 1, node.getKeyValues().size())));

        // 子节点分配：
        // B+ 树内部节点有 n 个键和 n+1 个子节点
        // 分裂后：左节点保留键 [0, mid-1] 和子节点 [0, mid]
        // 分隔键是 keys[mid]
        // 右节点保留键 [mid+1, n-1] 和子节点 [mid+1, n]

        // 右节点的子节点：[mid+1, children.size)
        List<TreeNode> rightChildren = new ArrayList<>(node.getChildren().subList(mid + 1, node.getChildren().size()));
        for (TreeNode child : rightChildren) {
            child.setParent(rightNode);
        }
        rightNode.setChildren(rightChildren);

        // 左节点保留的子节点：[0, mid]
        // 注意：不是 [0, mid+1)，而是保留前 mid+1 个子节点（索引 0 到 mid）
        List<TreeNode> leftChildren = new ArrayList<>(node.getChildren().subList(0, mid + 1));
        node.setChildren(leftChildren);

        // 左节点保留的键：[0, mid) - 不包含分隔键
        node.getKeyValues().subList(mid, node.getKeyValues().size()).clear();

        node.setDirty(true);

        // 创建提升节点
        TreeNode promoteNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
        promoteNode.getKeyValues().add(separatorKey);
        promoteNode.getChildren().add(rightNode);

        // 递归处理父节点
        insertIntoParent(parent, node, promoteNode);
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
                // 加载空闲页链表
                this.freePageList = loadFreePageList();
            } else {
                this.metaNode = new MetaNode(configuration);
                PageLoader.writeMeta(metaNode);
                this.root = new TreeNode(configuration, true, true, 0L);
                this.root.setPosition(metaNode.getRootPosition());
                // 创建新的空闲页链表
                this.freePageList = new FreePageList(configuration);
            }
            this.pageNo = new AtomicLong(this.metaNode.getNextPage());
        }
        if (Objects.isNull(this.root)) {
            this.root = new TreeNode(configuration, true, true, 0L);
        }
        if (Objects.isNull(this.freePageList)) {
            this.freePageList = new FreePageList(configuration);
        }
    }

    /**
     * 加载空闲页链表
     */
    private FreePageList loadFreePageList() {
        try {
            long freePageListPosition = configuration.getPageSize(); // 存储在元数据页之后
            return PageLoader.loadFreePageList(configuration, freePageListPosition);
        } catch (Exception e) {
            // 加载失败则创建新的
            return new FreePageList(configuration);
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

            // 保存空闲页链表
            if (freePageList != null) {
                freePageList.save();
            }
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

            // 获取被删除的记录，检查是否有溢出页
            KeyValue deletedKv = leaf.getKeyValues().get(index);
            Long overflowPagePosition = getOverflowPagePosition(leaf, index);

            // 删除键值对
            leaf.getKeyValues().remove(index);
            leaf.setDirty(true);

            // 释放溢出页空间
            if (overflowPagePosition != null && overflowPagePosition > 0) {
                freePageList.freeOverflowPageChain(overflowPagePosition);
            }

            // 注意：暂时禁用节点合并功能，因为合并逻辑需要更复杂的处理
            // 在实际生产环境中，应该在节点键值对数量低于阈值时触发合并
            // 这里保持简单，只删除不合并

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取记录的溢出页位置
     */
    private Long getOverflowPagePosition(TreeNode node, int kvIndex) {
        try {
            if (node.getPage() != null && node.getPage().getRecords() != null) {
                java.util.List<Record> records = node.getPage().getRecords();
                if (kvIndex >= 0 && kvIndex < records.size()) {
                    Record record = records.get(kvIndex);
                    if (record.isOverflow()) {
                        return record.getOverflowPage();
                    }
                }
            }
        } catch (Exception e) {
            // 忽略错误
        }
        return null;
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
        // 使用较小的阈值避免过早触发合并
        // 实际上，只有当节点完全为空或接近空时才需要合并
        return 1;
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
        if (nodeIndex < 0) {
            return; // 安全检查
        }

        if (node.isLeaf()) {
            // 叶子节点：直接移动最后一个键值对
            KeyValue borrowed = leftSibling.getKeyValues().remove(leftSibling.getKeyValues().size() - 1);
            node.getKeyValues().add(0, borrowed);

            // 更新父节点的分隔键
            if (nodeIndex - 1 >= 0 && nodeIndex - 1 < parent.getKeyValues().size()) {
                parent.getKeyValues().set(nodeIndex - 1, new KeyValue(node.getKeyValues().get(0)));
            }
        } else {
            // 内部节点：需要处理子节点
            if (nodeIndex - 1 >= 0 && nodeIndex - 1 < parent.getKeyValues().size()) {
                KeyValue parentKey = parent.getKeyValues().get(nodeIndex - 1);
                parent.getKeyValues().set(nodeIndex - 1, leftSibling.getKeyValues().remove(leftSibling.getKeyValues().size() - 1));
                node.getKeyValues().add(0, parentKey);
            }

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
        if (nodeIndex < 0) {
            return; // 安全检查
        }

        if (node.isLeaf()) {
            // 叶子节点：直接移动第一个键值对
            KeyValue borrowed = rightSibling.getKeyValues().remove(0);
            node.getKeyValues().add(borrowed);

            // 更新父节点的分隔键
            if (nodeIndex < parent.getKeyValues().size() && !rightSibling.getKeyValues().isEmpty()) {
                parent.getKeyValues().set(nodeIndex, new KeyValue(rightSibling.getKeyValues().get(0)));
            }
        } else {
            // 内部节点：需要处理子节点
            if (nodeIndex < parent.getKeyValues().size()) {
                KeyValue parentKey = parent.getKeyValues().get(nodeIndex);
                parent.getKeyValues().set(nodeIndex, rightSibling.getKeyValues().remove(0));
                node.getKeyValues().add(parentKey);
            }

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
        if (nodeIndex < 0) {
            return; // 安全检查
        }

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
            if (nodeIndex - 1 >= 0 && nodeIndex - 1 < parent.getKeyValues().size()) {
                KeyValue parentKey = parent.getKeyValues().get(nodeIndex - 1);
                leftSibling.getKeyValues().add(parentKey);
            }
            leftSibling.getKeyValues().addAll(node.getKeyValues());

            // 移动所有子节点
            for (TreeNode child : node.getChildren()) {
                child.setParent(leftSibling);
                leftSibling.getChildren().add(child);
            }
        }

        // 从父节点移除合并的键和子节点
        if (nodeIndex - 1 >= 0 && nodeIndex - 1 < parent.getKeyValues().size()) {
            parent.getKeyValues().remove(nodeIndex - 1);
        }
        if (nodeIndex < parent.getChildren().size()) {
            parent.getChildren().remove(nodeIndex);
        }
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
        if (nodeIndex < 0) {
            return; // 安全检查
        }

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
            if (nodeIndex < parent.getKeyValues().size()) {
                KeyValue parentKey = parent.getKeyValues().get(nodeIndex);
                node.getKeyValues().add(parentKey);
            }
            node.getKeyValues().addAll(rightSibling.getKeyValues());

            // 移动所有子节点
            for (TreeNode child : rightSibling.getChildren()) {
                child.setParent(node);
                node.getChildren().add(child);
            }
        }

        // 从父节点移除合并的键和子节点
        if (nodeIndex < parent.getKeyValues().size()) {
            parent.getKeyValues().remove(nodeIndex);
        }
        if (nodeIndex + 1 < parent.getChildren().size()) {
            parent.getChildren().remove(nodeIndex + 1);
        }
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

    /**
     * 获取空闲页链表
     */
    public FreePageList getFreePageList() {
        return freePageList;
    }

    /**
     * 分配溢出页位置
     */
    public long allocateOverflowPage() {
        lock.writeLock().lock();
        try {
            return freePageList.allocatePage();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
