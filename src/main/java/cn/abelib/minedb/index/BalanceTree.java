package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.PageLoader;
import cn.abelib.minedb.utils.KeyValue;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: abel.huang
 * @Date: 2020-10-25 01:13
 */
public class BalanceTree {
    /**
     * 根节点
     */
    private MetaNode metaNode;
    private TreeNode root;
    /**
     * 配置文件
     */
    private Configuration configuration;

    private AtomicLong pageNo;

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
        KeyValue entry = new KeyValue(key, value);
        // 当前仅有根节点
        if (getRoot().getChildren().isEmpty()) {
            insert(entry, getRoot());
        }
        // 当前根节点已有子节点
        else {
            TreeNode curr = getRoot();
            while (!curr.getChildren().isEmpty()) {
                curr = curr.getChildren().get(PageUtils.binarySearchForIndex(entry, curr.getKeyValues()));
            }
            insert(entry, curr);
            if (curr.isFull()) {
                split(curr);
            }
        }
    }

    /**
     * 节点分裂, 在磁盘B+树中只有磁盘页使用达到上限时才会
     * @param node
     */
    private void split(TreeNode node) {
        int mid = PageUtils.midIndex(node);
        TreeNode middleNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
        TreeNode rightNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());

        // 分裂后的右边节点
        rightNode.setKeys(new ArrayList<>(node.getKeyValues().subList(mid, node.getKeyValues().size())));
        rightNode.setParent(middleNode);

        // 分裂后的中间节点
        middleNode.getKeyValues().add(new KeyValue(node.getKeyValues().get(mid)));
        middleNode.getChildren().add(rightNode);

        // 分裂之前的原始节点, 即分裂后左边的部分
        node.setKeys(new ArrayList<>(node.getKeyValues().subList(0, mid)));

        split(node.getParent(), node, middleNode, true);
    }

    /**
     *
     * @param curr
     * @param prev
     * @param insertingNode
     * @param first
     */
    private void split(TreeNode curr, TreeNode prev, TreeNode insertingNode, boolean first) {
        if (curr == null) {
            this.root = insertingNode;
            int indexForPrev = PageUtils.binarySearchForIndex (prev.getKeyValues().get(0), insertingNode.getKeyValues());
            // 设置 insertingNode 父子节点关系
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
        }
        else {
            promote(insertingNode, curr);
            // 如果合并后的节点已经满了
            if (curr.isFull()) {
                int mid = PageUtils.midIndex(curr);
                TreeNode middleNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());
                TreeNode rightNode = new TreeNode(this.configuration, true, false, pageNo.getAndIncrement());

                // 分裂后的右边节点
                rightNode.setKeys(new ArrayList<>((curr.getKeyValues().subList(mid + 1, curr.getKeyValues().size()))));
                rightNode.setParent(middleNode);

                // 分裂后的中间节点
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

    /**
     * 合并内部节点, 符合条件的数据节点晋升为索引节点
     * @param mergeFrom
     * @param mergeInto
     */
    private void promote(TreeNode mergeFrom, TreeNode mergeInto) {
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

        //
        if (!mergeInto.getChildren().isEmpty() && mergeInto.getChildren().get(0).getChildren().isEmpty()) {
            if (mergeInto.getChildren().size() - 1 != childIndex && mergeInto.getChildren().get(childIndex + 1).getPrevious() == null) {
                mergeInto.getChildren().get(childIndex + 1).setPrevious(mergeInto.getChildren().get(childIndex));
                mergeInto.getChildren().get(childIndex).setNext(mergeInto.getChildren().get(childIndex + 1));
            }

            else if (childIndex != 0 && mergeInto.getChildren().get(childIndex - 1).getNext() == null) {
                mergeInto.getChildren().get(childIndex).setPrevious(mergeInto.getChildren().get(childIndex - 1));
                mergeInto.getChildren().get(childIndex - 1).setNext(mergeInto.getChildren().get(childIndex));
            }

            else {
                mergeInto.getChildren().get(childIndex).setNext(mergeInto.getChildren().get(childIndex - 1).getNext());
                mergeInto.getChildren().get(childIndex).getNext().setPrevious(mergeInto.getChildren().get(childIndex));

                mergeInto.getChildren().get(childIndex - 1).setNext(mergeInto.getChildren().get(childIndex));
                mergeInto.getChildren().get(childIndex).setPrevious(mergeInto.getChildren().get(childIndex - 1));
            }
        }
    }

    /**
     *  todo 目前先不考虑一个key对应多个value的场景
     * insert root
     * @param entry
     * @param node
     */
    private void insert(KeyValue entry, TreeNode node) {
        int index = PageUtils.binarySearchForIndex(entry, root.getKeyValues());
        if (index != 0 && node.getKeyValues().get(index - 1).getKey().equals(entry.getKey())) {
            node.getKeyValues().get(index - 1).setValue((entry.getValue()));
        } else {
            node.getKeyValues().add(index, entry);
        }
        GlobalPageCache.putDirtyPage(node);
    }

    /**
     * 初始化
     */
    private void init() throws IOException {
        if (Objects.isNull(this.metaNode)) {
            if (PageLoader.existsMeta(configuration.getPath())) {
                this.metaNode = PageLoader.readMeta(configuration);
            } else {
                this.metaNode = new MetaNode(configuration);
                PageLoader.writeMeta(metaNode);
            }
            this.pageNo = new AtomicLong(this.metaNode.getNextPage());
        }
        if (Objects.isNull(this.root)) {
            this.root = new TreeNode(configuration, true, true, 0L);
        }
    }

    public MetaNode getMetaNode() {
        return metaNode;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public TreeNode getRoot() {
        return this.root;
    }
}
