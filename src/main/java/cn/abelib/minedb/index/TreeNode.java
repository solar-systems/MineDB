package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import cn.abelib.minedb.utils.KeyValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author: abel.huang
 * @Date: 2020-10-25 00:01
 * 内存中的节点
 */
public class TreeNode {
    /**
     * 当前结点的地址
     */
    private long position;
    /**
     * 是否为叶子节点
     */
    private boolean isLeaf;
    /**
     * 是否为根节点
     */
    private boolean isRoot;
    /**
     * 叶节点号
     */
    private long pageNo;
    /**
     * 父节点
     */
    private TreeNode parent;
    /**
     * 前一个节点
     */
    private TreeNode previous;
    /**
     * 后一个节点
     */
    private TreeNode next;
    /**
     * 节点的键情况
     */
    private List<KeyValue> keyValues;
    /**
     * 该页当前记录数量
     */
    private int entrySize;
    /**
     * 子节点
     */
    private List<TreeNode> children;
    /**
     * 该叶节点是否已经满
     */
    private boolean isFull;
    /**
     * 该节点是否已经删除
     */
    private boolean isDeleted;
    /**
     * 当前页是否为脏页(即当前页进行过修改，与磁盘页产生区别)
     */
    private boolean isDirty;
    /**
     * 对应磁盘的页
     */
    private Page page;
    /**
     * 配置文件
     */
    private Configuration conf;

    private Path path;

    public TreeNode(Configuration conf, boolean isLeaf, boolean isRoot, long pageNo) {
        this.conf = conf;
        this.path = Paths.get(conf.getDbName());
        this.isLeaf = isLeaf;
        this.isRoot = isRoot;
        this.pageNo = pageNo;
        this.parent = null;
        this.next = null;
        this.previous = null;
        this.keyValues = new ArrayList<>();
        this.entrySize = 0;
        this.children = new ArrayList<>();
        this.isFull = false;
        this.isDeleted = false;
        this.page = new Page(this);
    }

    public TreeNode() {
        this.conf = new Configuration();

    }

    public boolean isDirty() {
        return isDirty;
    }

    /**
     * 改变改状态会被加入到全局页缓存中
     * @param dirty
     */
    public void setDirty(boolean dirty) {
        isDirty = dirty;
        if (dirty) {
            GlobalPageCache.putDirtyPage(this);
        } else {
            GlobalPageCache.putCleanPage(this);
        }
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public List<KeyValue> getKeyValues() {
        return this.keyValues;
    }

    public void setKeys(List<KeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    public long getPageNo() {
        return pageNo;
    }

    public void setPageNo(long pageNo) {
        this.pageNo = pageNo;
    }

    /**
     * 获得当前结点的父节点
     * @return
     */
    public TreeNode getParent() {
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public TreeNode getPrevious() {
        if (Objects.isNull(previous)) {
            setPrevious(PageLoader.loadTreeNode(conf, this.page.getPrevious()));
        }
        return previous;
    }

    public void setPrevious(TreeNode previous) {
        this.previous = previous;
    }

    public TreeNode getNext() {
        if (Objects.isNull(next)) {
            setNext(PageLoader.loadTreeNode(conf, this.page.getNext()));
        }
        return next;
    }

    public void setNext(TreeNode next) {
        this.next = next;
    }

    /**
     * 从磁盘加载子节点
     * @return
     */
    public List<TreeNode> getChildren() {
        List<Long> childrenPointers = this.getPage().getChildren();
        setChildren(PageLoader.loadTreeNodes(conf, childrenPointers));
        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }

    /**
     * todo, 需要判断该节点磁盘空间是否已经满了
     * 判断是否需要进行分裂
     * @return
     */
    public boolean isFull() {
        return isFull;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public Configuration getConfiguration() {
        return conf;
    }
}
