package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import cn.abelib.minedb.utils.KeyValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: abel.huang
 * @Date: 2020-10-25 00:01
 * 内存中的节点，
 * 其中对于磁盘中的节点而言有四种类型：
 *  1. isRoot = true; isLeaf = true => 仅有一个节点，即根节点
 *  [headerSize, childrenSize] -> 指针域， [childrenSize, pageSize] -> 值域
 *  2. isRoot = true; isLeaf = false => 多个节点时候的根节点
 *  [headerSize, pageSize]-> 指针域
 *  3. isRoot = false; isLeaf = false => 多个节点时候的索引节点
 *  [headerSize, pageSize] -> 指针域
 *  4. isRoot = false; isLeaf = false => 多个节点时候的叶子节点
 *  [headerSize, pageSize] -> 值域
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
     * 子节点
     */
    private List<TreeNode> children;
    /**
     * 该节点是否已经删除
     */
    private boolean isDeleted;
    /**
     * 当前页是否为脏页(即当前页进行过修改，与磁盘页产生区别)
     */
    private boolean isDirty;
    /**
     * 当前总字节数
     */
    private int total;
    /**
     * 对应磁盘的页
     */
    private Page page;
    /**
     * 配置文件
     */
    private Configuration conf;

    private boolean readFlag;

    public TreeNode(Configuration conf, boolean isLeaf, boolean isRoot, long pageNo) throws IOException {
        this.conf = conf;
        this.isLeaf = isLeaf;
        this.isRoot = isRoot;
        this.pageNo = pageNo;
        this.parent = null;
        this.next = null;
        this.previous = null;
        this.keyValues = new ArrayList<>();
        this.children = new ArrayList<>();
        this.isDeleted = false;
        this.readFlag = false;
        this.total = 0;
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

    public void addTotal(int addition) {
        this.total += addition;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getTotal() {
        return this.total;
    }

    public ByteBuffer byteBuffer() {
        return page.byteBuffer();
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
    public TreeNode getParent() throws IOException {
        if (readFlag) {
            long parentPos = this.getPage().getParent();
            setPrevious(PageLoader.loadTreeNode(conf, parentPos));
        }
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public TreeNode getPrevious() throws IOException {
        if (readFlag) {
            long previousPos = this.getPage().getPrevious();
            setPrevious(PageLoader.loadTreeNode(conf, previousPos));
        }
        return previous;
    }

    public void setPrevious(TreeNode previous) {
        this.previous = previous;
    }

    public TreeNode getNext() throws IOException {
        if (readFlag) {
            long nextPos = this.getPage().getNext();
            setPrevious(PageLoader.loadTreeNode(conf, nextPos));
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
    public List<TreeNode> getChildren() throws IOException {
        if (readFlag) {
            List<Long> childrenPointers = this.getPage().getChildren();
            setChildren(PageLoader.loadTreeNodes(conf, childrenPointers));
        }

        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
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

    public boolean isReadFlag() {
        return readFlag;
    }

    public void setReadFlag(boolean readFlag) {
        this.readFlag = readFlag;
    }
}
