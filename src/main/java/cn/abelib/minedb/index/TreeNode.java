package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.OverFlowPage;
import cn.abelib.minedb.index.fs.Page;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
     * 是否为溢出节点
     */
    private boolean isOverFlow;
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
    private List<String> keys;
    /**
     * 节点的记录值, 目前会采用定长的方式
     */
    private List<String> entries;
    /**
     * 改页当前记录数量
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
     * 对应磁盘的页
     */
    private Page page;
    /**
     * 当前节点对应的溢出页
     */
    private List<OverFlowPage> flowPages;
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
        this.entries = new ArrayList<>();
        this.entrySize = 0;
        this.children = new ArrayList<>();
        this.isFull = false;
        this.isDeleted = false;
        this.page = new Page(path, this);
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

    public boolean isOverFlow() {
        return isOverFlow;
    }

    public void setOverFlow(boolean overFlow) {
        isOverFlow = overFlow;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public long getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public TreeNode getParent() {
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public TreeNode getPrevious() {
        return previous;
    }

    public void setPrevious(TreeNode previous) {
        this.previous = previous;
    }

    public TreeNode getNext() {
        return next;
    }

    public void setNext(TreeNode next) {
        this.next = next;
    }

    public List<String> getEntries() {
        return entries;
    }

    public void setEntries(List<String> entries) {
        this.entries = entries;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }

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

    public int getEntrySize() {
        return entries.size();
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
