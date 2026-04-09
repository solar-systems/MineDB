package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import cn.abelib.minedb.utils.KeyValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 内存中的节点
 *
 * @author abel.huang
 * @version 1.0
 * @date 2020-10-25 00:50
 */
public class TreeNode {
    private long position;
    private boolean isLeaf;
    private boolean isRoot;
    private long pageNo;
    private TreeNode parent;
    private TreeNode previous;
    private TreeNode next;
    private List<KeyValue> keyValues;
    private List<TreeNode> children;
    private boolean isDeleted;
    private boolean isDirty;
    private int total;
    private Page page;
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

    public boolean isDirty() { return isDirty; }

    public void setDirty(boolean dirty) {
        isDirty = dirty;
        if (dirty) {
            GlobalPageCache.putDirtyPage(this);
        } else {
            GlobalPageCache.putCleanPage(this);
        }
    }

    public void addTotal(int addition) { this.total += addition; }
    public void setTotal(int total) { this.total = total; }
    public int getTotal() { return this.total; }

    public ByteBuffer byteBuffer() { return page.byteBuffer(); }

    public boolean isLeaf() { return isLeaf; }
    public void setLeaf(boolean leaf) { isLeaf = leaf; }

    public boolean isRoot() { return isRoot; }
    public void setRoot(boolean root) { isRoot = root; }

    public List<KeyValue> getKeyValues() { return this.keyValues; }
    public void setKeys(List<KeyValue> keyValues) { this.keyValues = keyValues; }

    public long getPageNo() { return pageNo; }
    public void setPageNo(long pageNo) { this.pageNo = pageNo; }

    public TreeNode getParent() throws IOException {
        if (readFlag) {
            long parentPos = this.getPage().getParent();
            if (parentPos > 0) {
                setParent(PageLoader.loadTreeNode(conf, parentPos));
            }
        }
        return parent;
    }

    public void setParent(TreeNode parent) { this.parent = parent; }

    public TreeNode getPrevious() throws IOException {
        if (readFlag) {
            long previousPos = this.getPage().getPrevious();
            if (previousPos > 0) {
                setPrevious(PageLoader.loadTreeNode(conf, previousPos));
            }
        }
        return previous;
    }

    public void setPrevious(TreeNode previous) { this.previous = previous; }

    public TreeNode getNext() throws IOException {
        if (readFlag) {
            long nextPos = this.getPage().getNext();
            if (nextPos > 0) {
                setNext(PageLoader.loadTreeNode(conf, nextPos));
            }
        }
        return next;
    }

    public void setNext(TreeNode next) { this.next = next; }

    public List<TreeNode> getChildren() throws IOException {
        if (readFlag) {
            List<Long> childrenPointers = this.getPage().getChildren();
            setChildren(PageLoader.loadTreeNodes(conf, childrenPointers));
        }
        return children;
    }

    public void setChildren(List<TreeNode> children) { this.children = children; }

    public boolean isDeleted() { return isDeleted; }
    public void setDeleted(boolean deleted) { isDeleted = deleted; }

    public Page getPage() { return page; }
    public void setPage(Page page) { this.page = page; }

    public long getPosition() { return position; }
    public void setPosition(long position) { this.position = position; }

    public Configuration getConfiguration() { return conf; }
    public void setConfiguration(Configuration conf) { this.conf = conf; }

    public boolean isReadFlag() { return readFlag; }
    public void setReadFlag(boolean readFlag) { this.readFlag = readFlag; }
}
