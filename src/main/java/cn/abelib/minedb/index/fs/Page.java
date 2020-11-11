package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.TreeNode;
import cn.abelib.minedb.utils.KeyValue;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * @Author: abel.huang
 * @Date: 2020-10-24 00:55
 * 磁盘中的页，与内存中的树节点对应
 *  todo
 */
public class Page {
    /**
     * 当前结点指针,
     * 页内地址都是基于当前地址的偏移量
     */
    private long position;
    /**
     * 页大小
     */
    private int pageSize;
    /**
     * 父节点指针
     */
    private long parent;
    /**
     * 链表前驱指针
     */
    private long previous;
    /**
     * 链表后继指针
     */
    private long next;
    /**
     * 每个页头部大小，
     * 会有部分空缺, 由配置文件决定
     */
    private int headerSize;
    /**
     * 实际范围是[headerSize, headerSize + childrenSize]
     * 子节点指针域大小，每个指针都是8个字节
     * 可以支持的最大孩子数 = childrenSize / 8
     */
    private int childrenSize;
    /**
     * 当前页编号
     */
    private long pageNo;
    /**
     * 空闲空间位置
     */
    private int free;
    /**
     * 子节点数量
     */
    private int childrenNum;
    /**
     * 子节点
     */
    private List<Long> children;
    /**
     * 是否是根节点
     */
    private boolean isRoot;
    /**
     * 是否是叶节点
     */
    private boolean isLeaf;
    /**
     * 节点是否已经满了
     */
    private boolean isFull;
    /**
     * 具体的记录值
     */
    private List<Record> records;
    private TreeNode node;
    private Configuration conf;

    /**
     * 基于TreeNode
     * @param node
     */
    public Page(TreeNode node) {
        this.node = node;
        init();
    }

    /**
     * 读取磁盘中的一个页
     * @param buffer
     */
    public Page(ByteBuffer buffer) {
        this.children = Lists.newArrayList();
        buffer.flip();
        this.position = buffer.getLong();
        this.pageSize = buffer.getInt();
        this.parent = buffer.getLong();
        this.previous = buffer.getLong();
        this.next = buffer.getLong();
        this.headerSize = buffer.getInt();
        this.childrenSize = buffer.getInt();
        this.pageNo = buffer.getLong();
        this.free = buffer.getInt();
        this.childrenNum = buffer.getInt();
        this.isRoot = buffer.get() == 1;
        this.isLeaf = buffer.get() == 1;
        this.isFull = buffer.get() == 1;
        buffer.position(this.headerSize);
        for (int i = 0; i < childrenNum; i++) {
            children.add(buffer.getLong());
        }

        buffer.position(this.childrenSize);
        // todo 读出record的值

        this.node = new TreeNode();
        this.node.setPosition(position);
        this.node.setLeaf(isLeaf);
        this.node.setRoot(isRoot);
        this.node.setFull(isFull);
        this.node.setPageNo(pageNo);
    }

    private void init() {
        this.conf = this.node.getConfiguration();
        // 当前开始位置
        this.position = this.node.getPosition();
        // 父节点位置
        this.parent = getNodePosition(this.node.getParent());
        this.previous = getNodePosition(this.node.getPrevious());
        this.next = getNodePosition(this.node.getNext());
        this.headerSize = this.conf.getHeaderSize();
        this.childrenSize = this.conf.getChildrenSize();
        this.pageNo = this.node.getPageNo();
        recordsProperties(this.node.getKeyValues());
    }

    private long getNodePosition(TreeNode node) {
        if (Objects.isNull(node)) {
            return 0L;
        }
        return node.getPosition();
    }

    /**
     * 将keyValues转为List<Record>以及其他相关的属性,
     * 默认情况下所有结点内部数据从小到大排列
     * @param keyValues
     * @return
     */
    private void recordsProperties(List<KeyValue> keyValues) {
        this.records = Lists.newArrayList();
        if (Objects.isNull(keyValues) || keyValues.size() == 0) {
            return;
        }
        int recordPosition = this.headerSize;
        for (KeyValue keyValue : keyValues) {
            Record record = new Record(keyValue, recordPosition);
            // 下一个节点应该所处的位置
            recordPosition += record.getTotalLen();
            this.records.add(record);
        }
        // 剩余可用的位置
        this.free = recordPosition;
    }

    /**
     * todo
     * @return
     */
    public ByteBuffer byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
        buffer.putLong(this.position);
        buffer.putInt(this.pageSize);
        buffer.putLong(this.parent);
        buffer.putLong(this.previous);
        buffer.putLong(this.next);
        buffer.putInt(this.headerSize);
        buffer.putInt(this.childrenSize);
        buffer.putLong(this.pageNo);
        buffer.putInt(this.free);
        buffer.putInt(this.childrenNum);
        buffer.put((byte) (this.isRoot ? 1 : 0));
        buffer.put((byte) (this.isLeaf ? 1 : 0));
        buffer.put((byte) (this.isFull ? 1 : 0));
        buffer.position(this.headerSize);
        // todo children指针

        // todo 值
        return buffer;
    }

    public TreeNode getNode() {
        return node;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public List<Long> getChildren() {
        return this.children;
    }

    public long getParent() {
        return parent;
    }

    public void setParent(long parent) {
        this.parent = parent;
    }

    public long getPrevious() {
        return previous;
    }

    public void setPrevious(long previous) {
        this.previous = previous;
    }

    public long getNext() {
        return next;
    }

    public void setNext(long next) {
        this.next = next;
    }

    public boolean isFull() {
        if (free >= pageSize) {
            isFull = true;
        } else {
            isFull = false;
        }
        return isFull;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }
}
