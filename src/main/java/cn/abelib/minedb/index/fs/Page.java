package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.TreeNode;
import cn.abelib.minedb.utils.KeyValue;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * 磁盘中的页，与内存中的树节点对应
 *
 * @Author: abel.huang
 * @Date: 2020-10-25 00:50
 */
public class Page {
    private long position;
    private long parent;
    private long previous;
    private long next;
    private long pageNo;
    private int free;
    private int childrenNum;
    private List<Long> children;
    private boolean isRoot;
    private boolean isLeaf;
    private int entrySize;
    private List<Record> records;
    private List<KeyValue> keyValues;
    private List<TreeNode> childrenNodes;
    private TreeNode node;
    private Configuration conf;

    public Page(TreeNode node) throws IOException {
        this.node = node;
        this.conf = this.node.getConfiguration();
        this.position = this.node.getPosition();
        this.parent = getNodePosition(this.node.getParent());
        this.previous = getNodePosition(this.node.getPrevious());
        this.next = getNodePosition(this.node.getNext());
        this.pageNo = this.node.getPageNo();
        this.isRoot = this.node.isRoot();
        this.isLeaf = this.node.isLeaf();
        this.keyValues = this.node.getKeyValues();
        this.childrenNodes = this.node.getChildren();
        recordsProperties();
        childrenProperties();
    }

    public Page(Configuration conf, ByteBuffer buffer) {
        this.children = Lists.newArrayList();
        this.records = Lists.newArrayList();
        buffer.flip();
        this.position = buffer.getLong();
        this.parent = buffer.getLong();
        this.previous = buffer.getLong();
        this.next = buffer.getLong();
        this.pageNo = buffer.getLong();
        this.free = buffer.getInt();
        this.childrenNum = buffer.getInt();
        this.entrySize = buffer.getInt();
        this.isRoot = buffer.get() == 1;
        this.isLeaf = buffer.get() == 1;
        buffer.position(conf.getHeaderSize());
        for (int i = 0; i < childrenNum; i++) {
            children.add(buffer.getLong());
        }

        buffer.position(conf.getChildrenSize());
        List<KeyValue> kvs = Lists.newArrayList();
        for (int i = 0; i < entrySize; i++) {
            Record record = new Record(buffer);
            records.add(record);
            String key = new String(record.getKey(), java.nio.charset.StandardCharsets.UTF_8);
            String value = record.isOverflow() ? "" : new String(record.getValue(), java.nio.charset.StandardCharsets.UTF_8);
            kvs.add(new KeyValue(key, value));
        }

        this.node = new TreeNode();
        this.node.setPosition(position);
        this.node.setLeaf(isLeaf);
        this.node.setRoot(isRoot);
        this.node.setPageNo(pageNo);
        this.node.setReadFlag(true);
        this.node.setKeys(kvs);
        this.conf = conf;
    }

    private long getNodePosition(TreeNode node) {
        if (Objects.isNull(node)) {
            return 0L;
        }
        return node.getPosition();
    }

    private void recordsProperties() {
        this.records = Lists.newArrayList();
        if (Objects.isNull(keyValues) || keyValues.size() == 0) {
            this.free = conf.getHeaderSize();
            this.entrySize = 0;
            return;
        }
        int recordPosition = conf.getHeaderSize();
        for (KeyValue keyValue : keyValues) {
            Record record = new Record(keyValue, recordPosition);
            recordPosition += record.getTotalLen();
            this.records.add(record);
        }
        this.free = recordPosition;
        this.entrySize = records.size();
    }

    private void childrenProperties() {
        this.children = Lists.newArrayList();
        if (Objects.nonNull(this.childrenNodes) && this.childrenNodes.size() > 0) {
            for (TreeNode node : this.childrenNodes) {
                this.children.add(node.getPosition());
            }
        }
        this.childrenNum = this.children.size();
    }

    public ByteBuffer byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        buffer.putLong(this.position);
        buffer.putLong(this.parent);
        buffer.putLong(this.previous);
        buffer.putLong(this.next);
        buffer.putLong(this.pageNo);
        buffer.putInt(this.free);
        buffer.putInt(this.childrenNum);
        buffer.putInt(this.entrySize);
        buffer.put((byte) (this.isRoot ? 1 : 0));
        buffer.put((byte) (this.isLeaf ? 1 : 0));
        buffer.position(conf.getHeaderSize());
        for (int i = 0; i < childrenNum; i++) {
            buffer.putLong(children.get(i));
        }
        buffer.position(conf.getChildrenSize());
        if (records != null) {
            for (Record record : records) {
                buffer.put(record.byteBuffer());
            }
        }
        return buffer;
    }

    public TreeNode getNode() { return node; }
    public List<Record> getRecords() { return records; }
    public void setRecords(List<Record> records) { this.records = records; }
    public List<Long> getChildren() { return this.children; }
    public long getParent() { return parent; }
    public long getPrevious() { return previous; }
    public long getNext() { return next; }
    public long getPosition() { return position; }
}
