package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.TreeNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
     * 会有部分空缺
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
    private int pageNo;
    /**
     * 最小记录索引
     */
    private int minRecord;
    /**
     * 最大记录索引
     */
    private int maxRecord;
    /**
     * 空闲空间位置
     */
    private int free;
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
     * 是否是溢出节点
     */
    private boolean isOverFlow;
    /**
     * 节点是否已经满了
     */
    private boolean isFull;
    /**
     * 节点是否已经被删除
     */
    private boolean isDeleted;
    /**
     * 节点是否已经过半
     */
    private boolean isHalf;
    private Path path;
    private TreeNode node;
    private Configuration conf;

    public Page(Path path, TreeNode node) {
        this.path = path;
        this.node = node;
        this.conf = node.getConfiguration();
        persistence();
    }

    private void persistence() {
        // 判断数据库文件是否存在
        if (Files.exists(this.path)) {
            readPage();
        } else {
            createPage();
        }
    }

    /**
     * 创建新的磁盘页
     */
    private void createPage() {
        this.conf.getEntrySize();
    }

    /**
     * 从Page中读取Node相关的数据
     */
    private void readPage() {

    }

    public TreeNode getNode() {
        return node;
    }
}
