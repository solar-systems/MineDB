package cn.abelib.minedb.index;

import java.nio.file.Path;

/**
 * @Author: abel.huang
 * @Date: 2020-10-26 23:40
 */
public class MetaNode {
    /**
     * 魔数, 6bytes
     */
    private final String MAGIC = "minedb";
    /**
     * 大版本，2bytes
     */
    private final short majorVersion = 0;
    /**
     * 小版本，2bytes
     */
    private final short minorVersion = 1;
    /**
     * 当前B+树分页数
     */
    private long totalPage;
    /**
     * 下一页的大小
     */
    private long nextPage;
    /**
     * 头部大小
     */
    private int headerSize;
    /**
     * 子节点指针大小
     */
    private int childrenSize;

    private int pageSize;

    private Configuration conf;

    private Path path;

    public MetaNode(Configuration conf){
        this.totalPage = 0;
        this.nextPage = 0;
        this.conf = conf;
        this.path = conf.getPath();
        this.pageSize = conf.getPageSize();
        this.headerSize = conf.getHeaderSize();
        this.childrenSize = conf.getChildrenSize();
    }

    public MetaNode() {

    }

    public long getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(long totalPage) {
        this.totalPage = totalPage;
    }

    public String getVersion() {
        return this.majorVersion + "." + this.minorVersion;
    }

    public Path getPath() {
        return path;
    }

    public Configuration getConfiguration() {
        return this.conf;
    }

    public void setConfiguration(Configuration configuration) {
        this.conf = configuration;
        this.path = conf.getPath();
    }

    public long getNextPage() {
        return nextPage;
    }

    public void setNextPage(long nextPage) {
        this.nextPage = nextPage;
    }

    public String getMAGIC() {
        return MAGIC;
    }

    public short getMajorVersion() {
        return majorVersion;
    }

    public short getMinorVersion() {
        return minorVersion;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public int getChildrenSize() {
        return childrenSize;
    }

    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
        this.conf.setHeaderSize(getHeaderSize());
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
        this.conf.setPageSize(getPageSize());
    }

    public void setChildrenSize(int childrenSize) {
        this.childrenSize = childrenSize;
        this.conf.setChildrenSize(getChildrenSize());
    }
}
