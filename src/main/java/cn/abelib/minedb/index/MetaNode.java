package cn.abelib.minedb.index;

import java.nio.file.Path;

/**
 * 元数据节点
 *
 * @Author: abel.huang
 * @Date: 2020-10-25 00:50
 */
public class MetaNode {
    private final String MAGIC = "minedb";
    private final short majorVersion = 0;
    private final short minorVersion = 1;
    private long totalPage;
    private long nextPage;
    private long rootPosition;
    private long entryCount;
    private int headerSize;
    private int childrenSize;
    private int pageSize;
    private Configuration conf;
    private Path path;

    public MetaNode(Configuration conf) {
        this.totalPage = 1;
        this.nextPage = conf.getPageSize();
        this.rootPosition = conf.getPageSize();
        this.entryCount = 0;
        this.conf = conf;
        this.path = conf.getPath();
        this.pageSize = conf.getPageSize();
        this.headerSize = conf.getHeaderSize();
        this.childrenSize = conf.getChildrenSize();
    }

    public MetaNode() {}

    public long getTotalPage() { return totalPage; }
    public void setTotalPage(long totalPage) { this.totalPage = totalPage; }

    public String getVersion() { return this.majorVersion + "." + this.minorVersion; }

    public Path getPath() { return path; }

    public Configuration getConfiguration() { return this.conf; }
    public void setConfiguration(Configuration configuration) {
        this.conf = configuration;
        this.path = conf.getPath();
    }

    public long getNextPage() { return nextPage; }
    public void setNextPage(long nextPage) { this.nextPage = nextPage; }

    public long getRootPosition() { return rootPosition; }
    public void setRootPosition(long rootPosition) { this.rootPosition = rootPosition; }

    public long getEntryCount() { return entryCount; }
    public void setEntryCount(long entryCount) { this.entryCount = entryCount; }

    public String getMAGIC() { return MAGIC; }
    public short getMajorVersion() { return majorVersion; }
    public short getMinorVersion() { return minorVersion; }

    public int getPageSize() { return this.pageSize; }
    public int getHeaderSize() { return headerSize; }
    public int getChildrenSize() { return childrenSize; }

    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
        if (this.conf != null) {
            this.conf.setHeaderSize(headerSize);
        }
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
        if (this.conf != null) {
            this.conf.setPageSize(pageSize);
        }
    }

    public void setChildrenSize(int childrenSize) {
        this.childrenSize = childrenSize;
        if (this.conf != null) {
            this.conf.setChildrenSize(childrenSize);
        }
    }
}
