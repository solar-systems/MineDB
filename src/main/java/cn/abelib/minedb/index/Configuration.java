package cn.abelib.minedb.index;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: abel.huang
 * @Date: 2020-10-23 23:57
 */
public class Configuration {
    /**
     * 单个页大小， 默认16kB
     */
    private int pageSize;
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

    private String dbName;

    public Configuration() {
        this(16 * 1024, 128, 1024);
    }

    public Configuration(int pageSize, int headerSize, int childrenSize) {
        this.pageSize = pageSize;
        this.headerSize = headerSize;
        this.childrenSize = childrenSize;
        this.dbName = "default.db";
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    }

    public int getChildrenSize() {
        return childrenSize;
    }

    public void setChildrenSize(int childrenSize) {
        this.childrenSize = childrenSize;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Path getPath() {
        return Paths.get(getDbName());
    }

}


