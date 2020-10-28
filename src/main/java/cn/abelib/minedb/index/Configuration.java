package cn.abelib.minedb.index;

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
     * 关键字长度设置
     */
    private int keySize;
    /**
     * 值的长度
     */
    private int entrySize;
    /**
     * B+树扇出
     */
    private int degree;
    /**
     * 页头部长度
     */
    private int headerSize;

    private String dbName;

    public Configuration(int degree) {
        this(16 * 1024, 1024, 1014, degree, 1024);
    }

    public Configuration(int pageSize, int keySize, int entrySize, int degree, int headerSize) {
        this.pageSize = pageSize;
        this.keySize = keySize;
        this.entrySize = entrySize;
        this.degree = degree;
        this.headerSize = headerSize;
        this.dbName = "default.db";
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public int getEntrySize() {
        return entrySize;
    }

    public void setEntrySize(int entrySize) {
        this.entrySize = entrySize;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
