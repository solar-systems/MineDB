package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.MetaPage;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @Author: abel.huang
 * @Date: 2020-10-26 23:40
 */
public class MetaNode {
    private Path path;
    /**
     * 当前B+树分页数
     */
    private long pageTotal;

    private String version;

    private long pageSize;

    private int entrySize;
    /**
     * 下一个页的地址
     */
    private long nextPage;
    /**
     * 根节点
     */
    private TreeNode root;

    private MetaPage page;

    private Configuration conf;

    public MetaNode(Configuration conf) throws IOException {
        this.pageTotal = 0;
        this.path = Paths.get(conf.getDbName());
        this.conf = conf;
        this.page = new MetaPage(this.path, this);
    }

    public long getPageTotal() {
        return pageTotal;
    }

    public void setPageTotal(long pageTotal) {
        this.pageTotal = pageTotal;
    }

    public String getVersion() {
        return this.version;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public TreeNode getRoot() {
        return root;
    }

    public void setRoot(TreeNode root) {
        this.root = root;
    }

    public MetaPage getPage() {
        return page;
    }

    public void setPage(MetaPage page) {
        this.page = page;
    }

    public Configuration getConfiguration() {
        return this.conf;
    }

    public long getNextPage() {
        return nextPage;
    }

    public void setNextPage(long nextPage) {
        this.nextPage = nextPage;
    }
}
