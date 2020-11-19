package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.Page;
import cn.abelib.minedb.index.fs.PageLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: abel.huang
 * @Date: 2020-11-15 16:09
 */
public class PageTest {
    private TreeNode treeNode;
    private Configuration conf;

    @Before
    public void init() throws IOException {
        conf = new Configuration();
        treeNode = new TreeNode(conf, true, true, 100);
    }

    @Test
    public void writePageTest() {
         Page page = treeNode.getPage();
         System.err.println(page);
    }

    @Test
    public void writePage2Test() throws IOException {
        PageLoader.writePage(treeNode);
    }

    @Test
    public void loadPageTest() throws IOException {
        Page page = PageLoader.loadPage(conf, 0);
        System.err.println(page);
    }
}
