package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.MetaNode;
import cn.abelib.minedb.index.TreeNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @Author: abel.huang
 * @Date: 2020-11-15 16:09
 */
public class PageTest {
    private TreeNode treeNode;
    private Configuration conf;

    @Before
    public void init() throws IOException {
        cleanup();
        conf = new Configuration();
        // Create the database file first
        MetaNode meta = new MetaNode(conf);
        PageLoader.writeMeta(meta);
        treeNode = new TreeNode(conf, true, true, 100);
        // Set position for the tree node
        treeNode.setPosition(conf.getPageSize());
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get("default.db"));
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
        PageLoader.writePage(treeNode);
        Page page = PageLoader.loadPage(conf, treeNode.getPosition());
        System.err.println(page);
    }
}
