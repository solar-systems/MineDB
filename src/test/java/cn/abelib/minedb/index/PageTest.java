package cn.abelib.minedb.index;

import org.junit.Before;
import org.junit.Test;

/**
 * @Author: abel.huang
 * @Date: 2020-11-15 16:09
 */
public class PageTest {
    private TreeNode treeNode;
    private Configuration conf;

    @Before
    public void init() {
        conf = new Configuration();
        treeNode = new TreeNode(conf, true, true, 100);
    }

    @Test
    public void writePageTest() {
        treeNode.getPage();
    }
}
