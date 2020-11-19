package cn.abelib.minedb.index;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: abel.huang
 * @Date: 2020-10-29 00:33
 */
public class BalanceTreeTest {
    BalanceTree balanceTree;

    @Before
    public void init() throws IOException {
        balanceTree = new BalanceTree(new Configuration());
    }

    @Test
    public void initTest() {
        System.err.println(balanceTree.getConfiguration());
        System.err.println(balanceTree.getMetaNode());
    }

    @Test
    public void insertTest() throws Exception {
        balanceTree.insert("Hello", "World");
        balanceTree.insert("Foo", "Bar");
        balanceTree.insert("Abel", "Huang");
    }
}
