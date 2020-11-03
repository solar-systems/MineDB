package cn.abelib.minedb.index;

import org.junit.Test;

import java.io.IOException;

/**
 * @Author: abel.huang
 * @Date: 2020-10-29 00:33
 */
public class BalanceTreeTest {

    @Test
    public void initTest() throws IOException {
        BalanceTree balanceTree = new BalanceTree(new Configuration(5));
        balanceTree.getMetaNode();
    }
}
