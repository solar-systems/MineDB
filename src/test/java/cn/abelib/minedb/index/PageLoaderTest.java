package cn.abelib.minedb.index;

import cn.abelib.minedb.index.fs.PageLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: abel.huang
 * @Date: 2020-11-14 18:17
 */
public class PageLoaderTest {
    private Configuration configuration;

    @Before
    public void init() {
        configuration = new Configuration();
    }

    @Test
    public void metaWriteTest() throws IOException {
        MetaNode metaNode = new MetaNode(configuration);
        PageLoader.writeMeta(metaNode);
    }

    @Test
    public void metaReadTest() throws IOException {
        MetaNode metaNode = PageLoader.readMeta(configuration);
        System.err.println(metaNode);
    }
}
