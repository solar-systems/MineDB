package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.MetaNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @Author: abel.huang
 * @Date: 2020-11-14 18:17
 */
public class PageLoaderTest {
    private Configuration configuration;

    @Before
    public void init() throws IOException {
        cleanup();
        configuration = new Configuration();
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get("default.db"));
    }

    @Test
    public void metaWriteTest() throws IOException {
        MetaNode metaNode = new MetaNode(configuration);
        PageLoader.writeMeta(metaNode);
    }

    @Test
    public void metaReadTest() throws IOException {
        // Write first, then read
        MetaNode metaNode = new MetaNode(configuration);
        PageLoader.writeMeta(metaNode);
        MetaNode readMeta = PageLoader.readMeta(configuration);
        System.err.println(readMeta);
    }
}
