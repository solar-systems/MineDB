package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.MetaNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class PageLoaderOverflowTest {
    private Configuration conf;

    @Before
    public void init() throws IOException {
        cleanup();
        conf = new Configuration();
        // Create database file
        MetaNode meta = new MetaNode(conf);
        PageLoader.writeMeta(meta);
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get("default.db"));
    }

    @Test
    public void testWriteAndLoadOverflowPage() throws IOException {
        OverFlowPage original = new OverFlowPage(conf, conf.getPageSize() * 2);
        byte[] data = "Test overflow data".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        original.setData(data);

        PageLoader.writeOverflowPage(original);
        OverFlowPage loaded = PageLoader.loadOverflowPage(conf, original.getPosition());

        assertEquals(original.getPosition(), loaded.getPosition());
        assertEquals(original.getDataLen(), loaded.getDataLen());
        assertArrayEquals(data, loaded.getData());
    }

    @Test
    public void testOverflowPageChain() throws IOException {
        // Create chain of overflow pages
        int availableSpace = OverFlowPage.getAvailableDataSpace(conf);
        byte[] largeData = new byte[availableSpace * 2 + 100];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        List<OverFlowPage> pages = OverFlowPage.createOverflowPages(conf, largeData, conf.getPageSize() * 2);
        PageLoader.writeOverflowPages(pages);

        // Load chain
        List<OverFlowPage> loadedPages = PageLoader.loadOverflowPageChain(conf, conf.getPageSize() * 2);

        assertEquals(pages.size(), loadedPages.size());

        // Verify data
        byte[] recovered = OverFlowPage.mergeData(loadedPages);
        assertArrayEquals(largeData, recovered);
    }

    @Test
    public void testOverflowPageWithNext() throws IOException {
        OverFlowPage page1 = new OverFlowPage(conf, conf.getPageSize() * 2);
        page1.setData("Page 1 data".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        page1.setNextPage(conf.getPageSize() * 3);

        OverFlowPage page2 = new OverFlowPage(conf, conf.getPageSize() * 3);
        page2.setData("Page 2 data".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        PageLoader.writeOverflowPage(page1);
        PageLoader.writeOverflowPage(page2);

        OverFlowPage loaded1 = PageLoader.loadOverflowPage(conf, page1.getPosition());
        assertEquals(page1.getNextPage(), loaded1.getNextPage());

        OverFlowPage loaded2 = PageLoader.loadOverflowPage(conf, page2.getPosition());
        assertEquals(-1, loaded2.getNextPage());
    }
}
