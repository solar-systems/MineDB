package cn.abelib.minedb.index;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class MetaNodeTest {

    @Test
    public void testCreateMetaNode() {
        Configuration conf = new Configuration();
        MetaNode meta = new MetaNode(conf);

        assertEquals("minedb", meta.getMAGIC());
        assertEquals("0.1", meta.getVersion());
        assertEquals(1, meta.getTotalPage());
        assertEquals(conf.getPageSize(), meta.getNextPage());
        assertEquals(conf.getPageSize(), meta.getRootPosition());
        assertEquals(0, meta.getEntryCount());
    }

    @Test
    public void testSetters() {
        Configuration conf = new Configuration();
        MetaNode meta = new MetaNode(conf);

        meta.setTotalPage(10);
        assertEquals(10, meta.getTotalPage());

        meta.setNextPage(100);
        assertEquals(100, meta.getNextPage());

        meta.setRootPosition(200);
        assertEquals(200, meta.getRootPosition());

        meta.setEntryCount(50);
        assertEquals(50, meta.getEntryCount());

        meta.setPageSize(32 * 1024);
        assertEquals(32 * 1024, meta.getPageSize());

        meta.setHeaderSize(256);
        assertEquals(256, meta.getHeaderSize());

        meta.setChildrenSize(2048);
        assertEquals(2048, meta.getChildrenSize());
    }

    @Test
    public void testGetVersion() {
        Configuration conf = new Configuration();
        MetaNode meta = new MetaNode(conf);

        assertEquals("0.1", meta.getVersion());
        assertEquals(0, meta.getMajorVersion());
        assertEquals(1, meta.getMinorVersion());
    }

    @Test
    public void testGetConfiguration() {
        Configuration conf = new Configuration();
        MetaNode meta = new MetaNode(conf);

        assertSame(conf, meta.getConfiguration());
    }

    @Test
    public void testSetConfiguration() {
        MetaNode meta = new MetaNode();
        Configuration conf = new Configuration();

        meta.setConfiguration(conf);

        assertSame(conf, meta.getConfiguration());
    }
}
