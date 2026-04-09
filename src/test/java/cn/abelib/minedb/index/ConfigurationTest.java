package cn.abelib.minedb.index;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class ConfigurationTest {

    @Test
    public void testDefaultConfiguration() {
        Configuration conf = new Configuration();

        assertEquals(16 * 1024, conf.getPageSize());
        assertEquals(128, conf.getHeaderSize());
        assertEquals(1024, conf.getChildrenSize());
        assertEquals("default.db", conf.getDbName());
    }

    @Test
    public void testCustomConfiguration() {
        Configuration conf = new Configuration(8 * 1024, 64, 512);

        assertEquals(8 * 1024, conf.getPageSize());
        assertEquals(64, conf.getHeaderSize());
        assertEquals(512, conf.getChildrenSize());
    }

    @Test
    public void testSetters() {
        Configuration conf = new Configuration();

        conf.setPageSize(32 * 1024);
        assertEquals(32 * 1024, conf.getPageSize());

        conf.setHeaderSize(256);
        assertEquals(256, conf.getHeaderSize());

        conf.setChildrenSize(2048);
        assertEquals(2048, conf.getChildrenSize());

        conf.setDbName("test.db");
        assertEquals("test.db", conf.getDbName());
    }

    @Test
    public void testGetPath() {
        Configuration conf = new Configuration();
        conf.setDbName("mytest.db");

        assertEquals("mytest.db", conf.getPath().toString());
    }
}
