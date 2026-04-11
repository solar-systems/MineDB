package cn.abelib.minedb.index.wal;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * WAL 条目测试
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class WalEntryTest {

    @Test
    public void testPutEntrySerialization() {
        WalEntry entry = new WalEntry(WalEntry.Type.PUT, "testKey", "testValue", 1L);

        ByteBuffer buffer = entry.toByteBuffer();
        // toByteBuffer() 已经调用了 flip()，不需要再次调用

        WalEntry deserialized = WalEntry.fromByteBuffer(buffer);

        assertEquals(WalEntry.Type.PUT, deserialized.getType());
        assertEquals("testKey", deserialized.getKey());
        assertEquals("testValue", deserialized.getValue());
        assertEquals(1L, deserialized.getSequence());
        assertTrue(deserialized.validate());
    }

    @Test
    public void testDeleteEntrySerialization() {
        WalEntry entry = new WalEntry(WalEntry.Type.DELETE, "deleteKey", null, 2L);

        ByteBuffer buffer = entry.toByteBuffer();

        WalEntry deserialized = WalEntry.fromByteBuffer(buffer);

        assertEquals(WalEntry.Type.DELETE, deserialized.getType());
        assertEquals("deleteKey", deserialized.getKey());
        assertNull(deserialized.getValue());
        assertEquals(2L, deserialized.getSequence());
        assertTrue(deserialized.validate());
    }

    @Test
    public void testSyncEntrySerialization() {
        WalEntry entry = new WalEntry(WalEntry.Type.SYNC, null, null, 3L);

        ByteBuffer buffer = entry.toByteBuffer();

        WalEntry deserialized = WalEntry.fromByteBuffer(buffer);

        assertEquals(WalEntry.Type.SYNC, deserialized.getType());
        assertNull(deserialized.getKey());
        assertNull(deserialized.getValue());
        assertEquals(3L, deserialized.getSequence());
        assertTrue(deserialized.validate());
    }

    @Test
    public void testGetSize() {
        WalEntry putEntry = new WalEntry(WalEntry.Type.PUT, "key", "value", 1L);
        assertTrue(putEntry.getSize() > WalEntry.HEADER_SIZE);

        WalEntry deleteEntry = new WalEntry(WalEntry.Type.DELETE, "key", null, 2L);
        assertTrue(deleteEntry.getSize() > WalEntry.HEADER_SIZE);

        WalEntry syncEntry = new WalEntry(WalEntry.Type.SYNC, null, null, 3L);
        assertEquals(WalEntry.HEADER_SIZE, syncEntry.getSize());
    }

    @Test
    public void testChecksumValidation() {
        WalEntry entry = new WalEntry(WalEntry.Type.PUT, "key", "value", 1L);
        assertTrue(entry.validate());

        // 重新序列化后校验应该成功
        ByteBuffer buffer = entry.toByteBuffer();
        WalEntry deserialized = WalEntry.fromByteBuffer(buffer);
        assertTrue(deserialized.validate());
    }

    @Test
    public void testTypeCodes() {
        assertEquals(1, WalEntry.Type.PUT.getCode());
        assertEquals(2, WalEntry.Type.DELETE.getCode());
        assertEquals(3, WalEntry.Type.SYNC.getCode());

        assertEquals(WalEntry.Type.PUT, WalEntry.Type.fromCode((byte) 1));
        assertEquals(WalEntry.Type.DELETE, WalEntry.Type.fromCode((byte) 2));
        assertEquals(WalEntry.Type.SYNC, WalEntry.Type.fromCode((byte) 3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTypeCode() {
        WalEntry.Type.fromCode((byte) 99);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMagic() {
        ByteBuffer buffer = ByteBuffer.allocate(WalEntry.HEADER_SIZE);
        buffer.putInt(0x12345678); // 无效的 magic
        buffer.put((byte) 1);
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.putLong(1);
        buffer.flip();

        WalEntry.fromByteBuffer(buffer);
    }

    @Test
    public void testLargeValue() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("a");
        }
        String largeValue = sb.toString();

        WalEntry entry = new WalEntry(WalEntry.Type.PUT, "largeKey", largeValue, 1L);

        ByteBuffer buffer = entry.toByteBuffer();

        WalEntry deserialized = WalEntry.fromByteBuffer(buffer);

        assertEquals("largeKey", deserialized.getKey());
        assertEquals(largeValue, deserialized.getValue());
        assertTrue(deserialized.validate());
    }

    @Test
    public void testGetters() {
        WalEntry entry = new WalEntry(WalEntry.Type.PUT, "key", "value", 123L);

        assertEquals(WalEntry.Type.PUT, entry.getType());
        assertEquals("key", entry.getKey());
        assertEquals("value", entry.getValue());
        assertEquals(123L, entry.getSequence());
        assertTrue(entry.getChecksum() != 0);
    }
}
