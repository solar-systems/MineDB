package cn.abelib.minedb.utils;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @Author: abel.huang
 * @Date: 2020-10-28 23:00
 */
public class ByteUtilsTest {

    @Test
    public void testInt2Bytes() {
        int value = 123456;
        byte[] bytes = ByteUtils.int2Bytes(value);

        assertEquals(4, bytes.length);
        assertEquals(value, ByteUtils.bytes2Int(bytes));
    }

    @Test
    public void testShort2Bytes() {
        short value = 1234;
        byte[] bytes = ByteUtils.short2Bytes(value);

        assertEquals(2, bytes.length);
        assertEquals(value, ByteUtils.bytes2Short(bytes));
    }

    @Test
    public void testBytes2UTF8() {
        String str = "Hello World";
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        assertEquals(str, ByteUtils.bytes2UTF8(bytes));
    }

    @Test
    public void testGetBytesUTF8() {
        String str = "测试中文";
        byte[] bytes = ByteUtils.getBytesUTF8(str);

        assertEquals(str, new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testString2Binary() {
        String str = "A";
        String binary = ByteUtils.string2Binary(str);

        assertNotNull(binary);
        assertTrue(binary.length() > 0);
    }

    @Test
    public void testConcatBytes() {
        byte[] a = {1, 2, 3};
        byte[] b = {4, 5, 6};

        byte[] result = ByteUtils.concatBytes(a, b);

        assertEquals(6, result.length);
        assertEquals(1, result[0]);
        assertEquals(6, result[5]);
    }

    @Test
    public void testSlice() {
        byte[] data = {1, 2, 3, 4, 5};

        byte[] slice = ByteUtils.slice(data, 1, 3);

        assertEquals(3, slice.length);
        assertEquals(2, slice[0]);
        assertEquals(4, slice[2]);
    }

    @Test
    public void testToUTF8StringByteBuffer() {
        String str = "Hello";
        ByteBuffer buffer = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));

        String result = ByteUtils.toUTF8String(buffer);

        assertEquals(str, result);
    }

    @Test
    public void testToUTF8StringWithLen() {
        String str = "Hello";
        ByteBuffer buffer = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));

        String result = ByteUtils.toUTF8String(buffer, 5);

        assertEquals(str, result);
    }

    @Test
    public void testToBytes() {
        byte[] data = {1, 2, 3, 4, 5};
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] result = ByteUtils.toBytes(buffer, 3);

        assertEquals(3, result.length);
        assertEquals(1, result[0]);
        assertEquals(3, result[2]);
    }

    @Test
    public void testStringBytesWithLen() {
        String str = "Hello";
        byte[] bytes = ByteUtils.stringBytesWithLen(str);

        assertNotNull(bytes);
        assertTrue(bytes.length > str.length());
    }
}
