package cn.abelib.minedb;

import cn.abelib.minedb.utils.ByteUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @Author: abel.huang
 * @Date: 2020-10-28 23:00
 */
public class SimpleTest {


    @Test
    public void strBytesLen() {
        String magic = "MINEDB";
        System.err.println(magic.getBytes(StandardCharsets.UTF_8).length);
        String version = "1.1.1";
        System.err.println(version.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void shortBytesTest() {
        short majorVersion = 10;
        byte[] bytes = ByteUtils.short2Bytes(majorVersion);
        System.err.println(bytes.length);
        System.err.println(ByteUtils.bytes2Short(bytes));
    }

    @Test
    public void byte2Boolean() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put((byte) 0);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);
        buffer.flip();
        while (buffer.hasRemaining()) {
            System.err.println(buffer.get());
        }

    }
}
