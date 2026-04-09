package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据溢出页
 *
 * <p>当记录大小超过页面可用空间时，数据存储在溢出页中。
 * 溢出页通过链表连接，支持大数据的分页存储。
 *
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class OverFlowPage {
    /** 页面魔数，用于校验 */
    private static final int PAGE_MAGIC = 0x4F564654; // "OVFT"

    /** 页面头大小：position(8) + magic(4) + dataLen(4) + nextPage(8) = 24 */
    public static final int PAGE_HEADER_SIZE = 24;

    /** 页面位置 */
    private long position;
    /** 实际数据长度 */
    private int dataLen;
    /** 下一页位置（用于链式溢出页，-1表示无下一页） */
    private long nextPage;
    /** 数据 */
    private byte[] data;
    /** 配置 */
    private Configuration conf;

    public OverFlowPage(Configuration conf, long position) {
        this.conf = conf;
        this.position = position;
        this.dataLen = 0;
        this.nextPage = -1;
        this.data = new byte[getAvailableDataSpace(conf)];
    }

    public OverFlowPage(Configuration conf, ByteBuffer buffer) {
        this.conf = conf;
        this.position = buffer.getLong();
        int magic = buffer.getInt();
        if (magic != PAGE_MAGIC) {
            throw new IllegalArgumentException("Invalid overflow page magic: " + magic);
        }
        this.dataLen = buffer.getInt();
        this.nextPage = buffer.getLong();
        this.data = ByteUtils.toBytes(buffer, dataLen);
    }

    /**
     * 获取页面可用数据空间
     */
    public static int getAvailableDataSpace(Configuration conf) {
        return conf.getPageSize() - PAGE_HEADER_SIZE;
    }

    /**
     * 计算存储指定大小数据需要的溢出页数量
     */
    public static int calculateOverflowPageCount(Configuration conf, int dataSize) {
        int availableSpace = getAvailableDataSpace(conf);
        return (dataSize + availableSpace - 1) / availableSpace;
    }

    /**
     * 创建溢出页链表
     *
     * @param conf 配置
     * @param data 完整数据
     * @param startPosition 起始位置
     * @return 溢出页列表
     */
    public static List<OverFlowPage> createOverflowPages(Configuration conf, byte[] data, long startPosition) {
        List<OverFlowPage> pages = new ArrayList<>();
        int availableSpace = getAvailableDataSpace(conf);
        int totalLen = data.length;
        int offset = 0;
        long currentPosition = startPosition;

        while (offset < totalLen) {
            OverFlowPage page = new OverFlowPage(conf, currentPosition);
            int copyLen = Math.min(availableSpace, totalLen - offset);

            byte[] pageData = new byte[copyLen];
            System.arraycopy(data, offset, pageData, 0, copyLen);
            page.setData(pageData);

            offset += copyLen;
            currentPosition += conf.getPageSize();

            // 如果还有更多数据，设置下一页指针
            if (offset < totalLen) {
                page.setNextPage(currentPosition);
            }

            pages.add(page);
        }

        return pages;
    }

    /**
     * 合并多个溢出页的数据
     */
    public static byte[] mergeData(List<OverFlowPage> pages) {
        int totalLen = 0;
        for (OverFlowPage page : pages) {
            totalLen += page.getDataLen();
        }

        byte[] result = new byte[totalLen];
        int offset = 0;
        for (OverFlowPage page : pages) {
            System.arraycopy(page.getData(), 0, result, offset, page.getDataLen());
            offset += page.getDataLen();
        }

        return result;
    }

    /**
     * 序列化为 ByteBuffer
     */
    public ByteBuffer byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        buffer.putLong(position);
        buffer.putInt(PAGE_MAGIC);
        buffer.putInt(dataLen);
        buffer.putLong(nextPage);
        buffer.put(data, 0, dataLen);
        buffer.flip();
        return buffer;
    }

    // Getters and Setters
    public long getPosition() { return position; }
    public void setPosition(long position) { this.position = position; }

    public int getDataLen() { return dataLen; }
    public void setDataLen(int dataLen) { this.dataLen = dataLen; }

    public long getNextPage() { return nextPage; }
    public void setNextPage(long nextPage) { this.nextPage = nextPage; }

    public byte[] getData() {
        // Return only the actual data, not the padded buffer
        byte[] result = new byte[dataLen];
        System.arraycopy(data, 0, result, 0, dataLen);
        return result;
    }

    public void setData(byte[] data) {
        int maxLen = getAvailableDataSpace(conf);
        if (data.length > maxLen) {
            throw new IllegalArgumentException("Data too large for overflow page: " + data.length + " > " + maxLen);
        }
        this.data = new byte[maxLen];
        System.arraycopy(data, 0, this.data, 0, data.length);
        this.dataLen = data.length;
    }

    public Configuration getConfiguration() { return conf; }
    public void setConfiguration(Configuration conf) { this.conf = conf; }
}
