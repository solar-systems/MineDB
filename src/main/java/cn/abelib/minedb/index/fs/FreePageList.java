package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 自由页链表管理
 *
 * <p>管理溢出页的空间分配和回收，维护一个空闲页链表，
 * 当需要分配新的溢出页时，优先从空闲链表中获取，避免文件无限增长。
 *
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:10
 */
public class FreePageList {
    /** 页面魔数，用于校验 */
    private static final int PAGE_MAGIC = 0x46524545; // "FREE"

    /** 页面头大小：magic(4) + count(4) + nextAllocPos(8) + reserved(8) = 24 */
    private static final int HEADER_SIZE = 24;

    /** 每个页位置占用的字节数 */
    private static final int POSITION_SIZE = 8;

    /** 配置 */
    private Configuration conf;

    /** 自由页位置列表 */
    private List<Long> freePages;

    /** 下一个可分配的页位置 */
    private long nextAllocPosition;

    /** 空闲页链表在磁盘中的位置 */
    private long diskPosition;

    public FreePageList(Configuration conf) {
        this.conf = conf;
        this.freePages = new ArrayList<>();
        // 初始位置从元数据页之后开始，预留足够空间
        this.nextAllocPosition = conf.getPageSize() * 10;
        // 空闲页链表存储在元数据页之后
        this.diskPosition = conf.getPageSize();
    }

    public FreePageList(Configuration conf, long diskPosition) {
        this.conf = conf;
        this.diskPosition = diskPosition;
        this.freePages = new ArrayList<>();
        this.nextAllocPosition = conf.getPageSize() * 10;
    }

    /**
     * 分配一个新的溢出页位置
     *
     * @return 页位置
     */
    public long allocatePage() {
        if (!freePages.isEmpty()) {
            return freePages.remove(freePages.size() - 1);
        }

        long position = nextAllocPosition;
        nextAllocPosition += conf.getPageSize();
        return position;
    }

    /**
     * 分配多个溢出页位置
     *
     * @param count 页数量
     * @return 页位置列表
     */
    public List<Long> allocatePages(int count) {
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            positions.add(allocatePage());
        }
        return positions;
    }

    /**
     * 释放一个溢出页
     *
     * @param position 页位置
     */
    public void freePage(long position) {
        if (position > 0) {
            freePages.add(position);
        }
    }

    /**
     * 释放溢出页链表中的所有页面
     *
     * @param startPosition 起始位置
     */
    public void freeOverflowPageChain(long startPosition) {
        if (startPosition <= 0) {
            return;
        }

        try {
            List<OverFlowPage> pages = PageLoader.loadOverflowPageChain(conf, startPosition);
            for (OverFlowPage page : pages) {
                freePage(page.getPosition());
            }
        } catch (Exception e) {
            // 忽略加载失败的情况
        }
    }

    /**
     * 释放多个溢出页
     *
     * @param positions 页位置列表
     */
    public void freePages(List<Long> positions) {
        if (positions != null) {
            freePages.addAll(positions);
        }
    }

    /**
     * 获取空闲页数量
     */
    public int getFreePageCount() {
        return freePages.size();
    }

    /**
     * 获取下一个分配位置
     */
    public long getNextAllocPosition() {
        return nextAllocPosition;
    }

    /**
     * 设置下一个分配位置（用于从磁盘恢复）
     */
    public void setNextAllocPosition(long position) {
        this.nextAllocPosition = position;
    }

    /**
     * 获取自由页列表（用于持久化）
     */
    public List<Long> getFreePages() {
        return new ArrayList<>(freePages);
    }

    /**
     * 设置自由页列表（用于从磁盘恢复）
     */
    public void setFreePages(List<Long> pages) {
        this.freePages = new ArrayList<>(pages);
    }

    /**
     * 获取磁盘存储位置
     */
    public long getDiskPosition() {
        return diskPosition;
    }

    /**
     * 设置磁盘存储位置
     */
    public void setDiskPosition(long position) {
        this.diskPosition = position;
    }

    /**
     * 获取配置
     */
    public Configuration getConfiguration() {
        return conf;
    }

    /**
     * 清空自由页链表
     */
    public void clear() {
        freePages.clear();
    }

    /**
     * 序列化为 ByteBuffer
     */
    public ByteBuffer byteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());

        // 写入头部
        buffer.putInt(PAGE_MAGIC);
        buffer.putInt(freePages.size());
        buffer.putLong(nextAllocPosition);
        buffer.putLong(0); // reserved

        // 写入空闲页列表
        for (Long position : freePages) {
            if (buffer.remaining() >= POSITION_SIZE) {
                buffer.putLong(position);
            }
        }

        buffer.flip();
        return buffer;
    }

    /**
     * 从 ByteBuffer 反序列化
     */
    public static FreePageList fromByteBuffer(Configuration conf, long diskPosition, ByteBuffer buffer) {
        FreePageList freePageList = new FreePageList(conf, diskPosition);

        // 检查 buffer 是否有足够的数据
        if (buffer.remaining() < HEADER_SIZE) {
            return freePageList;
        }

        int magic = buffer.getInt();
        if (magic != PAGE_MAGIC) {
            // 无效的空闲页链表，返回空的新实例
            return freePageList;
        }

        int count = buffer.getInt();
        freePageList.nextAllocPosition = buffer.getLong();
        buffer.getLong(); // skip reserved

        List<Long> pages = new ArrayList<>();
        for (int i = 0; i < count && buffer.remaining() >= POSITION_SIZE; i++) {
            pages.add(buffer.getLong());
        }
        freePageList.setFreePages(pages);

        return freePageList;
    }

    /**
     * 保存到磁盘
     */
    public void save() throws java.io.IOException {
        PageLoader.writeFreePageList(this);
    }

    /**
     * 从磁盘加载
     */
    public void load() throws java.io.IOException {
        FreePageList loaded = PageLoader.loadFreePageList(conf, diskPosition);
        this.freePages = loaded.getFreePages();
        this.nextAllocPosition = loaded.getNextAllocPosition();
    }
}
