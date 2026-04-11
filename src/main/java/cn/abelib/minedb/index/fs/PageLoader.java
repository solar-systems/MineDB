package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.MetaNode;
import cn.abelib.minedb.index.TreeNode;
import cn.abelib.minedb.utils.ByteUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 磁盘页加载器
 *
 * @Author: abel.huang
 * @Date: 2020-10-25 00:50
 */
public class PageLoader {

    public static boolean existsMeta(Path path) {
        return Files.exists(path);
    }

    public static void writeMeta(MetaNode meta) throws IOException {
        Path path = meta.getPath();
        Configuration conf = meta.getConfiguration();
        // Delete existing file if present to allow overwriting
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.position(0);
            ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
            buffer.put(ByteUtils.getBytesUTF8(meta.getMAGIC()));
            buffer.putShort(meta.getMajorVersion());
            buffer.putShort(meta.getMinorVersion());
            buffer.putInt(conf.getPageSize());
            buffer.putLong(meta.getTotalPage());
            buffer.putLong(meta.getNextPage());
            buffer.putLong(meta.getRootPosition());
            buffer.putLong(meta.getEntryCount());
            buffer.putInt(conf.getHeaderSize());
            buffer.putInt(conf.getChildrenSize());
            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        }
    }

    public static void updateMeta(MetaNode meta) throws IOException {
        Path path = meta.getPath();
        Configuration conf = meta.getConfiguration();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.position(0);
            ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
            buffer.put(ByteUtils.getBytesUTF8(meta.getMAGIC()));
            buffer.putShort(meta.getMajorVersion());
            buffer.putShort(meta.getMinorVersion());
            buffer.putInt(conf.getPageSize());
            buffer.putLong(meta.getTotalPage());
            buffer.putLong(meta.getNextPage());
            buffer.putLong(meta.getRootPosition());
            buffer.putLong(meta.getEntryCount());
            buffer.putInt(conf.getHeaderSize());
            buffer.putInt(conf.getChildrenSize());
            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        }
    }

    public static MetaNode readMeta(Configuration conf) throws IOException {
        Path path = conf.getPath();
        MetaNode meta = new MetaNode();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(0);
            ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
            channel.read(buffer);
            buffer.flip();
            String magic = ByteUtils.toUTF8String(buffer, 6);
            if (!StringUtils.equals(magic, meta.getMAGIC())) {
                throw new IllegalArgumentException("Invalid disk file!");
            }
            buffer.getShort(); // majorVersion
            buffer.getShort(); // minorVersion
            meta.setConfiguration(conf);
            meta.setPageSize(buffer.getInt());
            meta.setTotalPage(buffer.getLong());
            meta.setNextPage(buffer.getLong());
            if (buffer.remaining() >= 8) {
                meta.setRootPosition(buffer.getLong());
            }
            if (buffer.remaining() >= 8) {
                meta.setEntryCount(buffer.getLong());
            }
            if (buffer.remaining() >= 8) {
                meta.setHeaderSize(buffer.getInt());
                meta.setChildrenSize(buffer.getInt());
            }
        }
        return meta;
    }

    public static Page loadPage(Configuration conf, long position) throws IOException {
        try (FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.READ)) {
            channel.position(position);
            ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
            channel.read(buffer);
            return new Page(conf, buffer);
        }
    }

    public static TreeNode loadTreeNode(Configuration conf, long position) throws IOException {
        Page page = loadPage(conf, position);
        TreeNode node = page.getNode();
        node.setConfiguration(conf);
        node.setPage(page);
        return node;
    }

    public static List<TreeNode> loadTreeNodes(Configuration conf, List<Long> positions) throws IOException {
        List<TreeNode> nodes = Lists.newArrayList();
        for (Long pos : positions) {
            nodes.add(loadTreeNode(conf, pos));
        }
        return nodes;
    }

    public static void writePage(TreeNode node) throws IOException {
        Configuration conf = node.getConfiguration();
        try (FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.WRITE)) {
            channel.position(node.getPosition());
            ByteBuffer buffer = node.byteBuffer();
            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        }
    }

    // ==================== 溢出页操作 ====================

    /**
     * 写入溢出页
     */
    public static void writeOverflowPage(OverFlowPage page) throws IOException {
        Configuration conf = page.getConfiguration();
        try (FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.WRITE)) {
            channel.position(page.getPosition());
            ByteBuffer buffer = page.byteBuffer();
            channel.write(buffer);
            channel.force(true);
        }
    }

    /**
     * 加载溢出页
     */
    public static OverFlowPage loadOverflowPage(Configuration conf, long position) throws IOException {
        try (FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.READ)) {
            channel.position(position);
            ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
            channel.read(buffer);
            buffer.flip();
            return new OverFlowPage(conf, buffer);
        }
    }

    /**
     * 加载溢出页链表中的所有页面
     */
    public static List<OverFlowPage> loadOverflowPageChain(Configuration conf, long startPosition) throws IOException {
        List<OverFlowPage> pages = Lists.newArrayList();
        long currentPos = startPosition;

        while (currentPos > 0) {
            OverFlowPage page = loadOverflowPage(conf, currentPos);
            pages.add(page);
            currentPos = page.getNextPage();
        }

        return pages;
    }

    /**
     * 写入溢出页链表
     */
    public static void writeOverflowPages(List<OverFlowPage> pages) throws IOException {
        for (OverFlowPage page : pages) {
            writeOverflowPage(page);
        }
    }

    // ==================== 空闲页链表操作 ====================

    /**
     * 写入空闲页链表
     */
    public static void writeFreePageList(FreePageList freePageList) throws IOException {
        Configuration conf = freePageList.getConfiguration();
        try (FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.WRITE)) {
            channel.position(freePageList.getDiskPosition());
            ByteBuffer buffer = freePageList.byteBuffer();
            channel.write(buffer);
            channel.force(true);
        }
    }

    /**
     * 加载空闲页链表
     */
    public static FreePageList loadFreePageList(Configuration conf, long position) throws IOException {
        try (FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.READ)) {
            channel.position(position);
            ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
            channel.read(buffer);
            return FreePageList.fromByteBuffer(conf, position, buffer);
        }
    }
}
