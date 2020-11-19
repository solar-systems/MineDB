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
 * @Author: abel.huang
 * @Date: 2020-10-26 23:42
 * 从磁盘中加载页
 * todo
 */
public class PageLoader {

    public static boolean existsMeta(Path path) {
        return Files.exists(path);
    }

    /**
     * MAGIC_NUMBER
     * 写入磁盘文件
     */
    public static void writeMeta(MetaNode meta) throws IOException {
        Path path = meta.getPath();
        Configuration conf = meta.getConfiguration();
        Files.createFile(path);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        // 6B
        buffer.put(ByteUtils.getBytesUTF8(meta.getMAGIC()));
        // 2B
        buffer.putShort(meta.getMajorVersion());
        // 2B
        buffer.putShort(meta.getMinorVersion());
        // 4B
        buffer.putInt(conf.getPageSize());
        // 8B
        buffer.putLong(meta.getTotalPage());
        // 8B
        buffer.putLong(meta.getNextPage());
        // 4B
        buffer.putInt(conf.getHeaderSize());
        // 4B
        buffer.putInt(conf.getChildrenSize());
        // total = 38B
        buffer.flip();
        channel.write(buffer);
        channel.force(true);
        channel.close();
    }

    /**
     * 加载元数据节点
     * @param conf
     * @return
     */
    public static MetaNode readMeta(Configuration conf) throws IOException {
        Path path = conf.getPath();
        MetaNode meta = new MetaNode();
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        channel.read(buffer);
        buffer.flip();
        String magic = ByteUtils.toUTF8String(buffer, 6);
        if (!StringUtils.equals(magic, meta.getMAGIC())) {
            throw new IllegalArgumentException("Invalid disk file!");
        }
        if (meta.getMajorVersion() != buffer.getShort()
                || meta.getMinorVersion() != buffer.getShort()) {
            throw new IllegalArgumentException("Invalid version file!");
        }
        meta.setConfiguration(conf);
        meta.setPageSize(buffer.getInt());
        meta.setTotalPage(buffer.getLong());
        meta.setNextPage(buffer.getLong());

        meta.setHeaderSize(buffer.getInt());
        meta.setChildrenSize(buffer.getInt());
        channel.close();
        return meta;
    }

    public static Page loadPage(Configuration conf, long position) throws IOException {
        FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.READ);
        channel.position(position);
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        channel.read(buffer);
        channel.close();
        return new Page(conf, buffer);
    }

    public static TreeNode loadTreeNode(Configuration conf, long position) throws IOException {
        Page page = loadPage(conf, position);
        return page.getNode();
    }

    public static List<TreeNode> loadTreeNodes(Configuration conf, List<Long> positions) throws IOException {
        List<TreeNode> nodes = Lists.newArrayList();
        for (int i = 0; i < positions.size(); i++) {
            TreeNode node = loadTreeNode(conf, positions.get(i));
            nodes.add(node);
        }
        return nodes;
    }

    public static void writePage(TreeNode node) throws IOException {
        Configuration conf = node.getConfiguration();
        FileChannel channel = FileChannel.open(conf.getPath(), StandardOpenOption.WRITE);
        channel.position(node.getPosition());
        ByteBuffer buffer = node.byteBuffer();
        buffer.flip();
        channel.write(buffer);
        channel.force(true);
        channel.close();
    }
}
