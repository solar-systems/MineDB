package cn.abelib.minedb.index.fs;

import cn.abelib.minedb.index.Configuration;
import cn.abelib.minedb.index.MetaNode;
import cn.abelib.minedb.utils.ByteUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @Author: abel.huang
 * @Date: 2020-10-25 00:45
 * B+树 Meta页
 * 用于存放B+树索引元信息
 */
public class MetaPage {
    /**
     * 魔数, 6bytes
     */
    private final String MAGIC = "minedb";
    /**
     * 大版本，2bytes
     */
    private final short majorVersion = 0;
    /**
     * 小版本，2bytes
     */
    private final short minorVersion = 1;
    private long totalPage;
    private long rootPosition;
    private int pageSize;
    private int entrySize;
    private int degree;
    private Path path;
    private MetaNode metaNode;
    private Configuration conf;

    public MetaPage(Path path, MetaNode metaNode) throws IOException {
        this.path = path;
        this.metaNode = metaNode;
        this.conf = this.metaNode.getConfiguration();
        persistence();
    }

    public void persistence() throws IOException {
        // 判断数据库文件是否存在
        if (Files.exists(this.path)) {
            readMetaPage();
        } else {
            createMetaPage();
        }
    }

    /**
     * MAGIC_NUMBER
     * 写入磁盘文件
     */
    private void createMetaPage() throws IOException {
        Files.createFile(path);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
        channel.position(0);

        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        // 6B
        buffer.put(ByteUtils.getBytesUTF8(this.MAGIC));
        // 2B
        buffer.putShort(majorVersion);
        // 2B
        buffer.putShort(minorVersion);
        // 4B
        buffer.putInt(conf.getPageSize());
        // 8B
        buffer.putLong(0L);
        // 4B
        buffer.putInt(conf.getEntrySize());
        // 4B
        buffer.putInt(conf.getDegree());
        // total = 30B
        channel.write(buffer);
    }

    /**
     * 从数据库文件中读取信息
     */
    private void readMetaPage() throws IOException {
        FileChannel channel = FileChannel.open(path);
        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(conf.getPageSize());
        channel.read(buffer);
        buffer.flip();
        String magic = ByteUtils.toUTF8String(buffer, 6);
        if (!StringUtils.equals(magic, this.MAGIC)) {
            throw new IllegalArgumentException("Invalid disk file!");
        }
        if (majorVersion != buffer.getShort() || minorVersion != buffer.getShort()) {
            throw new IllegalArgumentException("Invalid version file!");
        }
        this.pageSize = buffer.getInt();
        this.totalPage = buffer.getLong();
        this.entrySize = buffer.getInt();
        this.degree = buffer.getInt();

        this.conf.setPageSize(pageSize);
        this.conf.setEntrySize(entrySize);
        this.conf.setDegree(degree);
        this.metaNode.setPageTotal(totalPage);
    }
}
