package cn.abelib.minedb.index.wal;

import cn.abelib.minedb.index.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WAL 预写日志管理器
 *
 * <p>提供以下保证：
 * <ul>
 *   <li>写入持久化：每次操作先写入 WAL，再修改内存数据</li>
 *   <li>崩溃恢复：重启时从 WAL 恢复未提交的数据</li>
 *   <li>校验保护：每个条目包含校验和，防止数据损坏</li>
 * </ul>
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class WalLog {
    private static final String WAL_SUFFIX = ".wal";

    private final Path walPath;
    private FileChannel channel;
    private final AtomicLong sequence;
    private final Lock writeLock;
    private volatile boolean closed;

    public WalLog(Configuration configuration) throws IOException {
        String dbName = configuration.getDbName();
        this.walPath = Paths.get(dbName + WAL_SUFFIX);
        this.sequence = new AtomicLong(0);
        this.writeLock = new ReentrantLock();
        this.closed = false;

        init();
    }

    /**
     * 初始化 WAL 文件
     */
    private void init() throws IOException {
        if (Files.exists(walPath)) {
            // 文件存在，读取最大序列号
            sequence.set(readMaxSequence());
        } else {
            // 创建新文件
            channel = FileChannel.open(walPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ);
        }
    }

    /**
     * 从现有 WAL 文件读取最大序列号
     */
    private long readMaxSequence() throws IOException {
        long maxSeq = 0;

        try (FileChannel readChannel = FileChannel.open(walPath, StandardOpenOption.READ)) {
            ByteBuffer headerBuffer = ByteBuffer.allocate(WalEntry.HEADER_SIZE);

            while (readChannel.position() < readChannel.size()) {
                headerBuffer.clear();

                // 读取头部
                int bytesRead = readChannel.read(headerBuffer);
                if (bytesRead < WalEntry.HEADER_SIZE) {
                    break;
                }

                headerBuffer.flip();

                // 解析头部获取 key/value 长度
                int magic = headerBuffer.getInt();
                if (magic != 0x57414C00) {
                    break;
                }

                headerBuffer.get(); // type
                int keyLen = headerBuffer.getInt();
                int valueLen = headerBuffer.getInt();
                headerBuffer.getInt(); // checksum
                long seq = headerBuffer.getLong();

                maxSeq = Math.max(maxSeq, seq);

                // 跳过 key/value 数据
                long skipBytes = keyLen + valueLen;
                if (skipBytes > 0) {
                    readChannel.position(readChannel.position() + skipBytes);
                }
            }
        }

        return maxSeq;
    }

    /**
     * 写入 PUT 操作
     */
    public void logPut(String key, String value) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.PUT, key, value, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入 DELETE 操作
     */
    public void logDelete(String key) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.DELETE, key, null, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入 SYNC 检查点
     */
    public void logSync() throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.SYNC, null, null, sequence.incrementAndGet());
        writeEntry(entry);
    }

    // ==================== 事务相关日志 ====================

    /**
     * 写入事务开始标记
     */
    public void logTxBegin(long txId) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.TX_BEGIN, "tx:" + txId, null, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入事务内的 PUT 操作
     */
    public void logTxPut(long txId, String key, String value) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.PUT, key, value, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入事务内的 DELETE 操作
     */
    public void logTxDelete(long txId, String key) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.DELETE, key, null, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入事务提交标记
     */
    public void logTxCommit(long txId) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.TX_COMMIT, "tx:" + txId, null, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入事务回滚标记
     */
    public void logTxRollback(long txId) throws IOException {
        checkClosed();
        WalEntry entry = new WalEntry(WalEntry.Type.TX_ROLLBACK, "tx:" + txId, null, sequence.incrementAndGet());
        writeEntry(entry);
    }

    /**
     * 写入条目到 WAL 文件
     */
    private void writeEntry(WalEntry entry) throws IOException {
        writeLock.lock();
        try {
            ensureChannelOpen();

            ByteBuffer buffer = entry.toByteBuffer();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true); // 确保写入磁盘
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 确保文件通道已打开
     */
    private void ensureChannelOpen() throws IOException {
        if (channel == null || !channel.isOpen()) {
            channel = FileChannel.open(walPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
        }
    }

    /**
     * 读取所有 WAL 条目用于恢复
     */
    public List<WalEntry> readAllEntries() throws IOException {
        List<WalEntry> entries = new ArrayList<>();

        if (!Files.exists(walPath)) {
            return entries;
        }

        try (FileChannel readChannel = FileChannel.open(walPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) readChannel.size());
            readChannel.read(buffer);
            buffer.flip();

            while (buffer.remaining() >= WalEntry.HEADER_SIZE) {
                int startPos = buffer.position();

                try {
                    WalEntry entry = WalEntry.fromByteBuffer(buffer);

                    if (!entry.validate()) {
                        // 校验失败，跳过此条目
                        System.err.println("WAL entry checksum validation failed at position: " + startPos);
                        continue;
                    }

                    entries.add(entry);
                } catch (Exception e) {
                    // 解析失败，可能到达文件末尾或数据损坏
                    System.err.println("Failed to parse WAL entry at position " + startPos + ": " + e.getMessage());
                    break;
                }
            }
        }

        return entries;
    }

    /**
     * 清空 WAL 日志
     * 在成功 checkpoint 后调用
     */
    public void clear() throws IOException {
        writeLock.lock();
        try {
            closeChannel();

            if (Files.exists(walPath)) {
                Files.delete(walPath);
            }

            // 创建新的空文件
            channel = FileChannel.open(walPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ);

            sequence.set(0);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 截断 WAL 到指定序列号
     */
    public void truncate(long fromSequence) throws IOException {
        writeLock.lock();
        try {
            List<WalEntry> allEntries = readAllEntries();
            closeChannel();

            // 重新写入序列号大于 fromSequence 的条目
            channel = FileChannel.open(walPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.TRUNCATE_EXISTING);

            for (WalEntry entry : allEntries) {
                if (entry.getSequence() > fromSequence) {
                    ByteBuffer buffer = entry.toByteBuffer();
                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                }
            }

            channel.force(true);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 获取当前序列号
     */
    public long getSequence() {
        return sequence.get();
    }

    /**
     * 获取 WAL 文件大小
     */
    public long getFileSize() throws IOException {
        if (Files.exists(walPath)) {
            return Files.size(walPath);
        }
        return 0;
    }

    /**
     * 检查 WAL 文件是否存在
     */
    public boolean exists() {
        return Files.exists(walPath);
    }

    /**
     * 关闭 WAL
     */
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        closeChannel();
    }

    /**
     * 关闭文件通道
     */
    private void closeChannel() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.force(true);
                channel.close();
            } catch (AsynchronousCloseException e) {
                // 忽略异步关闭异常
            } catch (IOException e) {
                System.err.println("Warning: Failed to close WAL channel: " + e.getMessage());
            }
        }
        channel = null;
    }

    /**
     * 检查是否已关闭
     */
    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("WAL log is closed");
        }
    }

    /**
     * 获取 WAL 文件路径
     */
    public Path getWalPath() {
        return walPath;
    }

    /**
     * 是否已关闭
     */
    public boolean isClosed() {
        return closed;
    }
}
