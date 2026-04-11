package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.wal.WalLog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器
 *
 * <p>负责事务的创建和管理，使用串行隔离级别（一次只允许一个活跃事务）。
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class TransactionManager {
    private final BalanceTree tree;
    private final WalLog walLog;
    private final AtomicLong txIdGenerator;
    private final ReentrantLock txLock;
    private volatile Transaction activeTransaction;

    /**
     * 创建事务管理器
     *
     * @param tree B+ 树
     * @param walLog WAL 日志
     */
    public TransactionManager(BalanceTree tree, WalLog walLog) {
        this.tree = tree;
        this.walLog = walLog;
        this.txIdGenerator = new AtomicLong(0);
        this.txLock = new ReentrantLock();
        this.activeTransaction = null;
    }

    /**
     * 开启新事务
     *
     * @return 事务对象
     * @throws IOException 创建事务失败
     */
    public Transaction beginTransaction() throws IOException {
        txLock.lock();
        try {
            // 确保没有其他活跃事务
            if (activeTransaction != null && activeTransaction.getState() == Transaction.State.ACTIVE) {
                throw new IllegalStateException("Another transaction is already active. " +
                        "Current transaction: txId=" + activeTransaction.getTxId());
            }

            long txId = txIdGenerator.incrementAndGet();
            Transaction tx = new Transaction(txId, tree, walLog);
            activeTransaction = tx;
            return tx;
        } finally {
            txLock.unlock();
        }
    }

    /**
     * 尝试开启新事务（非阻塞）
     *
     * @return 事务对象，如果有活跃事务则返回 null
     * @throws IOException 创建事务失败
     */
    public Transaction tryBeginTransaction() throws IOException {
        if (!txLock.tryLock()) {
            return null;
        }
        try {
            if (activeTransaction != null && activeTransaction.getState() == Transaction.State.ACTIVE) {
                return null;
            }

            long txId = txIdGenerator.incrementAndGet();
            Transaction tx = new Transaction(txId, tree, walLog);
            activeTransaction = tx;
            return tx;
        } finally {
            txLock.unlock();
        }
    }

    /**
     * 获取当前活跃事务
     *
     * @return 活跃事务，没有则返回 null
     */
    public Transaction getActiveTransaction() {
        return activeTransaction;
    }

    /**
     * 检查是否有活跃事务
     *
     * @return true 如果有活跃事务
     */
    public boolean hasActiveTransaction() {
        return activeTransaction != null && activeTransaction.getState() == Transaction.State.ACTIVE;
    }

    /**
     * 获取下一个事务 ID
     */
    public long getNextTxId() {
        return txIdGenerator.get() + 1;
    }

    /**
     * 获取已创建的事务数量
     */
    public long getTransactionCount() {
        return txIdGenerator.get();
    }

    /**
     * 清理已完成的事务引用
     */
    public void cleanup() {
        if (activeTransaction != null && activeTransaction.getState() != Transaction.State.ACTIVE) {
            activeTransaction = null;
        }
    }
}
