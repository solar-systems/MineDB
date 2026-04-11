package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.BalanceTree;
import cn.abelib.minedb.index.wal.WalEntry;
import cn.abelib.minedb.index.wal.WalLog;
import cn.abelib.minedb.utils.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 事务对象
 *
 * <p>提供简化版事务支持：
 * <ul>
 *   <li>事务内操作先缓存在内存</li>
 *   <li>提交时批量写入 WAL 和 B+ 树</li>
 *   <li>支持手动回滚</li>
 *   <li>未提交的事务自动回滚</li>
 * </ul>
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class Transaction implements AutoCloseable {
    /** 事务状态 */
    public enum State {
        ACTIVE,     // 活跃状态
        COMMITTED,  // 已提交
        ROLLED_BACK // 已回滚
    }

    private final long txId;
    private final BalanceTree tree;
    private final WalLog walLog;
    private final List<Operation> operations;
    private State state;
    private final Object lock;

    /**
     * 操作记录
     */
    private static class Operation {
        final WalEntry.Type type;
        final String key;
        final String value;

        Operation(WalEntry.Type type, String key, String value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }

    /**
     * 创建事务
     *
     * @param txId 事务 ID
     * @param tree B+ 树
     * @param walLog WAL 日志
     */
    Transaction(long txId, BalanceTree tree, WalLog walLog) throws IOException {
        this.txId = txId;
        this.tree = tree;
        this.walLog = walLog;
        this.operations = new ArrayList<>();
        this.state = State.ACTIVE;
        this.lock = new Object();

        // 写入 BEGIN 标记
        if (walLog != null) {
            walLog.logTxBegin(txId);
        }
    }

    /**
     * 插入或更新键值对
     *
     * @param key 键
     * @param value 值
     */
    public void put(String key, String value) {
        checkActive();
        if (key == null || value == null) {
            throw new NullPointerException("Key and value cannot be null");
        }

        synchronized (lock) {
            operations.add(new Operation(WalEntry.Type.PUT, key, value));
        }
    }

    /**
     * 删除键值对
     *
     * @param key 键
     */
    public void delete(String key) {
        checkActive();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        synchronized (lock) {
            operations.add(new Operation(WalEntry.Type.DELETE, key, null));
        }
    }

    /**
     * 提交事务
     *
     * @throws Exception 提交失败
     */
    public void commit() throws Exception {
        synchronized (lock) {
            if (state != State.ACTIVE) {
                throw new IllegalStateException("Transaction is not active: " + state);
            }

            try {
                // 1. 写入所有操作到 WAL
                if (walLog != null) {
                    for (Operation op : operations) {
                        if (op.type == WalEntry.Type.PUT) {
                            walLog.logTxPut(txId, op.key, op.value);
                        } else if (op.type == WalEntry.Type.DELETE) {
                            walLog.logTxDelete(txId, op.key);
                        }
                    }

                    // 2. 写入 COMMIT 标记
                    walLog.logTxCommit(txId);
                }

                // 3. 应用操作到 B+ 树
                for (Operation op : operations) {
                    if (op.type == WalEntry.Type.PUT) {
                        tree.insert(op.key, op.value);
                    } else if (op.type == WalEntry.Type.DELETE) {
                        tree.delete(op.key);
                    }
                }

                state = State.COMMITTED;
                operations.clear();

            } catch (Exception e) {
                // 提交失败，尝试回滚
                state = State.ROLLED_BACK;
                operations.clear();
                throw new Exception("Transaction commit failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        synchronized (lock) {
            if (state != State.ACTIVE) {
                return; // 已经不是活跃状态，直接返回
            }

            try {
                // 写入 ROLLBACK 标记
                if (walLog != null) {
                    walLog.logTxRollback(txId);
                }
            } catch (IOException e) {
                // 忽略 WAL 写入错误
            }

            state = State.ROLLED_BACK;
            operations.clear();
        }
    }

    /**
     * 检查事务是否活跃
     */
    private void checkActive() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active: " + state);
        }
    }

    /**
     * 获取事务 ID
     */
    public long getTxId() {
        return txId;
    }

    /**
     * 获取事务状态
     */
    public State getState() {
        return state;
    }

    /**
     * 是否已提交
     */
    public boolean isCommitted() {
        return state == State.COMMITTED;
    }

    /**
     * 是否已回滚
     */
    public boolean isRolledBack() {
        return state == State.ROLLED_BACK;
    }

    /**
     * 获取操作数量
     */
    public int getOperationCount() {
        return operations.size();
    }

    @Override
    public void close() {
        // 未提交的事务自动回滚
        if (state == State.ACTIVE) {
            rollback();
        }
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txId=" + txId +
                ", state=" + state +
                ", operations=" + operations.size() +
                '}';
    }
}
