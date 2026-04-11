package cn.abelib.minedb.kv;

import cn.abelib.minedb.index.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * 事务测试
 *
 * @author abel.huang
 * @date 2026/4/11
 */
public class TransactionTest {
    private Db db;

    @Before
    public void init() throws IOException {
        cleanup();
        Configuration config = new Configuration();
        config.setDbName("test_tx.db");
        config.setAutoFlushEnabled(false);
        db = new MineDb("test_tx.db", config);
    }

    @After
    public void cleanup() throws IOException {
        if (db != null) {
            db.close();
        }
        Files.deleteIfExists(Paths.get("test_tx.db"));
        Files.deleteIfExists(Paths.get("test_tx.db.wal"));
    }

    @Test
    public void testSimpleTransaction() throws Exception {
        Transaction tx = db.beginTransaction();

        tx.put("key1", "value1");
        tx.put("key2", "value2");

        assertEquals(2, tx.getOperationCount());
        assertEquals(Transaction.State.ACTIVE, tx.getState());

        tx.commit();

        assertEquals(Transaction.State.COMMITTED, tx.getState());
        assertEquals("value1", db.get("key1"));
        assertEquals("value2", db.get("key2"));
    }

    @Test
    public void testTransactionWithDelete() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");

        Transaction tx = db.beginTransaction();

        tx.delete("key1");
        tx.put("key3", "value3");

        tx.commit();

        assertNull(db.get("key1"));
        assertEquals("value2", db.get("key2"));
        assertEquals("value3", db.get("key3"));
    }

    @Test
    public void testTransactionRollback() throws Exception {
        db.put("key1", "original");

        Transaction tx = db.beginTransaction();

        tx.put("key1", "modified");
        tx.put("key2", "new");

        tx.rollback();

        assertEquals(Transaction.State.ROLLED_BACK, tx.getState());
        // 原数据不变
        assertEquals("original", db.get("key1"));
        assertNull(db.get("key2"));
    }

    @Test
    public void testAutoRollbackOnClose() throws Exception {
        Transaction tx = db.beginTransaction();

        tx.put("key1", "value1");

        // 未提交，close 时自动回滚
        tx.close();

        assertEquals(Transaction.State.ROLLED_BACK, tx.getState());
        assertNull(db.get("key1"));
    }

    @Test
    public void testTryWithResources() throws Exception {
        try (Transaction tx = db.beginTransaction()) {
            tx.put("key1", "value1");
            // 未调用 commit，自动回滚
        }

        assertNull(db.get("key1"));
    }

    @Test
    public void testTryWithResourcesCommit() throws Exception {
        try (Transaction tx = db.beginTransaction()) {
            tx.put("key1", "value1");
            tx.commit();
        }

        assertEquals("value1", db.get("key1"));
    }

    @Test
    public void testTransactionState() throws Exception {
        Transaction tx = db.beginTransaction();

        assertEquals(Transaction.State.ACTIVE, tx.getState());
        assertFalse(tx.isCommitted());
        assertFalse(tx.isRolledBack());

        tx.commit();

        assertTrue(tx.isCommitted());
        assertFalse(tx.isRolledBack());
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleCommit() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key1", "value1");

        tx.commit();
        tx.commit(); // 应该抛出异常
    }

    @Test
    public void testDoubleRollback() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key1", "value1");

        tx.rollback();
        tx.rollback(); // 重复回滚应该安全
    }

    @Test
    public void testRollbackAfterCommit() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key1", "value1");

        tx.commit();

        // 提交后回滚应该无效
        tx.rollback();
        assertEquals(Transaction.State.COMMITTED, tx.getState());
    }

    @Test(expected = IllegalStateException.class)
    public void testPutAfterCommit() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key1", "value1");
        tx.commit();

        tx.put("key2", "value2"); // 应该抛出异常
    }

    @Test(expected = IllegalStateException.class)
    public void testPutAfterRollback() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key1", "value1");
        tx.rollback();

        tx.put("key2", "value2"); // 应该抛出异常
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullKey() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put(null, "value");
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullValue() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key", null);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteNullKey() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.delete(null);
    }

    @Test
    public void testTransactionManager() throws Exception {
        TransactionManager tm = ((MineDb) db).getTransactionManager();

        assertFalse(tm.hasActiveTransaction());

        Transaction tx = tm.beginTransaction();

        assertTrue(tm.hasActiveTransaction());

        tx.commit();

        tm.cleanup();
        assertFalse(tm.hasActiveTransaction());
    }

    @Test(expected = IllegalStateException.class)
    public void testOnlyOneActiveTransaction() throws Exception {
        TransactionManager tm = ((MineDb) db).getTransactionManager();

        Transaction tx1 = tm.beginTransaction();
        Transaction tx2 = tm.beginTransaction(); // 应该抛出异常
    }

    @Test
    public void testSequentialTransactions() throws Exception {
        // 第一个事务
        Transaction tx1 = db.beginTransaction();
        tx1.put("key1", "value1");
        tx1.commit();

        // 第二个事务
        Transaction tx2 = db.beginTransaction();
        tx2.put("key2", "value2");
        tx2.commit();

        assertEquals("value1", db.get("key1"));
        assertEquals("value2", db.get("key2"));
    }

    @Test
    public void testTransactionIdIncrement() throws Exception {
        TransactionManager tm = ((MineDb) db).getTransactionManager();

        long txId1 = tm.getNextTxId();

        Transaction tx = db.beginTransaction();
        long txId = tx.getTxId();
        tx.commit();

        long txId2 = tm.getNextTxId();

        assertEquals(txId1, txId);
        assertTrue(txId2 > txId1);
    }

    @Test
    public void testMultipleOperations() throws Exception {
        Transaction tx = db.beginTransaction();

        for (int i = 0; i < 100; i++) {
            tx.put("key" + i, "value" + i);
        }

        assertEquals(100, tx.getOperationCount());

        tx.commit();

        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, db.get("key" + i));
        }
    }

    @Test
    public void testTransactionToString() throws Exception {
        Transaction tx = db.beginTransaction();
        tx.put("key1", "value1");

        String str = tx.toString();
        assertTrue(str.contains("txId="));
        assertTrue(str.contains("state=ACTIVE"));
        assertTrue(str.contains("operations=1"));
    }

    @Test(expected = IllegalStateException.class)
    public void testBeginTransactionOnClosedDb() throws Exception {
        db.close();
        db.beginTransaction();
    }
}
