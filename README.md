# MineDB

MineDB 是一个基于 B+ 树的磁盘存储引擎，提供简洁的 Key-Value 数据库接口。

## 特性

- **B+ 树索引**: 高效的键值存储和检索，支持插入、删除、范围查询
- **磁盘持久化**: 数据持久化到磁盘，支持重启恢复
- **WAL 预写日志**: 崩溃恢复保障，确保数据不丢失
- **LRU 页缓存**: 支持缓存淘汰，防止内存溢出
- **自动刷盘**: 定时/阈值触发自动持久化
- **批量操作**: 高效的批量读写接口
- **迭代器遍历**: 支持有序遍历所有键值对
- **事务支持**: 简化版事务，保证原子性
- **溢出页支持**: 支持存储大记录，超过页面大小的数据自动使用溢出页存储
- **自由页管理**: 空间分配和回收，避免文件无限增长
- **线程安全**: 读写锁支持并发读，写操作互斥
- **范围查询**: 支持范围查询和前缀查询
- **简洁 API**: 简单易用的 KV 数据库接口

## 项目结构

```
src/main/java/cn/abelib/minedb/
├── index/
│   ├── BalanceTree.java        # B+ 树实现（支持节点合并再平衡）
│   ├── TreeNode.java           # 树节点
│   ├── MetaNode.java           # 元数据节点
│   ├── Configuration.java      # 配置类
│   ├── PageUtils.java          # 页面工具类
│   ├── PageCache.java          # 页缓存（LRU 淘汰）
│   ├── autoflush/
│   │   └── AutoFlushManager.java # 自动刷盘管理器
│   ├── wal/
│   │   ├── WalEntry.java       # WAL 日志条目
│   │   └── WalLog.java         # WAL 日志管理器
│   └── fs/
│       ├── Page.java           # 磁盘页结构
│       ├── Record.java         # 磁盘记录结构（支持溢出标记）
│       ├── PageLoader.java     # 磁盘页加载器
│       ├── OverFlowPage.java   # 溢出页（链式存储大记录）
│       └── FreePageList.java   # 自由页链表管理
├── kv/
│   ├── Db.java                 # KV 数据库接口
│   ├── MineDb.java             # KV 数据库实现
│   ├── DbIterator.java         # 数据库迭代器
│   ├── Transaction.java        # 事务对象
│   └── TransactionManager.java # 事务管理器
└── utils/
    ├── ByteUtils.java          # 字节工具类
    └── KeyValue.java           # 键值对
```

## 快速开始

### 添加依赖

```xml
<dependency>
    <groupId>cn.abelib</groupId>
    <artifactId>minedb</artifactId>
    <version>1.2.0</version>
</dependency>
```

### 基本使用

```java
import cn.abelib.minedb.kv.Db;
import cn.abelib.minedb.kv.MineDb;
import cn.abelib.minedb.utils.KeyValue;

// 创建数据库实例
Db db = new MineDb("mydb.db");

// 插入数据
db.put("key1", "value1");
db.put("key2", "value2");

// 获取数据
String value = db.get("key1"); // "value1"

// 检查键是否存在
boolean exists = db.contains("key1"); // true

// 范围查询
List<KeyValue> range = db.range("a", "z");

// 前缀查询
List<KeyValue> prefix = db.prefix("key");

// 删除数据
boolean deleted = db.delete("key1");

// 获取数据量
long size = db.size();

// 同步数据到磁盘
db.sync();

// 关闭数据库
db.close();
```

### 批量操作

```java
// 批量插入
Map<String, String> data = new HashMap<>();
data.put("key1", "value1");
data.put("key2", "value2");
int count = db.batchPut(data);

// 批量获取
Map<String, String> result = db.batchGet(Arrays.asList("key1", "key2"));

// 批量删除
int deleted = db.batchDelete(Arrays.asList("key1", "key2"));
```

### 迭代器遍历

```java
// 遍历所有键值对
Iterator<KeyValue> it = db.iterator();
while (it.hasNext()) {
    KeyValue kv = it.next();
    System.out.println(kv.getKey() + " = " + kv.getValue());
}

// 从指定键开始遍历
Iterator<KeyValue> it = db.iterator("key100");

// 跳过元素
DbIterator dbIt = (DbIterator) db.iterator();
dbIt.skip(100);  // 跳过前 100 个
```

### 事务操作

```java
// 方式一：手动管理
Transaction tx = db.beginTransaction();
try {
    tx.put("key1", "value1");
    tx.put("key2", "value2");
    tx.commit();
} catch (Exception e) {
    tx.rollback();
}

// 方式二：try-with-resources（推荐）
try (Transaction tx = db.beginTransaction()) {
    tx.put("key1", "value1");
    tx.commit();  // 必须显式提交
}  // 未提交自动回滚
```

### 使用配置

```java
import cn.abelib.minedb.index.Configuration;

// 创建自定义配置
Configuration config = new Configuration(
    16 * 1024,  // 页面大小 16KB
    128,        // 头部大小 128 bytes
    1024,       // 子节点指针域大小 1024 bytes
    1000,       // 缓存最大页数
    true        // 启用自动刷盘
);

// 设置自动刷盘参数
config.setAutoFlushIntervalMs(5000);    // 5秒刷盘一次
config.setDirtyPageThreshold(100);       // 100个脏页触发刷盘
config.setWalSizeThreshold(10 * 1024 * 1024); // WAL 10MB 触发刷盘

// 使用配置创建数据库
Db db = new MineDb("mydb.db", config);
```

## API 文档

### Db 接口

| 方法 | 说明 |
|------|------|
| `put(key, value)` | 插入或更新键值对 |
| `get(key)` | 获取键对应的值 |
| `delete(key)` | 删除键值对 |
| `contains(key)` | 检查键是否存在 |
| `range(start, end)` | 范围查询 |
| `prefix(prefix)` | 前缀查询 |
| `size()` | 获取键值对数量 |
| `clear()` | 清空数据库 |
| `sync()` | 同步数据到磁盘 |
| `batchPut(map)` | 批量插入 |
| `batchGet(keys)` | 批量获取 |
| `batchDelete(keys)` | 批量删除 |
| `iterator()` | 获取迭代器 |
| `iterator(startKey)` | 从指定键开始迭代 |
| `beginTransaction()` | 开启事务 |

## 技术细节

### B+ 树结构

- 叶子节点存储实际的键值对数据
- 非叶子节点存储索引信息
- 叶子节点通过链表连接，支持高效范围查询
- 删除操作支持节点合并和再平衡

### WAL 预写日志

- 每次写操作先记录到 WAL
- 支持三种操作类型：PUT、DELETE、SYNC
- 包含校验和防止数据损坏
- 崩溃恢复时自动重放未提交的操作

### LRU 页缓存

- 每个数据库实例独立缓存
- 使用 LinkedHashMap 实现访问顺序
- 脏页优先刷盘后再淘汰
- 支持命中率统计

### 自动刷盘

- 定时刷盘：按固定间隔自动持久化
- 阈值刷盘：脏页数量/WAL大小达到阈值触发
- 可配置刷盘参数

### 事务支持

- 串行隔离级别
- 事务内操作缓存在内存
- 提交时批量写入 WAL 和 B+ 树
- 支持 try-with-resources 自动回滚

### 磁盘存储

- 每个页面大小默认 16KB
- 元数据页存储数据库配置和根节点位置
- 溢出页用于存储超过页面可用空间的记录
- 自由页链表管理空间分配和回收

### 线程安全

- BalanceTree 使用 `ReentrantReadWriteLock` 实现线程安全
- 读操作共享锁，支持并发读
- 写操作独占锁，保证数据一致性
- PageCache 使用读写锁保护缓存操作

## 构建和测试

```bash
# 编译项目
mvn compile

# 运行测试
mvn test

# 打包
mvn package
```

### 测试覆盖

- 测试总数：200+ 个
- 测试覆盖所有源文件
- 测试文件位于对应包路径下

## 版本历史

- **1.2.0** - 核心功能完善
  - WAL 预写日志（崩溃恢复）
  - LRU 缓存淘汰机制
  - 自动刷盘（定时/阈值）
  - 批量操作 API
  - 迭代器遍历
  - 简化版事务支持

- **1.1.0** - 功能增强
  - B+ 树删除操作节点合并和再平衡
  - 线程安全实现（读写锁）
  - 溢出页链式存储大记录
  - 自由页链表空间管理
  - 完整测试覆盖

- **1.0.0** - 初始版本
  - B+ 树索引实现
  - 磁盘持久化
  - 溢出页支持
  - KV 数据库接口

## 许可证

MIT License

## 作者

abel.huang
