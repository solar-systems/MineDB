# MineDB

MineDB 是一个基于 B+ 树的磁盘存储引擎，提供简洁的 Key-Value 数据库接口。

## 特性

- **B+ 树索引**: 高效的键值存储和检索，支持插入、删除、范围查询
- **磁盘持久化**: 数据持久化到磁盘，支持重启恢复
- **溢出页支持**: 支持存储大记录，超过页面大小的数据自动使用溢出页存储
- **自由页管理**: 空间分配和回收，避免文件无限增长
- **页缓存**: 全局页缓存管理，区分脏页和净页
- **线程安全**: 读写锁支持并发读，写操作互斥
- **范围查询**: 支持范围查询和前缀查询
- **简洁 API**: 简单易用的 KV 数据库接口

## 项目结构

```
src/main/java/cn/abelib/minedb/
├── index/
│   ├── BalanceTree.java      # B+ 树实现（支持节点合并再平衡）
│   ├── TreeNode.java         # 树节点
│   ├── MetaNode.java         # 元数据节点
│   ├── Configuration.java    # 配置类
│   ├── PageUtils.java        # 页面工具类
│   ├── GlobalPageCache.java  # 全局页缓存（线程安全）
│   └── fs/
│       ├── Page.java         # 磁盘页结构
│       ├── Record.java       # 磁盘记录结构（支持溢出标记）
│       ├── PageLoader.java   # 磁盘页加载器
│       ├── OverFlowPage.java # 溢出页（链式存储大记录）
│       └── FreePageList.java # 自由页链表管理
├── kv/
│   ├── Db.java               # KV 数据库接口
│   └── MineDb.java           # KV 数据库实现
└── utils/
    ├── ByteUtils.java        # 字节工具类
    └── KeyValue.java         # 键值对
```

## 快速开始

### 添加依赖

```xml
<dependency>
    <groupId>cn.abelib</groupId>
    <artifactId>minedb</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 基本使用

```java
import cn.abelib.minedb.kv.Db;
import cn.abelib.minedb.kv.MineDb;

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

### 使用配置

```java
import cn.abelib.minedb.index.Configuration;

// 创建自定义配置
Configuration config = new Configuration(
    16 * 1024,  // 页面大小 16KB
    128,        // 头部大小 128 bytes
    1024        // 子节点指针域大小 1024 bytes
);

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
| `close()` | 关闭数据库 |

## 技术细节

### B+ 树结构

- 叶子节点存储实际的键值对数据
- 非叶子节点存储索引信息
- 叶子节点通过链表连接，支持高效范围查询
- 删除操作支持节点合并和再平衡

### 磁盘存储

- 每个页面大小默认 16KB
- 元数据页存储数据库配置和根节点位置
- 溢出页用于存储超过页面可用空间的记录
- 自由页链表管理空间分配和回收

### 页缓存

- 全局单例缓存管理
- 区分脏页（已修改）和净页（未修改）
- 关闭时自动刷新脏页到磁盘
- 读写锁保护并发访问

### 线程安全

- BalanceTree 使用 `ReentrantReadWriteLock` 实现线程安全
- 读操作共享锁，支持并发读
- 写操作独占锁，保证数据一致性
- GlobalPageCache 使用读写锁保护缓存操作

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

- 测试总数：139 个
- 测试覆盖所有源文件
- 测试文件位于对应包路径下

## 版本历史

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
