package cn.abelib.minedb.kv;

import cn.abelib.minedb.utils.KeyValue;

import java.io.Closeable;
import java.util.List;

/**
 * KV 数据库接口
 *
 * <p>提供简洁的键值存储操作 API
 *
 * @author abel.huang
 * @version 1.0
 * @date 2026/4/10 上午 4:00
 */
public interface Db extends Closeable {

    /**
     * 插入或更新键值对
     *
     * @param key 键
     * @param value 值
     * @throws Exception 操作异常
     */
    void put(String key, String value) throws Exception;

    /**
     * 获取键对应的值
     *
     * @param key 键
     * @return 值，不存在返回 null
     * @throws Exception 操作异常
     */
    String get(String key) throws Exception;

    /**
     * 删除键值对
     *
     * @param key 键
     * @return true 如果删除成功，false 如果 key 不存在
     * @throws Exception 操作异常
     */
    boolean delete(String key) throws Exception;

    /**
     * 检查 key 是否存在
     *
     * @param key 键
     * @return true 如果存在
     * @throws Exception 操作异常
     */
    boolean contains(String key) throws Exception;

    /**
     * 范围查询 [start, end)
     *
     * @param start 起始键（包含）
     * @param end 结束键（不包含）
     * @return 范围内的键值对列表
     * @throws Exception 操作异常
     */
    List<KeyValue> range(String start, String end) throws Exception;

    /**
     * 前缀查询
     *
     * @param prefix 键前缀
     * @return 匹配的键值对列表
     * @throws Exception 操作异常
     */
    List<KeyValue> prefix(String prefix) throws Exception;

    /**
     * 获取键值对数量
     *
     * @return 键值对数量
     */
    long size();

    /**
     * 清空数据库
     *
     * @throws Exception 操作异常
     */
    void clear() throws Exception;

    /**
     * 同步数据到磁盘
     *
     * @throws Exception 操作异常
     */
    void sync() throws Exception;
}
