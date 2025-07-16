package com.wenziyue.redis.utils;

import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import com.alibaba.fastjson.JSON;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Redis 工具类，封装常用操作
 * @author wenziyue
 */
@Component
@RequiredArgsConstructor
public class RedisUtils {

    private final RedisTemplate<String, Object> redisTemplate;

    // 专用于 Stream 操作的 RedisTemplate，key 和 value 都是 String 类型
    private final RedisTemplate<String, String> stringRedisTemplate;

    private final RedisScript<Long> incrementWithExpire;
    private final RedisScript<Long> sAddAndExpire;
    private final RedisScript<Long> hSetAllAndExpire;
    private final RedisScript<Long> lPushAndExpire;
    private final RedisScript<Long> rPushAndExpire;
    private final RedisScript<Long> zAddAndExpire;
    private final RedisScript<Long> setIfAbsentAndExpire;

    // ======================== String ========================

    /**
     * 设置 key 对应的值
     */
    public void set(String key, Object value) {
        redisTemplate.opsForValue().set(key, value);
    }

    /**
     * 设置 key 对应的值，并设置过期时间
     */
    public void set(String key, Object value, long timeout,  TimeUnit timeUnit) {
        redisTemplate.opsForValue().set(key, value, timeout, timeUnit);
    }

    /**
     * 获取 key 对应的值
     */
    public Object get(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    /**
     * 删除 key
     */
    public boolean delete(String key) {
        return Boolean.TRUE.equals(redisTemplate.delete(key));
    }

    /**
     * 设置过期时间
     */
    public boolean expire(String key, long timeout, TimeUnit timeUnit) {
        return Boolean.TRUE.equals(redisTemplate.expire(key, timeout, timeUnit));
    }

    /**
     * 获取剩余过期时间（秒）,永不过期用-1，key不存在用0
     */
    public long getExpire(String key) {
        if (Boolean.FALSE.equals(redisTemplate.hasKey(key))) {
            return 0L;
        }
        Long expire = redisTemplate.getExpire(key, TimeUnit.SECONDS);
        return expire != null ? expire : -1L;
    }


    /**
     * 如果key不存在，则设置值并设置过期时间;如果 key 已存在，则不做任何操作；
     * 可用于简单的分布式锁
     */
    public boolean setIfAbsentAndExpire(String key, String value, long timeout, TimeUnit unit) {
        long timeoutSeconds = unit.toSeconds(timeout);
        Long result = redisTemplate.execute(
                setIfAbsentAndExpire,
                Collections.singletonList(key),
                value,
                timeoutSeconds
        );
        return result != null && result == 1;
    }

    /**
     * 判断 key 是否存在
     */
    public boolean hasKey(String key) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    /**
     * 原子自增，如果key不存在，则创建，value为delta
     * @param key Redis Key
     * @param delta 增加的值（允许负数）
     * @return 增加后的结果，操作失败返回null
     */
    public Long increment(String key, long delta) {
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /**
     * 原子自增，并设置过期时间
     * @param key Redis Key
     * @param delta 增加的值（允许负数）
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return 增加后的结果,失败返回null
     */
    public Long increment(String key, long delta, long timeout, TimeUnit unit) {
        long timeoutSeconds = unit.toSeconds(timeout);
        return redisTemplate.execute(incrementWithExpire, Collections.singletonList(key), delta, timeoutSeconds);
    }

    // ======================== Hash ========================

    public void hSet(String key, String field, Object value) {
        redisTemplate.opsForHash().put(key, field, value);
    }

    public long hSet(String key, String field, Object value, long time, TimeUnit timeUnit) {
        if (field == null || value == null) {
            return 0L;
        }

        long timeoutSeconds = timeUnit.toSeconds(time);

        List<Object> args = Arrays.asList(timeoutSeconds, field, value);

        val result = redisTemplate.execute(hSetAllAndExpire, Collections.singletonList(key), args.toArray());
        if (result == null) {
            return 0L;
        }
        return result;
    }

    public Object hGet(String key, String field) {
        return redisTemplate.opsForHash().get(key, field);
    }

    public void hSetAll(String key, Map<String, Object> map) {
        redisTemplate.opsForHash().putAll(key, map);
    }

    public long hSetAll(String key, Map<String, Object> map, long time, TimeUnit timeUnit) {
        if (map == null || map.isEmpty()) {
            return 0L;
        }

        long timeoutSeconds = timeUnit.toSeconds(time);

        List<Object> args = new ArrayList<>(map.size() * 2 + 1);
        args.add(timeoutSeconds);
        map.forEach((k, v) -> {
            args.add(k);
            args.add(v.toString()); // toString 是为了 Lua 能接受
        });

        val result = redisTemplate.execute(hSetAllAndExpire, Collections.singletonList(key), args.toArray());
        if (result == null) {
            return 0L;
        }
        return result;
    }

    public Map<Object, Object> hGetAll(String key) {
        return redisTemplate.opsForHash().entries(key);
    }

    public boolean hHasKey(String key, String field) {
        return Boolean.TRUE.equals(redisTemplate.opsForHash().hasKey(key, field));
    }

    public void hDel(String key, String... fields) {
        redisTemplate.opsForHash().delete(key, (Object[]) fields);
    }

    // ======================== List ========================

    public void lPush(String key, Object value) {
        redisTemplate.opsForList().leftPush(key, value);
    }

    public void lPush(String key, Object value, long time, TimeUnit timeUnit) {
        long timeoutSeconds = timeUnit.toSeconds(time);
        redisTemplate.execute(
                lPushAndExpire,
                Collections.singletonList(key),
                value.toString(), timeoutSeconds
        );
    }

    public void rPush(String key, Object value) {
        redisTemplate.opsForList().rightPush(key, value);
    }

    public void rPush(String key, Object value, long time, TimeUnit timeUnit) {
        long timeoutSeconds = timeUnit.toSeconds(time);
        redisTemplate.execute(
                rPushAndExpire,
                Collections.singletonList(key),
                value.toString(), timeoutSeconds
        );
    }

    public Object lPop(String key) {
        return redisTemplate.opsForList().leftPop(key);
    }

    public Object rPop(String key) {
        return redisTemplate.opsForList().rightPop(key);
    }

    public List<Object> lRange(String key, long start, long end) {
        return redisTemplate.opsForList().range(key, start, end);
    }

    public long lSize(String key) {
        Long size = redisTemplate.opsForList().size(key);
        return size != null ? size : 0L;
    }

    // ======================== Set ========================

    public void sAdd(String key, Object... values) {
        redisTemplate.opsForSet().add(key, values);
    }

    /**
     * 往 Set 里添加成员，同时给整个 Set 设置过期时间
     * @param key    Redis 键
     * @param timeout 过期时间
     * @param timeUnit 过期时间单位
     * @param values 要加入 Set 的成员
     * @return 添加成功的成员数量
     */
    public long sAddAndExpire(String key, long timeout, TimeUnit timeUnit, Object... values) {
        if (values == null || values.length == 0) {
            return 0L;
        }
        long timeoutSeconds = timeUnit.toSeconds(timeout);
        List<Object> args = new ArrayList<>(values.length + 1);
        args.add(timeoutSeconds);
        args.addAll(Arrays.asList(values));
        val result = redisTemplate.execute(sAddAndExpire, Collections.singletonList(key), args.toArray());
        if (result == null) {
            return 0L;
        }
        return result;
    }

    public Set<Object> sMembers(String key) {
        return redisTemplate.opsForSet().members(key);
    }

    public boolean sIsMember(String key, Object value) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(key, value));
    }

    public long sRemove(String key, Object... values) {
        Long removed = redisTemplate.opsForSet().remove(key, values);
        return removed != null ? removed : 0L;
    }

    // ======================== ZSet ========================

    public void zAdd(String key, Object value, double score) {
        redisTemplate.opsForZSet().add(key, value, score);
    }

    public void zAdd(String key, Object value, double score, long time, TimeUnit timeUnit) {
        long timeoutSeconds = timeUnit.toSeconds(time);
        redisTemplate.execute(
                zAddAndExpire,
                Collections.singletonList(key),
                value.toString(),
                score,
                timeoutSeconds
        );
    }

    public Set<Object> zRange(String key, long start, long end) {
        return redisTemplate.opsForZSet().range(key, start, end);
    }

    public Long zRemove(String key, Object... values) {
        return redisTemplate.opsForZSet().remove(key, values);
    }

    /**
     * 获取 ZSet 中元素的分数
     * @param key ZSet 的 key
     * @param value 值
     * @return 分数，如果元素不存在或value不存在则返回 null
     */
    public Double zScore(String key, Object value) {
        return redisTemplate.opsForZSet().score(key, value);
    }

    /**
     * 获取 ZSet 的元素数量
     *
     * @param key ZSet 的 key
     * @return 元素个数，如果 key 不存在，返回 0
     */
    public long zSize(String key) {
        Long size = redisTemplate.opsForZSet().size(key);
        return size != null ? size : 0L;
    }

    /**
     * 获取对象并反序列化为指定类型
     */
    public <T> T get(String key, Class<T> clazz) {
        Object value = redisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        // 如果不是直接实例，尝试用 fastjson 反序列化
        return JSON.parseObject(value.toString(), clazz);
    }

    // ======================== Stream ========================

    private StreamOperations<String, String, String> streamOps() {
        return stringRedisTemplate.opsForStream();
    }

    /**
     * 创建消费者组（如果已存在则忽略异常）
     * @param key   Redis Stream 的 key
     * @param group 消费者组名
     * 使用方式：xGroupCreate("article_like_stream", "like_group");
     */
    public void xGroupCreate(String key, String group) {
        try {
            streamOps().createGroup(key, group);
        } catch (RedisSystemException e) {
            if (!Objects.requireNonNull(e.getMessage()).contains("BUSYGROUP")) {
                throw e; // 不是 group 已存在的异常就继续抛
            }
        }
    }

    /**
     * 往 Stream 中追加一条消息
     * @param key  Stream 的 key
     * @param data 要写入的键值对数据（Map）
     * @return 写入后返回的 RecordId
     * 使用方式：xAdd("article_like_stream", Map.of("userId", "1001", "articleId", "2001"));
     */
    public String xAdd(String key, Map<String, String> data) {
        return streamOps().add(MapRecord.create(key, data)).getValue();
    }

    /**
     * 消费者读取消息（从 last consumed 开始）
     * @param key      Stream 的 key
     * @param group    消费者组
     * @param consumer 消费者名称
     * @param count    一次最多读取几条
     * @param block    如果没有消息时，最多阻塞多久（建议设置几秒）
     * @return 读取到的消息列表
     * 使用方式：
     *   xReadGroup("article_like_stream", "like_group", "consumer1", 10, Duration.ofSeconds(2));
     */
    public List<MapRecord<String, String, String>> xReadGroup(
            String key, String group, String consumer, int count, Duration block) {
        return streamOps().read(
                Consumer.from(group, consumer),
                StreamReadOptions.empty().count(count).block(block),
                StreamOffset.create(key, ReadOffset.lastConsumed())
        );
    }

    /**
     * 手动确认消息已被成功处理
     * @param key     Stream 的 key
     * @param group   消费者组
     * @param recordIds 要确认的消息 ID 列表
     * 使用方式：
     *   xAck("article_like_stream", "like_group", List.of("1709876543210-0"));
     */
    public void xAck(String key, String group, List<String> recordIds) {
        recordIds.forEach(recordId -> streamOps().acknowledge(key, group, recordId));
    }

    /**
     * 手动确认消息已被成功处理
     * @param key Stream 的 key
     * @param group 消费者组名
     * @param recordId 要确认消息的 ID
     */
    public void xAck(String key, String group, String recordId) {
        streamOps().acknowledge(key, group, recordId);
    }

    /**
     * 获取某消费者组下的待确认消息统计信息
     * @param key   Stream 的 key
     * @param group 消费者组名
     * @return 摘要信息：总条数、最早未确认消息ID、最新未确认消息ID等
     * 使用方式：xPendingSummary("article_like_stream", "like_group");
     */
    public PendingMessagesSummary xPendingSummary(String key, String group) {
        return streamOps().pending(key, group);
    }

    /**
     * 获取某消费者组下指定消费者未确认的消息列表
     * @param key      Stream 的 key
     * @param group    消费者组
     * @param consumer 消费者名
     * @param count    最多拉多少条
     * @return 待确认消息列表（含 ID、时间戳、delivery count 等信息）
     * 使用方式：xPending("article_like_stream", "like_group", "consumer1", 10);
     */
    public PendingMessages xPending(String key, String group, String consumer, int count) {
        return streamOps().pending(
                key,
                Consumer.from(group, consumer),
                Range.unbounded(),
                count
        );
    }

    /**
     * 从指定消息 ID 开始读取（非消费者组方式）
     * @param key      Stream 的 key
     * @param recordId 起始消息 ID（如 "0" 或 "1709876123456-0"）
     * @param count    最多读取几条
     * 使用方式：xReadFromId("article_like_stream", "0", 10);
     */
    public List<MapRecord<String, String, String>> xReadFromId(String key, String recordId, int count) {
        return streamOps().read(
                StreamReadOptions.empty().count(count),
                StreamOffset.create(key, ReadOffset.from(recordId))
        );
    }


    /**
     * 删除 Redis Stream 中指定的消息记录
     *
     * <p>通常用于消费完成后清理消息，避免 Redis Stream 数据无限增长。
     * 推荐批量删除，例如每消费 100 条后统一调用本方法。</p>
     *
     * @param key       Stream 的键名，例如 "stream:article:like"
     * @param recordIds 要删除的消息 ID 列表，例如 ["1689637252831-0", "1689637252832-0"]
     * @return 实际删除的消息数量
     *
     * 用法示例：
     * <pre>{@code
     *     List<String> idsToDelete = Arrays.asList("1689637252831-0", "1689637252832-0");
     *     long removed = redisUtils.xDel("stream:article:like", idsToDelete);
     * }</pre>
     */
    public long xDel(String key, List<String> recordIds) {
        if (recordIds == null || recordIds.isEmpty()) {
            return 0L;
        }
        String[] ids = recordIds.toArray(new String[0]);
        Long removed = redisTemplate.opsForStream().delete(key, ids);
        return removed != null ? removed : 0L;
    }



}
