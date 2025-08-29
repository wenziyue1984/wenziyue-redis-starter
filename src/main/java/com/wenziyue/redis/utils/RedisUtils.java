package com.wenziyue.redis.utils;

import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import com.alibaba.fastjson.JSON;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Redis 工具类，封装常用操作
 *
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
    private final RedisScript<Long> batchZAddWithExpire;

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
    public void set(String key, Object value, long timeout, TimeUnit timeUnit) {
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
     *
     * @param key   Redis Key
     * @param delta 增加的值（允许负数）
     * @return 增加后的结果，操作失败返回null
     */
    public Long increment(String key, long delta) {
        return redisTemplate.opsForValue().increment(key, delta);
    }

    /**
     * 原子自增，并设置过期时间
     *
     * @param key     Redis Key
     * @param delta   增加的值（允许负数）
     * @param timeout 超时时间
     * @param unit    时间单位
     * @return 增加后的结果, 失败返回null
     */
    public Long increment(String key, long delta, long timeout, TimeUnit unit) {
        long timeoutSeconds = unit.toSeconds(timeout);
        return redisTemplate.execute(incrementWithExpire, Collections.singletonList(key), delta, timeoutSeconds);
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

    public void hDel(String key, Object[] fields) {
        redisTemplate.opsForHash().delete(key, fields);
    }

    /**
     * Hash 递增操作。
     * 如果字段不存在，则会创建字段并设置为增量值。
     * 如果 key 不存在，则会创建新的 Hash。
     *
     * @param key   Redis Key
     * @param field Hash 字段
     * @param delta 增量（可以是正数或负数）
     * @return 递增后的值
     */
    public Long hIncrBy(String key, String field, long delta) {
        return redisTemplate.opsForHash().increment(key, field, delta);
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
     *
     * @param key      Redis 键
     * @param timeout  过期时间
     * @param timeUnit 过期时间单位
     * @param values   要加入 Set 的成员
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

    public void batchZAddWithExpire(String key, Map<Double, Object> valueMap, long time, TimeUnit timeUnit) {
        List<String> keys = Collections.singletonList(key);

        // 构造 ARGV 参数：第 1 个为过期秒数，后面依次是 score, member 对
        List<Object> args = new ArrayList<>();
        args.add(timeUnit.toSeconds(time));
        for (Map.Entry<Double, Object> entry : valueMap.entrySet()) {
            args.add(entry.getKey());
            args.add(entry.getValue().toString());
        }
        redisTemplate.execute(
                batchZAddWithExpire,
                keys,
                args.toArray()
        );
    }

    public Set<Object> zRange(String key, long start, long end) {
        return redisTemplate.opsForZSet().range(key, start, end);
    }

    /**
     * 根据排名区间获取 ZSet 中的元素（不含 score），支持升序或降序。
     *
     * @param key Redis ZSet 的 key
     * @param start 起始排名（从 0 开始）
     * @param end 结束排名（inclusive）
     * @param reverse true 使用降序（score 大到小），false 使用升序
     * @return 指定区间内的元素集合
     */
    public Set<Object> zRangeByRank(String key, long start, long end, boolean reverse) {
        if (reverse) {
            return redisTemplate.opsForZSet().reverseRange(key, start, end);
        } else {
            return redisTemplate.opsForZSet().range(key, start, end);
        }
    }

    /**
     * 根据排名区间获取 ZSet 中的元素及其 score，支持升序或降序。
     *
     * @param key Redis ZSet 的 key
     * @param start 起始排名（从 0 开始）
     * @param end 结束排名（inclusive）
     * @param reverse true 使用降序（score 大到小），false 使用升序
     * @return Set of TypedTuple，包含元素及对应 score
     */
    public Set<ZSetOperations.TypedTuple<Object>> zRangeWithScoresByRank(String key, long start, long end, boolean reverse) {
        if (reverse) {
            return redisTemplate.opsForZSet().reverseRangeWithScores(key, start, end);
        } else {
            return redisTemplate.opsForZSet().rangeWithScores(key, start, end);
        }
    }

    /**
     * 根据 score 值区间获取 ZSet 中的元素，不返回 score，只返回 member。
     * 使用 ZRANGEBYSCORE 或 ZREVRANGEBYSCORE。
     *
     * @param key ZSet 的 key
     * @param minScore 下限，默认包含
     * @param maxScore 上限，默认包含
     * @param offset 分页偏移
     * @param count 分页条数
     * @param reverse true：按 score 从大到小排序；false：从小到大
     * @return 成员集合
     */
    public Set<Object> zRangeByScore(String key, double minScore, double maxScore, long offset, long count, boolean reverse) {
        if (reverse) {
            return redisTemplate.opsForZSet().reverseRangeByScore(key, minScore, maxScore, offset, count);
        } else {
            return redisTemplate.opsForZSet().rangeByScore(key, minScore, maxScore, offset, count);
        }
    }

    /**
     * 根据 score 值区间获取 ZSet 成员及其 score。
     *
     * @param key ZSet 的 key
     * @param minScore 分数最小值（默认包含）
     * @param maxScore 分数最大值（默认包含）
     * @param offset 分页偏移
     * @param count 分页条数
     * @param reverse true：降序排序；false：升序
     * @return TypedTuple 元素带 score
     */
    public Set<ZSetOperations.TypedTuple<Object>> zRangeByScoreWithScores(String key, double minScore, double maxScore,
                                                                         long offset, long count, boolean reverse) {
        if (reverse) {
            return redisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, minScore, maxScore, offset, count);
        } else {
            return redisTemplate.opsForZSet().rangeByScoreWithScores(key, minScore, maxScore, offset, count);
        }
    }

    public Long zRemove(String key, Object... values) {
        return redisTemplate.opsForZSet().remove(key, values);
    }

    /**
     * 删除 ZSet 中 score 在 [min, max] 区间内的元素
     *
     * @param key Redis 键
     * @param min 最小 score（闭区间）
     * @param max 最大 score（闭区间）
     * @return 删除的元素数量
     */
    public Long zRemoveRangeByScore(String key, double min, double max) {
        return redisTemplate.opsForZSet().removeRangeByScore(key, min, max);
    }

    /**
     * 获取 ZSet 中元素的分数
     *
     * @param key   ZSet 的 key
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
     * 获取 ZSet 中的所有元素（不含分数）
     *
     * @param key ZSet 的 key
     * @return 元素集合，如果 key 不存在返回空集合
     */
    public Set<Object> zRangeAll(String key) {
        return redisTemplate.opsForZSet().range(key, 0, -1);
    }

    /**
     * 获取 ZSet 中的所有元素及其分数
     *
     * @param key ZSet 的 key
     * @return 元素与分数组成的集合，如果 key 不存在返回空集合
     */
    public Set<ZSetOperations.TypedTuple<Object>> zRangeAllWithScores(String key) {
        return redisTemplate.opsForZSet().rangeWithScores(key, 0, -1);
    }

    // ======================== Stream ========================

    private StreamOperations<String, String, String> streamOps() {
        return stringRedisTemplate.opsForStream();
    }

    /**
     * 创建消费者组（如果已存在则忽略异常）
     *
     * @param key   Redis Stream 的 key
     * @param group 消费者组名
     *              使用方式：xGroupCreate("article_like_stream", "like_group");
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
     *
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
     *
     * @param key      Stream 的 key
     * @param group    消费者组
     * @param consumer 消费者名称
     * @param count    一次最多读取几条
     * @param block    如果没有消息时，最多阻塞多久（建议设置几秒）
     * @return 读取到的消息列表
     * 使用方式：
     * xReadGroup("article_like_stream", "like_group", "consumer1", 10, Duration.ofSeconds(2));
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
     *
     * @param key       Stream 的 key
     * @param group     消费者组
     * @param recordIds 要确认的消息 ID 列表
     *                  使用方式：
     *                  xAck("article_like_stream", "like_group", List.of("1709876543210-0"));
     */
    public void xAck(String key, String group, List<String> recordIds) {
        recordIds.forEach(recordId -> streamOps().acknowledge(key, group, recordId));
    }

    /**
     * 手动确认消息已被成功处理
     *
     * @param key      Stream 的 key
     * @param group    消费者组名
     * @param recordId 要确认消息的 ID
     */
    public void xAck(String key, String group, String recordId) {
        streamOps().acknowledge(key, group, recordId);
    }

    /**
     * 获取某消费者组下的待确认消息统计信息
     *
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
     *
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
     * 获取消费组内所有未确认的消息(不需要消费者名)
     *
     * @param key   Stream 的 key
     * @param group 消费者组
     * @param count 最多拉多少条
     * @return 待确认消息列表（含 ID、时间戳、delivery count 等信息）
     */
    public PendingMessages xPending(String key, String group, int count) {
        return streamOps().pending(key, group, Range.unbounded(), count);
    }

    /**
     * 获取消费组内所有未确认的消息，指定一个偏移量（offset）
     *
     * @param key    Stream 的 key
     * @param group  消费者组
     * @param offset 偏移量
     * @param count  最多拉多少条
     * @return 待确认消息列表（含 ID、时间戳、delivery count 等信息）
     */
    public PendingMessages xPendingHead(String key, String group, String offset, int count) {
        return streamOps().pending(
                key,
                group,
                Range.closed(offset, "+"),
                count
        );
    }

    /**
     * 从指定消息 ID 开始读取（非消费者组方式）
     *
     * @param key      Stream 的 key
     * @param recordId 起始消息 ID（如 "0" 或 "1709876123456-0"）
     * @param count    最多读取几条
     *                 使用方式：xReadFromId("article_like_stream", "0", 10);
     */
    public List<MapRecord<String, String, String>> xReadFromId(String key, String recordId, int count) {
        return streamOps().read(
                StreamReadOptions.empty().count(count),
                StreamOffset.create(key, ReadOffset.from(recordId))
        );
    }

    private final RedisTemplate<String, String> redisStreamTemplate;

    /**
     * 将 Redis Stream 中处于 Pending 状态并且超过最小空闲时间的消息重新分配给指定的消费者。
     * <p>
     * 此方法通常用于处理“僵尸消息”或“死信消息”，即已被某个消费者读取但未确认（ACK）的消息，
     * 如果该消息长时间未被处理完毕，可通过该方法将其转移到新的消费者进行重新处理。
     *
     * @param key         Redis Stream 的键名，即消息所在的 stream。
     * @param group       消费者组的名称。
     * @param newConsumer 新的消费者名称，通常为当前实例的消费者标识。
     * @param minIdleTime 消息的最小空闲时间（即从上次 delivery 到现在的时间）。只有超过该时间的消息才会被重新分配。
     * @param messageIds  待重新 claim 的消息 ID 列表。
     * @return 被成功 claim 的消息列表。返回的消息类型为 {@code MapRecord<String, Object, Object>}，
     * 可通过 {@code record.getValue()} 获取消息内容并做进一步处理。
     */
    public List<MapRecord<String, Object, Object>> xClaim(String key,
                                                          String group,
                                                          String newConsumer,
                                                          Duration minIdleTime,
                                                          List<String> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            return Collections.emptyList();
        }

        return redisStreamTemplate.opsForStream().claim(
                key,
                group,
                newConsumer,
                RedisStreamCommands.XClaimOptions.minIdle(minIdleTime).ids(messageIds.stream()
                        .map(RecordId::of).toArray(RecordId[]::new)));
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
     * <p>
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


    // ======================== 布隆过滤器（基于 EVAL） ========================

    /**
     * 初始化 Bloom Filter。
     * 若已存在会报 BUSYFILTER/exists，我们捕获后当作成功。
     */
    public boolean bfReserve(String key, double errorRate, long capacity) {
        String script = "return redis.call('BF.RESERVE', KEYS[1], ARGV[1], ARGV[2])";
        return Boolean.TRUE.equals(stringRedisTemplate.execute((RedisCallback<Boolean>) conn -> {
            try {
                StringRedisConnection c = (StringRedisConnection) conn;
                Object reply = c.eval(script, ReturnType.STATUS, 1,
                        key,
                        Double.toString(errorRate),
                        Long.toString(capacity)
                );
                return "OK".equalsIgnoreCase(asString(reply));
            } catch (Exception e) {
                String msg = e.getMessage();
                if (msg != null && (msg.contains("exists") || msg.contains("BUSYFILTER"))) {
                    return true;
                }
                throw e;
            }
        }));
    }

    /**
     * 创建布隆过滤器并设置过期时间（原子操作）<br>
     * 如果过滤器已存在，直接跳过 Reserve；无论如何都会刷新 TTL。<br>
     *
     * @param key       过滤器 key
     * @param errorRate 误判率，如 0.01
     * @param capacity  预估元素数，如 100000
     * @param timeout   过期时长
     * @param timeUnit  过期时长单位
     * @return true 表示脚本执行成功（reserve 成功或已存在）
     */
    public boolean bfReserve(String key, double errorRate, long capacity, long timeout, TimeUnit timeUnit) {
        long ttlSecs = timeUnit.toSeconds(timeout);

        // Lua：不存在先创建，再设置 expire
        String script =
                "if redis.call('EXISTS', KEYS[1]) == 0 then " +
                        "  redis.call('BF.RESERVE', KEYS[1], ARGV[1], ARGV[2]) " +
                        "end " +
                        "redis.call('EXPIRE', KEYS[1], ARGV[3]) " +
                        "return 1";

        // 直接 eval，ReturnType.INTEGER 保证 Lettuce 不会类型出错
        Object res = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1,
                    key,
                    Double.toString(errorRate),
                    Long.toString(capacity),
                    Long.toString(ttlSecs));
        });
        return res != null && ("1".equals(res.toString()) || "OK".equalsIgnoreCase(res.toString()));
    }

    /**
     * 获取 Bloom Filter 已插入元素数量（RedisBloom ≥ 2.4 用 BF.CARD，旧版回退 BF.INFO）
     *
     * @param key 过滤器 key
     * @return 已插入元素数；若过滤器不存在返回 0
     */
    public long bfCard(String key) {
        // Lua：优先 BF.CARD；若命令不存在或报错则解析 BF.INFO
        String script =
                "local ok, res = pcall(redis.call, 'BF.CARD', KEYS[1]); " +
                        "if ok then return res end; " +
                        "local info = redis.call('BF.INFO', KEYS[1]); " +
                        "if not info then return 0 end; " +
                        "for i = 1, #info, 2 do " +
                        "  if info[i] == 'Number of items inserted' then " +
                        "    return info[i + 1] " +
                        "  end " +
                        "end; " +
                        "return 0";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key);
        });
        if (reply == null) {
            return 0L;
        }
        if (reply instanceof Long) {
            return (Long) reply;
        }
        return Long.parseLong(reply.toString());
    }

    /**
     * 添加元素。true=第一次加入，false=可能已存在
     */
    public boolean bfAdd(String key, String item) {
        String script = "return redis.call('BF.ADD', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        return toBoolean(reply);
    }

    /**
     * 判断是否存在。true=可能存在；false=一定不存在
     */
    public boolean bfExists(String key, String item) {
        String script = "return redis.call('BF.EXISTS', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        return toBoolean(reply);
    }

    /**
     * 批量添加。返回与入参一致的布尔列表
     */
    public List<Boolean> bfMAdd(String key, List<String> items) {
        String script = buildMAddScript(items.size());
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            // KEYS[1] 是 key，ARGV 是 items
            String[] args = new String[1 + items.size()]; // numKeys=1
            args[0] = key;
            for (int i = 0; i < items.size(); i++) {
                args[i + 1] = items.get(i);
            }
            return c.eval(script, ReturnType.MULTI, 1, args);
        });
        return toBooleanList(reply);
    }

    /**
     * 批量判断。返回与入参一致的布尔列表
     */
    public List<Boolean> bfMExists(String key, List<String> items) {
        String script = buildMExistsScript(items.size());
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            String[] args = new String[1 + items.size()];
            args[0] = key;
            for (int i = 0; i < items.size(); i++) {
                args[i + 1] = items.get(i);
            }
            return c.eval(script, ReturnType.MULTI, 1, args);
        });
        return toBooleanList(reply);
    }

    /* 构造 BF.MADD 的 Lua 脚本 */
    private String buildMAddScript(int count) {
        // redisbloom 原生 BF.MADD 支持一次多个，但 eval 里 redis.call 接收可变参数
        // 下面脚本循环调用 BF.ADD 并把结果 push 到 table 里返回
        return
                "local res = {} " +
                        "for i=1, #ARGV do " +
                        "  local r = redis.call('BF.ADD', KEYS[1], ARGV[i]) " +
                        "  table.insert(res, r) " +
                        "end " +
                        "return res";
    }

    /* 构造 BF.MEXISTS 的 Lua 脚本 */
    private String buildMExistsScript(int count) {
        return
                "local res = {} " +
                        "for i=1, #ARGV do " +
                        "  local r = redis.call('BF.EXISTS', KEYS[1], ARGV[i]) " +
                        "  table.insert(res, r) " +
                        "end " +
                        "return res";
    }

    /* ------------------- helpers ------------------- */

    private static String asString(Object obj) {
        if (obj == null) return null;
        if (obj instanceof byte[]) return new String((byte[]) obj, StandardCharsets.UTF_8);
        return obj.toString();
    }

    private static boolean toBoolean(Object reply) {
        if (reply == null) return false;
        if (reply instanceof Long) return ((Long) reply) == 1L;
        if (reply instanceof Integer) return ((Integer) reply) == 1;
        if (reply instanceof byte[]) return "1".equals(new String((byte[]) reply, StandardCharsets.UTF_8));
        return "1".equals(String.valueOf(reply));
    }

    private static List<Boolean> toBooleanList(Object reply) {
        if (reply instanceof List<?>) {
            List<?> raw = (List<?>) reply;
            List<Boolean> result = new ArrayList<>(raw.size());
            for (Object o : raw) {
                result.add(toBoolean(o));
            }
            return result;
        }
        return Collections.emptyList();
    }


    // ======================== cf 布谷过滤器 Cuckoo Filter ========================

    /**
     * 预创建一个 Cuckoo Filter（只指定容量，其它参数用模块默认值）。
     *
     * <p>参数说明：
     * <br>• key：过滤器的 Redis 键名。建议按业务做分片前缀，便于迁移与统计。
     * <br>• capacity：预计元素数。实际会向上取整到 2^n；过滤器通常在“未达 capacity”前就会自判满并触发扩展，
     *               因此请预留冗余（capacity × 1.2~1.5）。</p>
     *
     * <p>返回：创建成功返回 true（底层返回 "OK"）。若 key 已存在会报错（本方法按 false 处理）。</p>
     */
    public boolean cfReserve(String key, long capacity) {
        String script = "return redis.call('CF.RESERVE', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.STATUS, 1, key, Long.toString(capacity));
        });
        return "OK".equalsIgnoreCase(String.valueOf(reply));
    }

    /**
     * 预创建一个 Cuckoo Filter，可同时指定桶大小/最大搬迁次数/扩展因子。
     *
     * <p>参数说明：
     * <br>• key：过滤器键名。
     * <br>• capacity：预计元素数（向上取整到 2^n；建议预留冗余）。
     * <br>• bucketSize：每个桶可容纳的指纹数量。整数 [1,255]，默认 2。
     *                  值越大，填充率越高，但**误判率线性升高**且插入稍慢。
     * <br>• maxIterations：插入时指纹在各桶之间“搬迁/置换”的最大尝试次数。整数 [1,65535]，默认 20。
     *                     值越高，越不容易触发扩展，但在接近满载时会拖慢插入。
     * <br>• expansion：当需要新建“子过滤器”时，新子过滤器的大小 = 旧子过滤器大小 × expansion。整数 [0,32768]，默认 1。
     *                 扩展会增加误判率并带来一定性能开销（过滤器最多可扩到 ~32 倍）。</p>
     *
     * <p>返回：创建成功返回 true。</p>
     */
    public boolean cfReserve(String key, long capacity, Integer bucketSize, Integer maxIterations, Integer expansion) {
        StringBuilder sb = new StringBuilder();
        sb.append("return redis.call('CF.RESERVE', KEYS[1], ARGV[1]");
        int i = 2;
        if (bucketSize != null) { sb.append(", 'BUCKETSIZE', ARGV[").append(i++).append("]"); }
        if (maxIterations != null) { sb.append(", 'MAXITERATIONS', ARGV[").append(i++).append("]"); }
        if (expansion != null) { sb.append(", 'EXPANSION', ARGV[").append(i++).append("]"); }
        sb.append(")");
        String script = sb.toString();

        List<String> argv = new ArrayList<>();
        argv.add(Long.toString(capacity));
        if (bucketSize != null) argv.add(Integer.toString(bucketSize));
        if (maxIterations != null) argv.add(Integer.toString(maxIterations));
        if (expansion != null) argv.add(Integer.toString(expansion));

        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            String[] args = new String[1 + argv.size()];
            args[0] = key;
            for (int j = 0; j < argv.size(); j++) args[j + 1] = argv.get(j);
            return c.eval(script, ReturnType.STATUS, 1, args);
        });
        return "OK".equalsIgnoreCase(String.valueOf(reply));
    }

    /**
     * 预创建并同时设置 TTL（原子执行）。若 key 已存在，则仅刷新 TTL。
     *
     * <p>参数说明：
     * <br>• key：过滤器键名。
     * <br>• capacity / bucketSize / maxIterations / expansion：语义同上；允许传 null 表示使用默认。
     * <br>• timeout / unit：整个过滤器 Key 的过期时间。注意：一旦过期，整张过滤器会被删除，
     *                      历史“已见过”的判定也随之消失。仅在确有生命周期管理需求时使用。</p>
     *
     * <p>返回：成功返回 true。</p>
     */
    public boolean cfReserve(String key, long capacity, Integer bucketSize, Integer maxIterations, Integer expansion,
                             long timeout, TimeUnit unit) {
        long ttlSecs = unit.toSeconds(timeout);

        // ARGV: 1=cap, 2=bucketSize or '', 3=maxIterations or '', 4=expansion or '', 5=ttl
        String script =
                "local cap=ARGV[1]; local b=ARGV[2]; local m=ARGV[3]; local e=ARGV[4]; " +
                        "local ttl=tonumber(ARGV[5]); " +
                        "if redis.call('EXISTS', KEYS[1]) == 0 then " +
                        "  local args={'CF.RESERVE', KEYS[1], cap}; " +
                        "  if b ~= '' then table.insert(args, 'BUCKETSIZE'); table.insert(args, b); end; " +
                        "  if m ~= '' then table.insert(args, 'MAXITERATIONS'); table.insert(args, m); end; " +
                        "  if e ~= '' then table.insert(args, 'EXPANSION'); table.insert(args, e); end; " +
                        "  redis.call(unpack(args)); " +
                        "end; " +
                        "if ttl and ttl > 0 then redis.call('EXPIRE', KEYS[1], ttl); end; " +
                        "return 1";

        String b = bucketSize == null ? "" : Integer.toString(bucketSize);
        String mi = maxIterations == null ? "" : Integer.toString(maxIterations);
        String ex = expansion == null ? "" : Integer.toString(expansion);

        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1,
                    key,
                    Long.toString(capacity), b, mi, ex, Long.toString(ttlSecs));
        });
        return reply != null;
    }

    /**
     * 单元素添加。
     *
     * <p>行为：允许对同一元素多次添加（会把“加入次数”累加），需要“只在不存在时添加”请用 {@link #cfAddNx}。</p>
     *
     * @param key  过滤器键名
     * @param item 元素（指纹由模块内部计算）
     * @return true 表示这次添加成功（或可能成功）；false 表示失败
     */
    public boolean cfAdd(String key, String item) {
        String script = "return redis.call('CF.ADD', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        return toBoolean(reply);
    }

    /**
     * 单元素添加（仅当不存在时）。
     *
     * <p>行为：如果元素已存在，则不做任何修改并返回 false；否则插入并返回 true。</p>
     */
    public boolean cfAddNx(String key, String item) {
        String script = "return redis.call('CF.ADDNX', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        return toBoolean(reply);
    }

    /**
     * 查询元素是否“可能存在”。
     *
     * <p>返回语义：true=可能存在（存在“假阳性”）；false=一定不存在（无“假阴性”）。</p>
     */
    public boolean cfExists(String key, String item) {
        String script = "return redis.call('CF.EXISTS', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        return toBoolean(reply);
    }

    /**
     * 批量存在性查询，返回与入参等长的布尔列表。
     *
     * <p>返回语义同 {@link #cfExists(String, String)}。</p>
     */
    public List<Boolean> cfMExists(String key, List<String> items) {
        if (items == null || items.isEmpty()) return Collections.emptyList();
        String script = "return redis.call('CF.MEXISTS', KEYS[1], unpack(ARGV))";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            String[] args = new String[1 + items.size()];
            args[0] = key;
            for (int i = 0; i < items.size(); i++) args[i + 1] = items.get(i);
            return c.eval(script, ReturnType.MULTI, 1, args);
        });
        return toBooleanList(reply);
    }

    /**
     * 删除一个元素的一次计数。
     *
     * <p>行为：如果该元素曾被多次添加（ADD 多次），本次只减少 1 次；需完全移除请循环调用直至 {@code false}。</p>
     */
    public boolean cfDel(String key, String item) {
        String script = "return redis.call('CF.DEL', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        return toBoolean(reply);
    }

    /**
     * 返回“可能被加入的次数”的估计值（上界估计，不会低估）。
     *
     * <p>若返回 0，表示元素不存在（或 key 不存在）。如果只关心“是否出现过”，优先用 {@link #cfExists}。</p>
     */
    public long cfCount(String key, String item) {
        String script = "return redis.call('CF.COUNT', KEYS[1], ARGV[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.INTEGER, 1, key, item);
        });
        if (reply == null) return 0L;
        if (reply instanceof Long) return (Long) reply;
        return Long.parseLong(String.valueOf(reply));
    }

    /**
     * 批量插入（可自动创建过滤器，或指定 CAPACITY；允许重复添加同一元素）。
     *
     * <p>参数说明：
     * <br>• key：过滤器键名。
     * <br>• items：要插入的元素列表。
     * <br>• capacity：当过滤器不存在时，按该容量创建（否则忽略）。
     * <br>• noCreate：为 true 时，过滤器不存在则直接失败（不创建）。</p>
     *
     * <p>返回：与 items 等长的整数列表：1=新增成功，-1=过滤器已满。</p>
     */
    public List<Integer> cfInsert(String key, List<String> items, Long capacity, boolean noCreate) {
        if (items == null || items.isEmpty()) return Collections.emptyList();
        StringBuilder lua = new StringBuilder("local args={'CF.INSERT', KEYS[1]} ");
        if (capacity != null) lua.append("; table.insert(args, 'CAPACITY'); table.insert(args, ARGV[1]) ");
        if (noCreate) lua.append("; table.insert(args, 'NOCREATE') ");
        lua.append("; table.insert(args, 'ITEMS') ");
        lua.append("; for i=").append(capacity != null ? 2 : 1).append(", #ARGV do table.insert(args, ARGV[i]) end ");
        lua.append("; return redis.call(unpack(args))");

        List<String> argv = new ArrayList<>();
        if (capacity != null) argv.add(Long.toString(capacity));
        argv.addAll(items);

        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            String[] all = new String[1 + argv.size()];
            all[0] = key;
            for (int i = 0; i < argv.size(); i++) all[i + 1] = argv.get(i);
            return c.eval(lua.toString(), ReturnType.MULTI, 1, all);
        });
        return toIntList(reply);
    }

    /**
     * 批量插入（仅在不存在时插入）。
     *
     * <p>返回：与 items 等长的整数列表：0=已存在（未插入），1=新增，-1=过滤器已满。</p>
     */
    public List<Integer> cfInsertNx(String key, List<String> items, Long capacity, boolean noCreate) {
        if (items == null || items.isEmpty()) return Collections.emptyList();
        StringBuilder lua = new StringBuilder("local args={'CF.INSERTNX', KEYS[1]} ");
        if (capacity != null) lua.append("; table.insert(args, 'CAPACITY'); table.insert(args, ARGV[1]) ");
        if (noCreate) lua.append("; table.insert(args, 'NOCREATE') ");
        lua.append("; table.insert(args, 'ITEMS') ");
        lua.append("; for i=").append(capacity != null ? 2 : 1).append(", #ARGV do table.insert(args, ARGV[i]) end ");
        lua.append("; return redis.call(unpack(args))");

        List<String> argv = new ArrayList<>();
        if (capacity != null) argv.add(Long.toString(capacity));
        argv.addAll(items);

        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            String[] all = new String[1 + argv.size()];
            all[0] = key;
            for (int i = 0; i < argv.size(); i++) all[i + 1] = argv.get(i);
            return c.eval(lua.toString(), ReturnType.MULTI, 1, all);
        });
        return toIntList(reply);
    }

    /**
     * 返回 CF.INFO 的关键信息（容量、已插入数、桶大小等），解析为 Map。
     *
     * <p>用途：监控过滤器是否逼近满载（误判率会上升）、是否频繁扩展等。</p>
     */
    @SuppressWarnings("unchecked")
    public Map<String, Long> cfInfo(String key) {
        String script = "return redis.call('CF.INFO', KEYS[1])";
        Object reply = stringRedisTemplate.execute((RedisCallback<Object>) conn -> {
            StringRedisConnection c = (StringRedisConnection) conn;
            return c.eval(script, ReturnType.MULTI, 1, key);
        });
        Map<String, Long> map = new LinkedHashMap<>();
        if (reply instanceof List<?>) {
            List<?> arr = (List<?>) reply;
            for (int i = 0; i + 1 < arr.size(); i += 2) {
                map.put(String.valueOf(arr.get(i)), Long.parseLong(String.valueOf(arr.get(i + 1))));
            }
        }
        return map;
    }

    /* ---- 小工具：把 MULTI 回复转成 Integer List ---- */
    @SuppressWarnings("unchecked")
    private static List<Integer> toIntList(Object reply) {
        if (reply instanceof List<?>) {
            List<?> raw = (List<?>) reply;
            List<Integer> result = new ArrayList<>(raw.size());
            for (Object o : raw) {
                if (o instanceof Long) result.add(((Long) o).intValue());
                else if (o instanceof Integer) result.add((Integer) o);
                else result.add(Integer.parseInt(String.valueOf(o)));
            }
            return result;
        }
        return Collections.emptyList();
    }

}
