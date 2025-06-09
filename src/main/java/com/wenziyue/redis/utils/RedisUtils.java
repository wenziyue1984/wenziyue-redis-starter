package com.wenziyue.redis.utils;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import com.alibaba.fastjson.JSON;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Redis 工具类，封装常用操作
 * @author wenziyue
 */
@Component
@RequiredArgsConstructor
public class RedisUtils {

    private final RedisTemplate<String, Object> redisTemplate;

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
     * 判断 key 是否存在
     */
    public boolean hasKey(String key) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    // ======================== Hash ========================

    public void hSet(String key, String field, Object value) {
        redisTemplate.opsForHash().put(key, field, value);
    }

    public void hSet(String key, String field, Object value, long time, TimeUnit timeUnit) {
        redisTemplate.opsForHash().put(key, field, value);
        redisTemplate.expire(key, time, timeUnit);
    }

    public Object hGet(String key, String field) {
        return redisTemplate.opsForHash().get(key, field);
    }

    public void hSetAll(String key, Map<String, Object> map) {
        redisTemplate.opsForHash().putAll(key, map);
    }

    public void hSetAll(String key, Map<String, Object> map, long time, TimeUnit timeUnit) {
        redisTemplate.opsForHash().putAll(key, map);
        redisTemplate.expire(key, time, timeUnit);
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
        redisTemplate.opsForList().leftPush(key, value);
        redisTemplate.expire(key, time, timeUnit);
    }

    public void rPush(String key, Object value) {
        redisTemplate.opsForList().rightPush(key, value);
    }

    public void rPush(String key, Object value, long time, TimeUnit timeUnit) {
        redisTemplate.opsForList().rightPush(key, value);
        redisTemplate.expire(key, time, timeUnit);
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
     */
    public void sAddAndExpire(String key, long timeout, TimeUnit timeUnit, Object... values) {
        redisTemplate.opsForSet().add(key, values);
        redisTemplate.expire(key, timeout, timeUnit);
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
        redisTemplate.opsForZSet().add(key, value, score);
        redisTemplate.expire(key, time, timeUnit);
    }

    public Set<Object> zRange(String key, long start, long end) {
        return redisTemplate.opsForZSet().range(key, start, end);
    }

    public Long zRemove(String key, Object... values) {
        return redisTemplate.opsForZSet().remove(key, values);
    }

    public Double zScore(String key, Object value) {
        return redisTemplate.opsForZSet().score(key, value);
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

}
