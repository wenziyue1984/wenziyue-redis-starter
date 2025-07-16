package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.dto.User;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Redis String 类型操作测试")
public class RedisStringUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String KEY_STRING = "test:string:name";
    private static final String KEY_EXPIRE = "test:string:expire";
    private static final String KEY_INCR = "test:string:incr";
    private static final String KEY_NX = "test:string:nx";
    private static final String KEY_OBJ = "test:string:user";

    @AfterEach
    void tearDown() {
        // 清理测试用的 key
        redisUtils.delete(KEY_STRING);
        redisUtils.delete(KEY_EXPIRE);
        redisUtils.delete(KEY_INCR);
        redisUtils.delete(KEY_NX);
        redisUtils.delete(KEY_OBJ);
    }

    @Test
    @Order(1)
    @DisplayName("测试 set 和 get")
    void testSetAndGet() {
        String value = "wenziyue";
        redisUtils.set(KEY_STRING, value);
        log.info("Set key: {}, value: {}", KEY_STRING, value);

        Object retrievedValue = redisUtils.get(KEY_STRING);
        log.info("Get key: {}, value: {}", KEY_STRING, retrievedValue);

        assertEquals(value, retrievedValue);
    }

    @Test
    @Order(2)
    @DisplayName("测试 set 带过期时间和 getExpire")
    void testSetWithExpire() throws InterruptedException {
        String value = "will expire";
        redisUtils.set(KEY_EXPIRE, value, 2, TimeUnit.SECONDS);
        log.info("Set key: {} with 2s expiration", KEY_EXPIRE);

        assertTrue(redisUtils.hasKey(KEY_EXPIRE), "Key应该存在");
        long expireTime = redisUtils.getExpire(KEY_EXPIRE);
        log.info("Key: {} 剩余过期时间: {}s", KEY_EXPIRE, expireTime);
        assertTrue(expireTime > 0 && expireTime <= 2, "过期时间应在 (0, 2] 秒之间");

        TimeUnit.SECONDS.sleep(3);

        assertFalse(redisUtils.hasKey(KEY_EXPIRE), "Key应该已过期");
        log.info("Key: {} 已过期", KEY_EXPIRE);
    }

    @Test
    @Order(3)
    @DisplayName("测试原子自增 increment")
    void testIncrement() {
        redisUtils.set(KEY_INCR, 10);
        Long result1 = redisUtils.increment(KEY_INCR, 5);
        log.info("Increment {} by 5, result: {}", KEY_INCR, result1);
        assertEquals(15L, result1);

        Long result2 = redisUtils.increment(KEY_INCR, -3);
        log.info("Increment {} by -3, result: {}", KEY_INCR, result2);
        assertEquals(12L, result2);
    }

    @Test
    @Order(4)
    @DisplayName("测试带过期时间的原子自增 increment")
    void testIncrementWithExpire() throws InterruptedException {
        Long result = redisUtils.increment(KEY_INCR, 1, 2, TimeUnit.SECONDS);
        log.info("Increment {} with 2s expiration, result: {}", KEY_INCR, result);

        assertEquals(1L, result);
        assertTrue(redisUtils.getExpire(KEY_INCR) > 0, "Key应该有过期时间");

        TimeUnit.SECONDS.sleep(3);
        assertFalse(redisUtils.hasKey(KEY_INCR), "Key应该已过期");
        log.info("Key: {} 已成功过期", KEY_INCR);
    }

    @Test
    @Order(5)
    @DisplayName("测试 setIfAbsentAndExpire (分布式锁)")
    void testSetIfAbsentAndExpire() {
        boolean success = redisUtils.setIfAbsentAndExpire(KEY_NX, "locked", 10, TimeUnit.SECONDS);
        log.info("第一次尝试加锁: {}", success ? "成功" : "失败");
        assertTrue(success, "第一次加锁应该成功");

        boolean failure = redisUtils.setIfAbsentAndExpire(KEY_NX, "locked-again", 10, TimeUnit.SECONDS);
        log.info("第二次尝试加锁: {}", failure ? "成功" : "失败");
        assertFalse(failure, "第二次加锁应该失败，因为key已存在");
    }

    @Test
    @Order(6)
    @DisplayName("测试 get 并反序列化为对象")
    void testGetAsObject() {
        User user = new User("007", "James Bond");
        redisUtils.set(KEY_OBJ, user);
        log.info("存储 User 对象: {}", user);

        User retrievedUser = redisUtils.get(KEY_OBJ, User.class);
        log.info("获取并反序列化的 User 对象: {}", retrievedUser);

        assertNotNull(retrievedUser);
        assertEquals(user.getId(), retrievedUser.getId());
        assertEquals(user.getName(), retrievedUser.getName());
    }

    @Test
    @Order(7)
    @DisplayName("测试 delete 和 hasKey")
    void testDeleteAndHasKey() {
        redisUtils.set(KEY_STRING, "some value");
        assertTrue(redisUtils.hasKey(KEY_STRING), "Key 设置后应该存在");

        boolean deleted = redisUtils.delete(KEY_STRING);
        assertTrue(deleted, "删除操作应该返回 true");
        log.info("Key: {} 已删除", KEY_STRING);

        assertFalse(redisUtils.hasKey(KEY_STRING), "Key 删除后不应该存在");
    }
}