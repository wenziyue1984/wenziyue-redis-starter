package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Redis Hash 类型操作测试")
public class RedisHashUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String KEY_HASH = "test:hash:user";

    @AfterEach
    void tearDown() {
        redisUtils.delete(KEY_HASH);
    }

    @Test
    @Order(1)
    @DisplayName("测试 hSet, hGet, hHasKey")
    void testHSetAndHGet() {
        redisUtils.hSet(KEY_HASH, "name", "Tom");
        log.info("hSet a field 'name' for key {}", KEY_HASH);

        assertTrue(redisUtils.hHasKey(KEY_HASH, "name"), "Field 'name' 应该存在");
        Object value = redisUtils.hGet(KEY_HASH, "name");
        assertEquals("Tom", value, "hGet 的值应该匹配");
        log.info("hGet 'name' -> {}", value);
    }

    @Test
    @Order(2)
    @DisplayName("测试 hSetAll 和 hGetAll")
    void testHSetAllAndHGetAll() {
        Map<String, Object> userMap = new HashMap<>();
        userMap.put("name", "Jerry");
        userMap.put("age", 5);
        userMap.put("friend", "Tom");

        redisUtils.hSetAll(KEY_HASH, userMap);
        log.info("hSetAll map for key {}", KEY_HASH);

        Map<Object, Object> retrievedMap = redisUtils.hGetAll(KEY_HASH);
        log.info("hGetAll map -> {}", retrievedMap);

        assertEquals(3, retrievedMap.size());
        assertEquals("Jerry", retrievedMap.get("name"));
        // 注意：由于序列化原因，数字可能被存为字符串
        assertEquals("5", retrievedMap.get("age").toString());
    }

    @Test
    @Order(3)
    @DisplayName("测试 hDel")
    void testHDel() {
        redisUtils.hSet(KEY_HASH, "field1", "value1");
        redisUtils.hSet(KEY_HASH, "field2", "value2");

        redisUtils.hDel(KEY_HASH, Collections.singletonList("field1").toArray());
        log.info("hDel field 'field1'");

        assertFalse(redisUtils.hHasKey(KEY_HASH, "field1"), "Field 'field1' 不应再存在");
        assertTrue(redisUtils.hHasKey(KEY_HASH, "field2"), "Field 'field2' 应该仍然存在");
    }

    @Test
    @Order(4)
    @DisplayName("测试 hSetAll 带过期时间")
    void testHSetAllWithExpire() throws InterruptedException {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");

        redisUtils.hSetAll(KEY_HASH, map, 2, TimeUnit.SECONDS);
        log.info("hSetAll with 2s expiration for key {}", KEY_HASH);

        assertTrue(redisUtils.hasKey(KEY_HASH), "Hash key 应该存在");

        TimeUnit.SECONDS.sleep(3);

        assertFalse(redisUtils.hasKey(KEY_HASH), "Hash key 应该已过期");
        log.info("Hash key {} 已成功过期", KEY_HASH);
    }
}