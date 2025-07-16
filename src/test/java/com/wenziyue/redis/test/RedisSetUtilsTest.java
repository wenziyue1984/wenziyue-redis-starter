package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Redis Set 类型操作测试")
public class RedisSetUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String KEY_SET = "test:set:tags";

    @AfterEach
    void tearDown() {
        redisUtils.delete(KEY_SET);
    }

    @Test
    @Order(1)
    @DisplayName("测试 sAdd, sMembers, sIsMember")
    void testAddAndCheckMembers() {
        redisUtils.sAdd(KEY_SET, "java", "python", "go", "java"); // "java" is duplicate
        log.info("sAdd members to {}", KEY_SET);

        Set<Object> members = redisUtils.sMembers(KEY_SET);
        log.info("sMembers -> {}", members);
        assertEquals(3, members.size(), "Set size 应该为 3 (去重后)");
        assertTrue(members.contains("java"));

        assertTrue(redisUtils.sIsMember(KEY_SET, "python"), "'python' 应该是成员");
        assertFalse(redisUtils.sIsMember(KEY_SET, "ruby"), "'ruby' 不应该是成员");
    }

    @Test
    @Order(2)
    @DisplayName("测试 sRemove")
    void testSRemove() {
        redisUtils.sAdd(KEY_SET, "apple", "banana");

        long removedCount = redisUtils.sRemove(KEY_SET, "apple");
        log.info("sRemove 'apple', removed count: {}", removedCount);
        assertEquals(1, removedCount);

        assertFalse(redisUtils.sIsMember(KEY_SET, "apple"), "'apple' 不应再是成员");
    }

    @Test
    @Order(3)
    @DisplayName("测试 sAddAndExpire")
    void testSAddAndExpire() throws InterruptedException {
        redisUtils.sAddAndExpire(KEY_SET, 2, TimeUnit.SECONDS, "temp1", "temp2");
        log.info("sAddAndExpire with 2s expiration for key {}", KEY_SET);

        assertTrue(redisUtils.hasKey(KEY_SET), "Set key 应该存在");

        TimeUnit.SECONDS.sleep(3);

        assertFalse(redisUtils.hasKey(KEY_SET), "Set key 应该已过期");
        log.info("Set key {} 已成功过期", KEY_SET);
    }
}