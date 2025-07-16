package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Redis List 类型操作测试")
public class RedisListUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String KEY_LIST = "test:list:queue";

    @AfterEach
    void tearDown() {
        redisUtils.delete(KEY_LIST);
    }

    @Test
    @Order(1)
    @DisplayName("测试 lPush, rPush, lSize 和 lRange")
    void testPushAndRange() {
        redisUtils.lPush(KEY_LIST, "A"); // List: [A]
        redisUtils.rPush(KEY_LIST, "C"); // List: [A, C]
        redisUtils.lPush(KEY_LIST, "B"); // List: [B, A, C]
        log.info("Push A, C, B to list {}", KEY_LIST);

        long size = redisUtils.lSize(KEY_LIST);
        assertEquals(3, size, "List size 应该为 3");
        log.info("List size: {}", size);

        List<Object> range = redisUtils.lRange(KEY_LIST, 0, -1);
        log.info("List content: {}", range);
        assertAll(
                () -> assertEquals("B", range.get(0)),
                () -> assertEquals("A", range.get(1)),
                () -> assertEquals("C", range.get(2))
        );
    }

    @Test
    @Order(2)
    @DisplayName("测试 lPop 和 rPop")
    void testPop() {
        redisUtils.rPush(KEY_LIST, "item1");
        redisUtils.rPush(KEY_LIST, "item2");
        redisUtils.rPush(KEY_LIST, "item3");

        Object leftPop = redisUtils.lPop(KEY_LIST);
        log.info("lPop -> {}", leftPop);
        assertEquals("item1", leftPop);

        Object rightPop = redisUtils.rPop(KEY_LIST);
        log.info("rPop -> {}", rightPop);
        assertEquals("item3", rightPop);

        assertEquals(1, redisUtils.lSize(KEY_LIST));
    }

    @Test
    @Order(3)
    @DisplayName("测试 lPush 带过期时间")
    void testLPushWithExpire() throws InterruptedException {
        redisUtils.lPush(KEY_LIST, "expire_val", 2, TimeUnit.SECONDS);
        log.info("lPush with 2s expiration for key {}", KEY_LIST);

        assertTrue(redisUtils.hasKey(KEY_LIST), "List key 应该存在");

        TimeUnit.SECONDS.sleep(3);

        assertFalse(redisUtils.hasKey(KEY_LIST), "List key 应该已过期");
        log.info("List key {} 已成功过期", KEY_LIST);
    }
}