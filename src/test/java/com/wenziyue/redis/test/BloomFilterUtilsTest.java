package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
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
@DisplayName("Redis 布隆过滤器测试")
public class BloomFilterUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String FILTER = "test:bloom:article";

    @BeforeAll
    static void initFilter(@Autowired RedisUtils redisUtils) {
        if (!redisUtils.hasKey(FILTER)) {
//            redisUtils.bfReserve(FILTER, 0.01, 100000);
            redisUtils.bfReserve(FILTER, 0.01, 100000, 1000, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void tearDown() {
        redisUtils.delete(FILTER);
    }

    @Test
    @Order(1)
    @DisplayName("测试添加与存在性判断")
    void testAddAndExists() {
        boolean firstAdd = redisUtils.bfAdd(FILTER, "article-123");
        boolean secondAdd = redisUtils.bfAdd(FILTER, "article-123");
        long number = redisUtils.bfCard(FILTER);
        log.info("当前过滤器中元素数量: {}", number);

        log.info("第一次添加结果: {}", firstAdd);
        log.info("第二次添加结果: {}", secondAdd);

        assertTrue(firstAdd, "第一次添加应返回 true");
        assertFalse(secondAdd, "第二次添加应返回 false");

        boolean exists = redisUtils.bfExists(FILTER, "article-123");
        assertTrue(exists, "添加后的元素应存在于布隆过滤器中");

        boolean notExists = redisUtils.bfExists(FILTER, "article-999");
        assertFalse(notExists, "未添加的元素不应存在（允许误判）");
    }
}