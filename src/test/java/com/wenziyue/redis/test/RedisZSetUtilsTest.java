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
@DisplayName("Redis ZSet 类型操作测试")
public class RedisZSetUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String KEY_ZSET = "test:zset:leaderboard";

    @AfterEach
    void tearDown() {
        redisUtils.delete(KEY_ZSET);
    }

    @Test
    @Order(1)
    @DisplayName("测试 zAdd, zScore, zSize")
    void testZAddAndScore() {
        redisUtils.zAdd(KEY_ZSET, "player1", 100);
        redisUtils.zAdd(KEY_ZSET, "player2", 150);
        redisUtils.zAdd(KEY_ZSET, "player3", 120);
        log.info("zAdd members to {}", KEY_ZSET);

        assertEquals(3, redisUtils.zSize(KEY_ZSET), "ZSet size 应该为 3");

        Double score = redisUtils.zScore(KEY_ZSET, "player2");
        log.info("zScore for 'player2' -> {}", score);
        assertEquals(150.0, score, "Score of 'player2' 应该为 150.0");
    }

    @Test
    @Order(2)
    @DisplayName("测试 zRange (按分数排序)")
    void testZRange() {
        redisUtils.zAdd(KEY_ZSET, "player1", 100);
        redisUtils.zAdd(KEY_ZSET, "player2", 150);
        redisUtils.zAdd(KEY_ZSET, "player3", 120);

        Set<Object> range = redisUtils.zRange(KEY_ZSET, 0, -1);
        log.info("zRange(0, -1) -> {}", range);

        // ZRange 返回按分数升序的 Set，Set 本身无序，但 Spring 的实现通常是有序的
        // 最好转换为数组或列表来断言顺序
        Object[] orderedPlayers = range.toArray();
        assertArrayEquals(new String[]{"player1", "player3", "player2"}, orderedPlayers);
    }

    @Test
    @Order(3)
    @DisplayName("测试 zRemove")
    void testZRemove() {
        redisUtils.zAdd(KEY_ZSET, "player1", 100);
        redisUtils.zAdd(KEY_ZSET, "player2", 150);

        long removedCount = redisUtils.zRemove(KEY_ZSET, "player1");
        log.info("zRemove 'player1', removed count: {}", removedCount);
        assertEquals(1, removedCount);

        assertNull(redisUtils.zScore(KEY_ZSET, "player1"), "'player1' 的分数应为 null");
        assertEquals(1, redisUtils.zSize(KEY_ZSET));
    }

    @Test
    @Order(4)
    @DisplayName("测试 zAdd 带过期时间")
    void testZAddWithExpire() throws InterruptedException {
        redisUtils.zAdd(KEY_ZSET, "temp_player", 99, 2, TimeUnit.SECONDS);
        log.info("zAdd with 2s expiration for key {}", KEY_ZSET);

        assertTrue(redisUtils.hasKey(KEY_ZSET), "ZSet key 应该存在");

        TimeUnit.SECONDS.sleep(3);

        assertFalse(redisUtils.hasKey(KEY_ZSET), "ZSet key 应该已过期");
        log.info("ZSet key {} 已成功过期", KEY_ZSET);
    }
}