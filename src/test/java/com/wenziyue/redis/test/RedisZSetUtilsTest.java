package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ZSetOperations;

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

    @Test
    @Order(5)
    @DisplayName("测试 zRangeAll")
    void testZRangeAll() {
        redisUtils.zAdd(KEY_ZSET, "player1", 100);
        redisUtils.zAdd(KEY_ZSET, "player2", 200);
        redisUtils.zAdd(KEY_ZSET, "player3", 150);

        Set<Object> all = redisUtils.zRangeAll(KEY_ZSET);
        log.info("zRangeAll -> {}", all);

        assertEquals(3, all.size());
        assertTrue(all.contains("player1"));
        assertTrue(all.contains("player2"));
        assertTrue(all.contains("player3"));
    }

    @Test
    @Order(6)
    @DisplayName("测试 zRangeAllWithScores")
    void testZRangeAllWithScores() {
        redisUtils.zAdd(KEY_ZSET, "player1", 100);
        redisUtils.zAdd(KEY_ZSET, "player2", 200);
        redisUtils.zAdd(KEY_ZSET, "player3", 150);

        Set<ZSetOperations.TypedTuple<Object>> tuples = redisUtils.zRangeAllWithScores(KEY_ZSET);
        log.info("zRangeAllWithScores ->");
        for (ZSetOperations.TypedTuple<Object> tuple : tuples) {
            log.info("  value: {}, score: {}", tuple.getValue(), tuple.getScore());
        }

        assertEquals(3, tuples.size());

        // 可选地检查某个具体分数
        ZSetOperations.TypedTuple<Object> player2 = tuples.stream()
                .filter(t -> "player2".equals(t.getValue()))
                .findFirst()
                .orElse(null);

        assertNotNull(player2);
        assertEquals(200.0, player2.getScore());
    }

    @Test
    @Order(7)
    @DisplayName("测试 zRemoveRangeByScore")
    void testZRemoveRangeByScore() {
        redisUtils.zAdd(KEY_ZSET, "player1", 100);
        redisUtils.zAdd(KEY_ZSET, "player2", 200);
        redisUtils.zAdd(KEY_ZSET, "player3", 300);
        redisUtils.zAdd(KEY_ZSET, "player4", 400);

        // 删除分数在 [150, 350] 之间的元素（包含200和300）
        long removedCount = redisUtils.zRemoveRangeByScore(KEY_ZSET, 150, 350);
        log.info("zRemoveRangeByScore(150, 350) 删除了 {} 个元素", removedCount);

        assertEquals(2, removedCount);

        Set<Object> remaining = redisUtils.zRangeAll(KEY_ZSET);
        log.info("删除后剩余成员: {}", remaining);

        assertEquals(2, remaining.size());
        assertTrue(remaining.contains("player1"));
        assertTrue(remaining.contains("player4"));
    }
}