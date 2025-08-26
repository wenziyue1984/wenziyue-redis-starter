package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.*;
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

    @Test
    @Order(8)
    @DisplayName("测试 batchZAddWithExpire")
    void testBatchZAddWithExpire() throws InterruptedException {
        Map<Double, Object> data = new LinkedHashMap<>();
        data.put(100.0, "userA");
        data.put(200.0, "userB");
        data.put(150.0, "userC");

        redisUtils.batchZAddWithExpire(KEY_ZSET, data, 3, TimeUnit.SECONDS);

        Set<Object> range = redisUtils.zRangeAll(KEY_ZSET);
        log.info("batchZAddWithExpire 插入后的成员: {}", range);
        assertEquals(3, range.size());

        TimeUnit.SECONDS.sleep(4);
        assertFalse(redisUtils.hasKey(KEY_ZSET), "ZSet key 应该已过期");
    }

    @Test
    @Order(9)
    @DisplayName("测试 zRangeByRank")
    void testRangeByRank() {
        redisUtils.zAdd(KEY_ZSET, "p1", 10);
        redisUtils.zAdd(KEY_ZSET, "p2", 20);
        redisUtils.zAdd(KEY_ZSET, "p3", 30);
        redisUtils.zAdd(KEY_ZSET, "p4", 40);

        Set<Object> asc = redisUtils.zRangeByRank(KEY_ZSET, 0, 1, false);
        List<Object> ascList = new ArrayList<>(asc);
        assertEquals(2, ascList.size());
        assertTrue(ascList.contains("p1") && ascList.contains("p2"));

        Set<Object> desc = redisUtils.zRangeByRank(KEY_ZSET, 0, 1, true);
        List<Object> descList = new ArrayList<>(desc);
        assertEquals(2, descList.size());
        assertTrue(descList.contains("p4") && descList.contains("p3"));
    }

    @Test
    @Order(10)
    @DisplayName("测试 zRangeWithScoresByRank")
    void testRangeWithScoresByRank() {
        redisUtils.zAdd(KEY_ZSET, "a", 5);
        redisUtils.zAdd(KEY_ZSET, "b", 15);
        redisUtils.zAdd(KEY_ZSET, "c", 25);

        Set<ZSetOperations.TypedTuple<Object>> result = redisUtils.zRangeWithScoresByRank(KEY_ZSET, 0, 1, true);
        assertEquals(2, result.size());

        Set<Object> values = new HashSet<>();
        for (ZSetOperations.TypedTuple<Object> t : result) {
            values.add(t.getValue());
        }
        assertTrue(values.contains("c") && values.contains("b"));
    }

    @Test
    @Order(11)
    @DisplayName("测试 zRangeByScore - 基本(score范围，正序，全部列出)")
    public void testRangeByScore_basic() {
        redisUtils.zAdd(KEY_ZSET, "x1", 10);
        redisUtils.zAdd(KEY_ZSET, "x2", 20);
        redisUtils.zAdd(KEY_ZSET, "x3", 30);
        // 查 score 在 15 ~ 25 之间的元素
        Set<Object> res = redisUtils.zRangeByScore(KEY_ZSET, 15, 25, 0, -1, false);
        // 仅有 x2 满足
        assertEquals(1, res.size());
        assertTrue(res.contains("x2"));
    }

    @Test
    @Order(12)
    @DisplayName("测试 zRangeByScoreWithScores - 限制分页 (offset/count)，升序")
    public void testRangeByScoreWithScores_paged() {
        redisUtils.zAdd(KEY_ZSET, "a", 5);
        redisUtils.zAdd(KEY_ZSET, "b", 15);
        redisUtils.zAdd(KEY_ZSET, "c", 25);
        redisUtils.zAdd(KEY_ZSET, "d", 35);
        redisUtils.zAdd(KEY_ZSET, "e", 45);
        // 取 score 10~40 范围内，第 1 条开始，取 3 条
        Set<ZSetOperations.TypedTuple<Object>> tuples =
                redisUtils.zRangeByScoreWithScores(KEY_ZSET, 10, 40, 1, 3, false);
        List<Object> vals = new ArrayList<Object>();
        for (ZSetOperations.TypedTuple<Object> t : tuples) {
            vals.add(t.getValue());
        }
        // sorted set 按 score 排序 → "b"(15), "c"(25), "d"(35), limit 从 offset=1 开始，应是 "c","d"
        assertEquals(2, vals.size());
        assertTrue(vals.contains("c") && vals.contains("d"));
    }

    @Test
    @Order(13)
    @DisplayName("测试 zRangeByScoreWithScores - 倒序排序")
    public void testRangeByScoreWithScores_reverse() {
        redisUtils.zAdd(KEY_ZSET, "p1", 100);
        redisUtils.zAdd(KEY_ZSET, "p2", 200);
        redisUtils.zAdd(KEY_ZSET, "p3", 300);
        Set<ZSetOperations.TypedTuple<Object>> tuples =
                redisUtils.zRangeByScoreWithScores(KEY_ZSET, 100, 300, 0, -1, true);
        List<Object> order = new ArrayList<Object>();
        for (ZSetOperations.TypedTuple<Object> t : tuples) {
            order.add(t.getValue());
        }
        // 倒序结果应为 p3 (300), p2 (200), p1 (100)
        assertEquals("p3", order.get(0));
        assertEquals("p2", order.get(1));
        assertEquals("p1", order.get(2));
    }

    @Test
    @Order(14)
    @DisplayName("测试 zRangeByScore - 翻页功能")
    public void testRangeByScore_offsetCount() {
        redisUtils.zAdd(KEY_ZSET, "u1", 10);
        redisUtils.zAdd(KEY_ZSET, "u2", 20);
        redisUtils.zAdd(KEY_ZSET, "u3", 30);
        redisUtils.zAdd(KEY_ZSET, "u4", 40);
        redisUtils.zAdd(KEY_ZSET, "u5", 50);
        // 查 score 0~60, 以 offset 2, count 2, 正序：期望 "u3","u4"
        Set<Object> res = redisUtils.zRangeByScore(KEY_ZSET, 0, 60, 2, 2, false);
        List<Object> ordered = new ArrayList<Object>(res);
        assertEquals(2, ordered.size());
        assertTrue(ordered.contains("u3"));
        assertTrue(ordered.contains("u4"));
    }

    @Test
    @Order(15)
    @DisplayName("测试 zRangeByScoreWithScores - 空结果及无 key")
    public void testRangeByScoreWithScores_emptyOrNoKey() {
        // 不存在的 key 返回 空集合
        Set<ZSetOperations.TypedTuple<Object>> noKeyRes =
                redisUtils.zRangeByScoreWithScores("nonexistent", 0, 50, 0, -1, false);
        assertNotNull(noKeyRes);
        assertTrue(noKeyRes.isEmpty());

        // 存在 key 但 score 无成员
        redisUtils.zAdd(KEY_ZSET, "item", 15);
        Set<ZSetOperations.TypedTuple<Object>> outside =
                redisUtils.zRangeByScoreWithScores(KEY_ZSET, 100, 200, 0, -1, false);
        assertNotNull(outside);
        assertTrue(outside.isEmpty());
    }
}