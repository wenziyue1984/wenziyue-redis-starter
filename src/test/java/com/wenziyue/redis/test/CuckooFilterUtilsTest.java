package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author wenziyue
 */
@Slf4j
@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Redis 布谷过滤器测试")
public class CuckooFilterUtilsTest {

    @Autowired
    private RedisUtils redisUtils;

    private static final String CF_KEY = "test:cf:comment-like";

    @BeforeAll
    static void init(@Autowired RedisUtils redisUtils) {
        if (!redisUtils.hasKey(CF_KEY)) {
            // 预创建：capacity=100000；其余参数用默认；并设置 10 分钟 TTL（可选）
            redisUtils.cfReserve(CF_KEY, 100_000, null, null, null, 10, TimeUnit.MINUTES);
        }
    }

    @AfterEach
    void clean() {
        redisUtils.delete(CF_KEY);
    }

    @Test
    @Order(1)
    @DisplayName("添加/去重添加/存在性/批量存在性/计数/删除")
    void testCuckooBasics() {
        // 1) ADD：允许重复添加
        boolean add1 = redisUtils.cfAdd(CF_KEY, "u1:c100");
        boolean add2 = redisUtils.cfAdd(CF_KEY, "u1:c100"); // 再加一次，计数会+1
        log.info("ADD #1={}, ADD #2={}", add1, add2);
        assertTrue(add1, "第一次 ADD 应返回 true");

        // 2) ADDNX：仅在不存在时添加
        boolean addNx1 = redisUtils.cfAddNx(CF_KEY, "u2:c200");
        boolean addNx2 = redisUtils.cfAddNx(CF_KEY, "u2:c200");
        log.info("ADDNX #1={}, ADDNX #2={}", addNx1, addNx2);
        assertTrue(addNx1, "ADDNX 首次应为 true");
        assertFalse(addNx2, "ADDNX 二次应为 false（已存在）");

        // 3) EXISTS / MEXISTS
        assertTrue(redisUtils.cfExists(CF_KEY, "u1:c100"), "已添加元素应返回可能存在");
        assertTrue(redisUtils.cfExists(CF_KEY, "u2:c200"), "已添加元素应返回可能存在");

        List<Boolean> batchExists = redisUtils.cfMExists(CF_KEY, Arrays.asList("u1:c100", "u2:c200", "uX:c999"));
        log.info("MEXISTS -> {}", batchExists);
        // 第三个未添加元素一般返回 false（允许极小概率误判为 true）
        assertEquals(3, batchExists.size());

        // 4) COUNT：返回“可能被加入的次数”（上界估计，不低估）
        long cntBefore = redisUtils.cfCount(CF_KEY, "u1:c100");
        log.info("COUNT before delete = {}", cntBefore);
        assertTrue(cntBefore >= 1, "重复 ADD 后计数应 >= 1");

        // 5) DEL：删除一次计数（若之前多次 ADD，需多次 DEL）
        boolean del1 = redisUtils.cfDel(CF_KEY, "u1:c100");
        boolean del2 = redisUtils.cfDel(CF_KEY, "u1:c100");
        log.info("DEL #1={}, DEL #2={}", del1, del2);
        assertTrue(del1, "第一次 DEL 应成功");
        // 第二次是否成功取决于之前 ADD 的次数，这里不强断言

        long cntAfter = redisUtils.cfCount(CF_KEY, "u1:c100");
        log.info("COUNT after delete = {}", cntAfter);
        assertTrue(cntAfter >= 0, "COUNT 不应为负数");

        // 6) INFO：打印关键指标（容量/已插入数/桶大小/扩展次数等），用于观察负载
        var info = redisUtils.cfInfo(CF_KEY);
        log.info("CF.INFO: {}", info);
        assertNotNull(info);
        assertFalse(info.isEmpty(), "INFO 不应为空");
    }
}
