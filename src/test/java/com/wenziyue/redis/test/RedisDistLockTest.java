package com.wenziyue.redis.test;

import com.wenziyue.redis.RedisStarterTestApplication;
import com.wenziyue.redis.lock.DistLock;
import com.wenziyue.redis.lock.DistLockFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest(classes = RedisStarterTestApplication.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Redis 看门狗分布式锁测试")
public class RedisDistLockTest {

    @Autowired
    private DistLockFactory distLockFactory;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    // 可选：直接验证“比对 value 再解锁”的 Lua
    @Autowired
    @Qualifier("unlock")
    private RedisScript<Long> lockUnlock;

    private static final String LOCK_KEY = "test:lock:watchdog";

    @AfterEach
    void tearDown() {
        stringRedisTemplate.delete(LOCK_KEY);
    }

    @Test
    @Order(1)
    @DisplayName("tryLock 互斥：同一时刻只能一个持有")
    void testExclusive() {
        try (DistLock l1 = distLockFactory.create(LOCK_KEY, 5, TimeUnit.SECONDS)) {
            assertTrue(l1.tryLock(), "第一个实例应当拿到锁");

            try (DistLock l2 = distLockFactory.create(LOCK_KEY, 5, TimeUnit.SECONDS)) {
                assertFalse(l2.tryLock(), "第二个实例不应当拿到锁（互斥）");
            }
        } // l1 释放

        // 释放后应当可以再次加锁
        try (DistLock l3 = distLockFactory.create(LOCK_KEY, 5, TimeUnit.SECONDS)) {
            assertTrue(l3.tryLock(), "释放后应当能再次拿到锁");
        }
    }

    @Test
    @Order(2)
    @DisplayName("看门狗自动续期：TTL 在长任务中保持 > 0")
    void testWatchdogRenewsTtl() throws Exception {
        try (DistLock lock = distLockFactory.create(LOCK_KEY, 2, TimeUnit.SECONDS)) { // 2s TTL
            assertTrue(lock.tryLock(), "应当成功拿到锁");

            // 等待 3.1s，理论上已超过初始 TTL，但看门狗会续期
            Thread.sleep(3_100);

            Long ttlMs = stringRedisTemplate.getExpire(LOCK_KEY, TimeUnit.MILLISECONDS);
            log.info("当前 TTL(ms) = {}", ttlMs);

            assertNotNull(ttlMs, "应返回 TTL");
            assertTrue(ttlMs > 0, "续期后 TTL 应大于 0（锁仍然存在）");
        }

        // 关闭后应当删除 key
        assertNotEquals(Boolean.TRUE, stringRedisTemplate.hasKey(LOCK_KEY), "close() 后应删除锁");
    }

    @Test
    @Order(3)
    @DisplayName("安全释放：只有持锁者才能解锁")
    void testSafeUnlock() {
        try (DistLock lock = distLockFactory.create(LOCK_KEY, 5, TimeUnit.SECONDS)) {
            assertTrue(lock.tryLock(), "应当成功拿到锁");

            // 伪造一个错误的 value 去尝试解锁，应该失败
            Long del = stringRedisTemplate.execute(lockUnlock, Collections.singletonList(LOCK_KEY), "wrong-value");
            assert del != null;
            assertEquals(0L, del.longValue(), "错误的 value 不能删除锁");

            assertEquals(Boolean.TRUE, stringRedisTemplate.hasKey(LOCK_KEY), "锁仍然存在");
        }

        // 由持锁者 close() 正确释放
        assertNotEquals(Boolean.TRUE, stringRedisTemplate.hasKey(LOCK_KEY), "持锁者 close() 后应删除锁");
    }
}