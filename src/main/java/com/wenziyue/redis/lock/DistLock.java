package com.wenziyue.redis.lock;


import lombok.Getter;
import lombok.val;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * 看门狗续期的分布式锁
 * 思路很简单：拿锁时写入唯一值；一个后台线程定期续期（PEXPIRE），只在值相等时续，避免给别人的锁续命。结束时释放锁也要比对值再 DEL。
 *
 * @author wenziyue
 */
public class DistLock implements AutoCloseable {

    private final StringRedisTemplate srt;
    private final RedisScript<Long> renewScript;
    private final RedisScript<Long> unlockScript;

    private final String key;
    @Getter
    private final String value;     // 唯一值，用于校验所有权
    private final long ttlMs;

    private final ScheduledExecutorService ses;
    private ScheduledFuture<?> watchdog;
    private volatile boolean held = false;

    public DistLock(StringRedisTemplate srt,
                    RedisScript<Long> renewScript,
                    RedisScript<Long> unlockScript,
                    String key, long ttlMs) {
        this.srt = srt;
        this.renewScript = renewScript;
        this.unlockScript = unlockScript;
        this.key = key;
        this.ttlMs = ttlMs;
        this.value = UUID.randomUUID().toString();
        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-lock-watchdog");
            t.setDaemon(true);
            return t;
        });
    }

    /** SET key value NX PX ttl —— 原子获取 */
    public boolean tryLock() {
        Boolean ok = srt.opsForValue().setIfAbsent(key, value, Duration.ofMillis(ttlMs));
        held = Boolean.TRUE.equals(ok);
        if (held) {
            startWatchdog();
        }
        return held;
    }

    private void startWatchdog() {
        long period = Math.max(ttlMs / 3, 1000); // 每 ttl/3 续一次，至少 1s
        watchdog = ses.scheduleAtFixedRate(() -> {
            try {
                val r = srt.execute(renewScript, Collections.singletonList(key), value, Long.toString(ttlMs));
                if (r == null || r == 0) {
                    // 续期失败：可能过期或被他人占用
                    held = false;
                    stopWatchdog();
                }
            } catch (Throwable ignore) {
                // 网络抖动，交给下一个周期；必要时可加失败计数并告警
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    private void stopWatchdog() {
        if (watchdog != null) {
            watchdog.cancel(false);
        }
        ses.shutdownNow();
    }

    /** 仅当 value 匹配时删除，避免释放别人的锁 */
    @Override
    public void close() {
        try {
            if (held) {
                srt.execute(unlockScript, Collections.singletonList(key), value);
            }
        } finally {
            stopWatchdog();
        }
    }
}
