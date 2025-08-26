package com.wenziyue.redis.lock;


import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.concurrent.TimeUnit;

/**
 * @author wenziyue
 */
@RequiredArgsConstructor
public class DistLockFactory {
    private final StringRedisTemplate srt;
    private final RedisScript<Long> renewLock;
    private final RedisScript<Long> unlock;

    public DistLock create(String key, long ttl, TimeUnit ttimeUnit) {
        return new DistLock(srt, renewLock, unlock, key, ttimeUnit.toMillis(ttl));
    }
}
