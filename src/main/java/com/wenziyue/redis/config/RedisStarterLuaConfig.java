//package com.wenziyue.redis.config;
//
//import com.wenziyue.redis.lock.DistLockFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Primary;
//import org.springframework.core.io.ClassPathResource;
//import org.springframework.data.redis.core.StringRedisTemplate;
//import org.springframework.data.redis.core.script.RedisScript;
//import org.springframework.stereotype.Component;
//
///**
// * @author wenziyue
// */
//@Component
//public class RedisStarterLuaConfig {
//
//    @Bean
//    public RedisScript<Long> incrementWithExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/incrementWithExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> sAddAndExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/sAddAndExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> hSetAllAndExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/hSetAllAndExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> lPushAndExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/lPushAndExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> rPushAndExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/rPushAndExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> zAddAndExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/zAddAndExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> setIfAbsentAndExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/setIfAbsentAndExpire.lua"), Long.class);
//    }
//
//    @Bean
//    public RedisScript<Long> batchZAddWithExpire() {
//        return RedisScript.of(new ClassPathResource("scripts/batchZAddWithExpire.lua"), Long.class);
//    }
//
//    @Bean("renewLock")
//    public RedisScript<Long> renewLock() {
//        return RedisScript.of(new ClassPathResource("scripts/renewLock.lua"), Long.class);
//    }
//
//    @Bean("unlock")
//    public RedisScript<Long> unlock() {
//        return RedisScript.of(new ClassPathResource("scripts/unlock.lua"), Long.class);
//    }
//
//    @Bean
//    @Primary
//    public DistLockFactory distLockFactory(StringRedisTemplate srt, RedisScript<Long> renewLock, RedisScript<Long> unlock) {
//        return new DistLockFactory(srt, renewLock, unlock);
//    }
//}
