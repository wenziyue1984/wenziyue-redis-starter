package com.wenziyue.redis.config;

import com.alibaba.fastjson.support.spring.FastJsonRedisSerializer;
import com.wenziyue.redis.lock.DistLockFactory;
import com.wenziyue.redis.utils.RedisUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author wenziyue
 */
@Configuration
@ConditionalOnClass(RedisTemplate.class)  // 如果类路径中有 RedisTemplate，才会执行这个配置
public class WenziyueRedisAutoConfiguration {

    @Bean(name = "wzyRedisTemplate")
    @ConditionalOnMissingBean(name = "wzyRedisTemplate")
    public RedisTemplate<String, Object> wzyRedisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();

        // key 使用 String 序列化
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);

        // value 使用 FastJson 序列化
        FastJsonRedisSerializer<Object> fastJsonRedisSerializer = new FastJsonRedisSerializer<>(Object.class);
        template.setValueSerializer(fastJsonRedisSerializer);
        template.setHashValueSerializer(fastJsonRedisSerializer);

        template.setConnectionFactory(factory);
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    @ConditionalOnMissingBean(StringRedisTemplate.class)
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory factory) {
        return new StringRedisTemplate(factory);
    }

    @Bean
    public RedisScript<Long> incrementWithExpire() {
        return RedisScript.of(new ClassPathResource("scripts/incrementWithExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> sAddAndExpire() {
        return RedisScript.of(new ClassPathResource("scripts/sAddAndExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> hSetAllAndExpire() {
        return RedisScript.of(new ClassPathResource("scripts/hSetAllAndExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> lPushAndExpire() {
        return RedisScript.of(new ClassPathResource("scripts/lPushAndExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> rPushAndExpire() {
        return RedisScript.of(new ClassPathResource("scripts/rPushAndExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> zAddAndExpire() {
        return RedisScript.of(new ClassPathResource("scripts/zAddAndExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> setIfAbsentAndExpire() {
        return RedisScript.of(new ClassPathResource("scripts/setIfAbsentAndExpire.lua"), Long.class);
    }

    @Bean
    public RedisScript<Long> batchZAddWithExpire() {
        return RedisScript.of(new ClassPathResource("scripts/batchZAddWithExpire.lua"), Long.class);
    }

    @Bean("renewLock")
    public RedisScript<Long> renewLock() {
        return RedisScript.of(new ClassPathResource("scripts/renewLock.lua"), Long.class);
    }

    @Bean("unlock")
    public RedisScript<Long> unlock() {
        return RedisScript.of(new ClassPathResource("scripts/unlock.lua"), Long.class);
    }

    @Bean
    @Primary
    public DistLockFactory distLockFactory(StringRedisTemplate srt, RedisScript<Long> renewLock, RedisScript<Long> unlock) {
        return new DistLockFactory(srt, renewLock, unlock);
    }

    @Bean
    @ConditionalOnMissingBean(RedisUtils.class)
    public RedisUtils redisUtils(
            @org.springframework.beans.factory.annotation.Qualifier("wzyRedisTemplate")
            RedisTemplate<String, Object> redisTemplate,
            StringRedisTemplate stringRedisTemplate,
            RedisScript<Long> incrementWithExpire,
            RedisScript<Long> sAddAndExpire,
            RedisScript<Long> hSetAllAndExpire,
            RedisScript<Long> lPushAndExpire,
            RedisScript<Long> rPushAndExpire,
            RedisScript<Long> zAddAndExpire,
            RedisScript<Long> setIfAbsentAndExpire,
            RedisScript<Long> batchZAddWithExpire) {

        return new RedisUtils(redisTemplate, stringRedisTemplate,
                incrementWithExpire, sAddAndExpire, hSetAllAndExpire,
                lPushAndExpire, rPushAndExpire, zAddAndExpire,
                setIfAbsentAndExpire, batchZAddWithExpire);
    }
}
