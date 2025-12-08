//package com.wenziyue.redis.serializer;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.parser.ParserConfig;
//import com.alibaba.fastjson.serializer.SerializerFeature;
//import org.springframework.data.redis.serializer.RedisSerializer;
//import org.springframework.data.redis.serializer.SerializationException;
//
//import java.nio.charset.StandardCharsets;
//
///**
// * 基于 Fastjson 的 Redis 序列化器
// *
// * @author wenziyue
// */
//public class FastJsonRedisSerializer<T> implements RedisSerializer<T> {
//
//    private final Class<T> clazz;
//
//    static {
//        // 开启 AutoType 支持（Fastjson 2 中默认关闭，手动打开）
//        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
//    }
//
//    public FastJsonRedisSerializer(Class<T> clazz) {
//        this.clazz = clazz;
//    }
//
//    @Override
//    public byte[] serialize(T t) throws SerializationException {
//        if (t == null) return new byte[0];
//        return JSON.toJSONString(t, SerializerFeature.WriteClassName).getBytes(StandardCharsets.UTF_8);
//    }
//
//    @Override
//    public T deserialize(byte[] bytes) throws SerializationException {
//        if (bytes == null || bytes.length == 0) return null;
//        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), clazz);
//    }
//}
