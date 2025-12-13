package com.wenziyue.redis.config;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author wenziyue
 */
public class RedisJsonMapper {

    private final ObjectMapper mapper;
    public RedisJsonMapper(ObjectMapper mapper) { this.mapper = mapper; }
    public ObjectMapper mapper() { return mapper; }
}
