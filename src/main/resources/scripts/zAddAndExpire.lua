-- KEYS[1]: zset 的 key
-- ARGV[1]: 元素的值
-- ARGV[2]: 元素的分数（score）
-- ARGV[3]: 过期时间（秒）

redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1