-- KEYS[1]: key
-- ARGV[1]: value
-- ARGV[2]: timeout（秒）

redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1