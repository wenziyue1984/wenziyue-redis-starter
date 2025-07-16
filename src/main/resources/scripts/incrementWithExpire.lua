-- KEYS[1]: key
-- ARGV[1]: delta
-- ARGV[2]: timeout（单位：秒）

local result = redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return result