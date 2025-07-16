-- KEYS[1]: 要设置的 key
-- ARGV[1]: 要设置的 value
-- ARGV[2]: 过期时间（秒）

local ok = redis.call('SETNX', KEYS[1], ARGV[1])
if ok == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return ok