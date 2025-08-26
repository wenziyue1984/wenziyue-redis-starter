-- KEYS[1] = zsetKey
-- ARGV: expireSeconds, score1, member1, score2, member2, ...
local ttl = tonumber(ARGV[1])
for i = 2, #ARGV, 2 do
    redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i+1])
end
if ttl > 0 then
    redis.call('EXPIRE', KEYS[1], ttl)
end
return redis.call('ZCARD', KEYS[1])