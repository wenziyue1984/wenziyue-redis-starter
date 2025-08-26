-- renewLock.lua：续期。
-- KEYS[1]=lockKey, ARGV[1]=value, ARGV[2]=ttlMillis
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
else
    return 0
end