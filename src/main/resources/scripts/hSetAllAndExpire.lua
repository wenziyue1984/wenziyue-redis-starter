-- KEYS[1]: Redis Key
-- ARGV[1]: 过期时间（秒）
-- ARGV[2...]: 交替的 field 和 value，例如 ARGV[2]=field1, ARGV[3]=value1, ARGV[4]=field2, ARGV[5]=value2, ...

-- 准备哈希字段和值
local hash = {}
for i = 2, #ARGV, 2 do
    hash[ARGV[i]] = ARGV[i + 1]
end

-- 写入哈希字段
for field, value in pairs(hash) do
    redis.call('HSET', KEYS[1], field, value)
end

-- 设置过期时间
redis.call('EXPIRE', KEYS[1], ARGV[1])

-- 返回字段数
return table.getn(hash)