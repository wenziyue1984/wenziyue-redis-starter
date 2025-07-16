-- KEYS[1]: Redis Key
-- ARGV[1]: 过期时间（秒）
-- ARGV[2...]: 要加入 Set 的成员列表

-- 将 ARGV[2...] 提取为 Set 的值
local members = {}
for i = 2, #ARGV do
    table.insert(members, ARGV[i])
end

-- 添加元素
local added = redis.call('SADD', KEYS[1], unpack(members))

-- 设置过期时间
redis.call('EXPIRE', KEYS[1], ARGV[1])

-- 返回添加成功的数量
return added