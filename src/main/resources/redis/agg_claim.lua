-- agg_claim: a new owner takes a shard. Bumps the fencing token; returns {gen, flushSeq, offset}.
--
-- All keys of one shard share the hash tag {<bizDate>:<partition>}, so the call touches a single Redis Cluster slot
-- and runs atomically:
--   agg:{D:p}:meta    hash  ownerGen, flushSeq, offset (S: the next journal offset the snapshot covers)
--
-- KEYS: meta
local meta = KEYS[1]

local function num(v, d)
  if v then return tonumber(v) end
  return d
end

local gen = redis.call('HINCRBY', meta, 'ownerGen', 1)
return { gen, num(redis.call('HGET', meta, 'flushSeq'), 0), num(redis.call('HGET', meta, 'offset'), -1) }
