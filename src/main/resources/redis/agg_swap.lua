-- agg_swap: full resync after a GAP. The owner has written its whole shard into staging keys (chunked, outside this
-- call); this swaps them in and resets meta, atomically.
--
-- KEYS: meta, pc, cusip, pcStage, cusipStage
-- ARGV: gen, offset
-- returns {'OK', newFlushSeq} | {'FENCED', storedGen}

local function num(v, d)
  if v then return tonumber(v) end
  return d
end

local meta, pc, cu, pcS, cuS = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local gen, off = tonumber(ARGV[1]), tonumber(ARGV[2])
if not (gen and off) then return redis.error_reply('BAD_ARGS') end
local storedGen = num(redis.call('HGET', meta, 'ownerGen'), 0)
if storedGen > gen then return { 'FENCED', storedGen } end
if redis.call('EXISTS', pcS) == 1 then redis.call('RENAME', pcS, pc) else redis.call('UNLINK', pc) end
if redis.call('EXISTS', cuS) == 1 then redis.call('RENAME', cuS, cu) else redis.call('UNLINK', cu) end
local seq = num(redis.call('HGET', meta, 'flushSeq'), 0) + 1
redis.call('HSET', meta, 'ownerGen', gen, 'offset', off, 'flushSeq', seq)
return { 'OK', seq }
