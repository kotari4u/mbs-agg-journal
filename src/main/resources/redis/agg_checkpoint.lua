-- agg_checkpoint: incremental checkpoint of one shard. Writes the changed containers, the changed CUSIP states and the
-- new offset, atomically and fenced.
--
-- Per flush the aggregator writes Redis FIRST (this script), then commits its Kafka transaction (output + offset C).
-- So Redis is normally at or ahead of Kafka; on restart the owner resumes at C and republishes what the records
-- between C and S touch.
--
--   agg:{D:p}:meta    hash  ownerGen, flushSeq, offset (S)
--   agg:{D:p}:pc      hash  containerKey -> position-grain JSON
--   agg:{D:p}:cusip   hash  cusip -> CUSIP-grain JSON (price, risk, assumptions with versions)
--
-- KEYS: meta, pc, cusip
-- ARGV: gen, expectedFlushSeq, newOffset, nPc, (k, v) * nPc, nCusip, (k, v) * nCusip
-- returns {'OK', newFlushSeq} | {'FENCED', storedGen} | {'GAP', storedFlushSeq}

local CHUNK = 500 -- field/value pairs per HSET, keeps unpack() well inside Lua's stack limit

local function num(v, d)
  if v then return tonumber(v) end
  return d
end

-- HSET key with args[first..last] (pairs), in chunks.
local function hset_range(key, args, first, last)
  local i = first
  while i <= last do
    local j = math.min(i + 2 * CHUNK - 1, last)
    redis.call('HSET', key, unpack(args, i, j))
    i = j + 1
  end
end

local meta, pc, cu = KEYS[1], KEYS[2], KEYS[3]
local gen, expected, newOff, nPc = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
if not (gen and expected and newOff and nPc) then return redis.error_reply('BAD_ARGS header') end
local iCu = 5 + 2 * nPc
local nCu = tonumber(ARGV[iCu])
if not nCu or #ARGV ~= iCu + 2 * nCu then return redis.error_reply('BAD_ARGS counts') end

-- every check happens before the first write: Redis does not roll back partial writes
local storedGen = num(redis.call('HGET', meta, 'ownerGen'), 0)
if storedGen > gen then return { 'FENCED', storedGen } end
local storedSeq = num(redis.call('HGET', meta, 'flushSeq'), 0)
local storedOff = num(redis.call('HGET', meta, 'offset'), -1)
if storedGen < gen or storedSeq ~= expected or newOff < storedOff then
  return { 'GAP', storedSeq } -- Redis lost writes (failover) or lost our claim: caller resyncs
end

if nPc > 0 then hset_range(pc, ARGV, 5, 4 + 2 * nPc) end
if nCu > 0 then hset_range(cu, ARGV, iCu + 1, iCu + 2 * nCu) end
local seq = storedSeq + 1
redis.call('HSET', meta, 'offset', newOff, 'flushSeq', seq)
return { 'OK', seq }
