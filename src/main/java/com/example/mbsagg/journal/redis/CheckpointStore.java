package com.example.mbsagg.journal.redis;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.example.mbsagg.journal.ShardState;

/**
 * The Redis snapshot of each shard, through three Lua scripts run with {@link StringRedisTemplate} (EVALSHA, falling
 * back to EVAL, so there is no separate deployment step). Keys per shard share the hash tag
 * {@code {bizDate:partition}}:
 * <pre>
 *   agg:{D:p}:meta    hash  ownerGen, flushSeq, offset
 *   agg:{D:p}:pc      hash  containerKey -&gt; position-grain JSON
 *   agg:{D:p}:cusip   hash  cusip -&gt; CUSIP-grain JSON
 * </pre>
 * Redis only speeds up restarts here; Kafka transactions decide what was published.
 */
@Component
public class CheckpointStore {
    private static final int HSET_CHUNK = 1000;

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> CLAIM = script("agg_claim");
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> CHECKPOINT = script("agg_checkpoint");
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SWAP = script("agg_swap");

    /** Result of {@code agg_claim}: the new owner generation and what the snapshot holds. */
    public record Claim(long gen, long flushSeq, long offset) {
    }

    public enum Status { OK, FENCED, GAP }

    /** Status plus the new flushSeq (OK), the stored generation (FENCED) or the stored flushSeq (GAP). */
    public record Result(Status status, long value) {
    }

    private final StringRedisTemplate redis;
    private final HashOperations<String, String, String> hashes;

    public CheckpointStore(StringRedisTemplate redis) {
        this.redis = redis;
        this.hashes = redis.opsForHash();
    }

    @SuppressWarnings("rawtypes")
    private static RedisScript<List> script(String name) {
        return RedisScript.of(new ClassPathResource("redis/" + name + ".lua"), List.class);
    }

    public static String tag(String bizDate, int partition) {
        return "{" + bizDate + ":" + partition + "}";
    }

    public static String meta(String bizDate, int partition) {
        return "agg:" + tag(bizDate, partition) + ":meta";
    }

    public static String slots(String bizDate, int partition) {
        return "agg:" + tag(bizDate, partition) + ":pc";
    }

    public static String cusips(String bizDate, int partition) {
        return "agg:" + tag(bizDate, partition) + ":cusip";
    }

    public Claim claim(String bizDate, int partition) {
        List<?> r = redis.execute(CLAIM, List.of(meta(bizDate, partition)));
        return new Claim(num(r.get(0)), num(r.get(1)), num(r.get(2)));
    }

    public void load(String bizDate, int partition, ShardState into) {
        hashes.entries(cusips(bizDate, partition)).forEach(into::loadCusip);
        hashes.entries(slots(bizDate, partition)).forEach(into::loadSlot);
    }

    public Result checkpoint(String bizDate, int partition, long gen, long expectedFlushSeq, long newOffset,
                             Map<String, String> changedSlots, Map<String, String> changedCusips) {
        List<String> args = new ArrayList<>(6 + 2 * (changedSlots.size() + changedCusips.size()));
        args.add(Long.toString(gen));
        args.add(Long.toString(expectedFlushSeq));
        args.add(Long.toString(newOffset));
        args.add(Integer.toString(changedSlots.size()));
        changedSlots.forEach((k, v) -> {
            args.add(k);
            args.add(v);
        });
        args.add(Integer.toString(changedCusips.size()));
        changedCusips.forEach((k, v) -> {
            args.add(k);
            args.add(v);
        });
        return result(redis.execute(CHECKPOINT,
                List.of(meta(bizDate, partition), slots(bizDate, partition), cusips(bizDate, partition)),
                args.toArray()));
    }

    /** Rewrites the whole shard: staging keys first (chunked), then {@code agg_swap} renames them in atomically. */
    public Result swap(String bizDate, int partition, long gen, long offset, Map<String, String> allSlots,
                       Map<String, String> allCusips) {
        String slotStage = slots(bizDate, partition) + ":stage:" + gen;
        String cusipStage = cusips(bizDate, partition) + ":stage:" + gen;
        redis.delete(List.of(slotStage, cusipStage));
        writeChunked(slotStage, allSlots);
        writeChunked(cusipStage, allCusips);
        return result(redis.execute(SWAP, List.of(meta(bizDate, partition), slots(bizDate, partition),
                cusips(bizDate, partition), slotStage, cusipStage), Long.toString(gen), Long.toString(offset)));
    }

    private void writeChunked(String key, Map<String, String> all) {
        Map<String, String> chunk = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : all.entrySet()) {
            chunk.put(e.getKey(), e.getValue());
            if (chunk.size() == HSET_CHUNK) {
                hashes.putAll(key, chunk);
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) hashes.putAll(key, chunk);
    }

    private static Result result(List<?> r) {
        return new Result(Status.valueOf(String.valueOf(r.get(0))), num(r.get(1)));
    }

    private static long num(Object o) {
        return o instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(o));
    }
}
