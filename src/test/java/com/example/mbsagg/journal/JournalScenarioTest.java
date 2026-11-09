package com.example.mbsagg.journal;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.example.mbsagg.journal.support.Tally;
import com.example.mbsagg.journal.support.TestRedis;
import com.example.mbsagg.journal.support.Verdict;

/**
 * End-to-end restart scenarios for the resume-from-C design, each over many synthetic days
 * ({@code -Dmbsagg.seeds=N}, default 40). The design must be exact in every run; each BUG scenario switches one
 * safeguard off and must be caught.
 */
class JournalScenarioTest {
    private static TestRedis redis;

    @BeforeAll
    static void startRedis() {
        redis = TestRedis.get();
    }

    @FunctionalInterface
    private interface Run {
        Verdict run(long seed);
    }

    private static Tally run(String name, Run run) {
        Tally t = new Tally(name);
        for (long seed = 1; seed <= JournalSimulation.seeds(); seed++) {
            redis.reset();
            t.add(run.run(seed));
        }
        System.out.println(t.row());
        return t;
    }

    // ------------------------------------------------------------------ the design: exact in every run

    @Test
    @DisplayName("Crashes after the Redis write or after the Kafka commit, random restarts")
    void crashes() {
        Tally t = run("Crashes after Redis write or Kafka commit",
                seed -> JournalSimulation.crashes(redis, seed, 0.03, Safeguards.ALL));
        assertTrue(t.allCorrect(), t.row());
    }

    @Test
    @DisplayName("Old owner stalls after its Redis write, before its Kafka commit")
    void zombieAfterRedisWrite() {
        Tally t = run("Old owner stalls after its Redis write", seed -> JournalSimulation.zombie(redis, seed, true));
        assertTrue(t.allCorrect(), t.row());
    }

    @Test
    @DisplayName("Old owner stalls before its Redis write")
    void zombieBeforeRedisWrite() {
        Tally t = run("Old owner stalls before its Redis write", seed -> JournalSimulation.zombie(redis, seed, false));
        assertTrue(t.allCorrect(), t.row());
    }

    @Test
    @DisplayName("Redis loses recent writes (Redis behind Kafka)")
    void redisLosesWrites() {
        Tally t = run("Redis loses recent writes", seed -> JournalSimulation.redisLoss(redis, seed, Safeguards.ALL));
        assertTrue(t.allCorrect(), t.row());
    }

    @Test
    @DisplayName("Redis wiped mid-day")
    void redisWiped() {
        Tally t = run("Redis wiped mid-day", seed -> JournalSimulation.redisWiped(redis, seed));
        assertTrue(t.allCorrect(), t.row());
    }

    // ------------------------------------------------------------------ each safeguard removed: must break

    @Test
    @DisplayName("BUG: no republish of C..S on resume")
    void bugNoRepublish() {
        Tally t = run("BUG: no republish of C..S on resume",
                seed -> JournalSimulation.crashes(redis, seed, 0.03, new Safeguards(true, false, true)));
        assertTrue(t.downstream() < t.runs(), "expected changes that never reach downstream: " + t.row());
    }

    @Test
    @DisplayName("BUG: trust Redis even when it is behind Kafka")
    void bugTrustRedisBehindKafka() {
        Tally t = run("BUG: trust Redis even when behind Kafka",
                seed -> JournalSimulation.redisLoss(redis, seed, new Safeguards(true, true, false)));
        assertTrue(t.state() < t.runs(), "expected lost state: " + t.row());
    }

    @Test
    @DisplayName("BUG: no version check, sequencer re-sends")
    void bugNoVersionCheck() {
        Tally t = run("BUG: no version check, sequencer re-sends",
                seed -> JournalSimulation.crashes(redis, seed, 0.03, new Safeguards(false, true, true)));
        // The sequencer re-sends the last few records and then the newer ones again, so the final state still ends
        // right; what breaks is the output in between, which briefly goes back to older values.
        assertTrue(t.monotonic() < t.runs(), "expected re-sent records to publish older values: " + t.row());
    }
}
