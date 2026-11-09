# mbs-agg-journal: journal-topic design, resume from the committed offset

Aggregates the four CUSIP-keyed feeds (position, price, risk, assumptions) into position containers keyed by
`productId|inventoryId|cusip`. One thread owns each journal partition, so updates for a CUSIP never race.

- **The sequencer** re-publishes the four feeds into one journal topic keyed by CUSIP. Each record carries its
  source offset as its version. It writes in Kafka transactions; anything it re-sends is harmless because of the
  version check.
- **The aggregator** runs one `ShardWorker` per assigned journal partition and applies records in memory. Price,
  risk and assumptions are stored once per CUSIP and fanned out to every open position of that CUSIP.
- **Every flush runs in this order:** Redis first (changed state plus offset S, atomic and fenced), then one Kafka
  transaction (output plus offset C, through `sendOffsetsToTransaction`).
- **Restart doesn't seek.** The new owner loads the Redis snapshot and resumes reading at C. Records between C and
  S are already in the snapshot, so they come back STALE, but the containers they touch are published again,
  because their Kafka transaction may never have committed. No outbox is needed. The owner replays from S only if
  Redis is behind Kafka (lost writes), and from the start of the day only if there's no snapshot.

This is the resume-from-C variant you chose. The journal-topic design doc still describes the earlier seek-to-S
version.

## Spring Boot 4 / Spring Kafka

Spring Boot 4.1, Spring Kafka 4.1 (Kafka clients 4.2), Spring Data Redis (Lettuce), Jackson 3, Java 17+.

- **Roles are Spring profiles.** `sequencer` and `aggregator`, one per pod (both can run in one JVM locally). The
  app refuses to start with neither.
- **Sequencer: container-managed Kafka transactions.** `spring.kafka.producer.transaction-id-prefix` makes Boot
  auto-configure a `KafkaTransactionManager`. The sequencer's batch `@KafkaListener` runs inside it, so each poll's
  journal records, dead letters and feed offsets commit together.
- **Aggregator: `@Transactional` flushes.** The listener container doesn't commit (`AckMode.MANUAL`, never
  acknowledged). A flush spans several polls, writes Redis before Kafka, holds a replaying partition's offset at C
  and carries the business date as offset metadata, which container-managed commits can't do. The flush calls
  `KafkaTransactionalSink.commit`, annotated `@Transactional("kafkaTransactionManager")`. It sends the containers
  through `KafkaTemplate` and the offsets through `sendOffsetsToTransaction(groupMetadata)` (KIP-447 fencing).
- **Flush cadence.** Every `flush-every` records, at the end of a batch, or on the container's
  `ListenerContainerIdleEvent` after `flush-interval` with nothing new.
- **Redis.** `StringRedisTemplate` runs the three Lua scripts under `src/main/resources/redis/` (EVALSHA with an
  EVAL fallback). There's no separate function-loading step anymore.
- **Failure.** Both containers use `CommonContainerStoppingErrorHandler`. A fenced or failed instance never flushes
  again. `ExitOnListenerFailure` closes the app with exit code 1, so Kubernetes restarts the pod, as before.

## Layout

```
src/main/resources/
  application.yml                               all settings; maps the original environment variables
  redis/agg_claim.lua, agg_checkpoint.lua, agg_swap.lua    the checkpoint scripts
src/main/java/com/example/mbsagg/journal/
  MbsAggApplication                             @SpringBootApplication, Clock bean, role check
  FeedSource, JournalRecord, Versioned          journal record model (JSON on the journal topic)
  ShardState                                    in-memory state of one partition; container views; snapshot JSON
  ShardWorker                                   assign (restart paths), apply, checkpoint, pending output
  StartPlan                                     RESUME_AT_C | REPLAY_FROM_SNAPSHOT | REPLAY_FROM_START_OF_DAY
  Flusher, TransactionalSink, OffsetCommit      Redis first, then the Kafka transaction
  config/MbsAggProperties                       @ConfigurationProperties("mbsagg"), validated
        BusinessCalendar                        business date from zone + rollover
  redis/CheckpointStore                         the Redis snapshot (StringRedisTemplate + RedisScript)
  kafka/JsonJournalParser                       feed JSON -> journal record
        StartOfDay, TimestampStartOfDay         start-of-day offset via offsetsForTimes
        ExitOnListenerFailure                   exit the pod when a container stops abnormally
  kafka/sequencer/JournalSequencer              @KafkaListener: feeds -> journal, in the container's transaction
                  SequencerConfig               its container factory (Boot configurer) and dead-letter handler
  kafka/aggregator/JournalListener              @KafkaListener on the journal + idle-event flush
                   JournalAggregator            the shard workers, routing, day cutover, rebalance callbacks
                   KafkaTransactionalSink       @Transactional output + offsets
                   AggregatorConfig             its container factory (manual offsets, one consumer thread)
src/test/java/...
  JournalScenarioTest, JournalSimulation        restart scenarios over 40 synthetic days each
  ShardWorkerTest                               small, readable examples of the behaviour
  JournalPipelineTest                           @SpringBootTest + @EmbeddedKafka: both roles end to end
  support/                                      test Redis, simulated journal, oracle
```

## Build and test

```
mvn test                       # needs redis-server 7.x on the PATH
mvn test -Dmbsagg.seeds=10     # quicker: 10 synthetic days per scenario instead of 40
```

The tests start `redis-server` on a free port. To use an existing Redis instead, set
`MBSAGG_TEST_REDIS=host:port`. **The tests run FLUSHALL on it**, so point it at a disposable instance. Without
either, the tests are skipped. The full suite takes about 40 seconds.

The simulation drives the production `ShardWorker` and `Flusher` against a real Redis. Kafka is modelled: the
committed offset, a group generation that fences a stale owner's commit, and the committed output.
`JournalPipelineTest` starts the whole application, both profiles active, against an embedded KRaft broker. It
checks that feeds become complete containers, that a price fans out to every position, that the Redis snapshot is
written and that the committed offsets carry the business date.

## Run

```
mvn package
java -jar target/mbs-agg-journal-1.0.0-SNAPSHOT.jar --spring.profiles.active=sequencer
java -jar target/mbs-agg-journal-1.0.0-SNAPSHOT.jar --spring.profiles.active=aggregator
```

The original environment variables still work. Defaults are in brackets:

- `KAFKA_BOOTSTRAP` [localhost:9092]
- `REDIS_HOST` [localhost], `REDIS_PORT` [6379]
- `POSITION_TOPIC`, `PRICE_TOPIC`, `RISK_TOPIC`, `ASSUMPTION_TOPIC`
- `JOURNAL_TOPIC` [mbs.journal], `OUTPUT_TOPIC` [mbs.containers]
- `ZONE` [America/New_York], `ROLLOVER` [00:00]
- `FLUSH_EVERY` [500], `FLUSH_MS` [100]
- `POD_NAME` [`HOSTNAME`]: static membership and the transactional ID prefix. It must be unique per pod
- `DEAD_LETTER_TOPIC` [unset]: if set, the sequencer publishes unparseable feed records there, in its transaction

Every setting is also a normal Spring Boot property, for example `--mbsagg.aggregator.flush-every=1000`,
`MBSAGG_AGGREGATOR_FLUSH_INTERVAL=250ms` or any `spring.kafka.*` / `spring.data.redis.*` property. See
`MbsAggProperties` and `application.yml`.

Keep the journal's and the source topics' partition counts fixed for the business day, because versions are
source offsets and the shard is the journal partition.

## Things to know before production

- **The business date travels as offset metadata.** Each committed offset carries its date, so a restarted owner
  loads the right day's snapshot. At cutover the aggregator flushes the old day and starts a fresh worker. It
  skips stragglers from an earlier date.
- **Start of day:** `TimestampStartOfDay` finds the day's first offset with `offsetsForTimes`, ten minutes early
  (`mbsagg.aggregator.start-of-day-margin`). It works best with the journal topic on
  `message.timestamp.type=LogAppendTime`.
- **Parser and dead letters:** `JsonJournalParser` assumes field names (`cusip`, `productId`, `inventoryId`,
  optional `bizDate`). Adapt them to your feeds. Without `DEAD_LETTER_TOPIC`, unparseable records are only logged.
- **One consumer thread per aggregator pod.** It owns all of that pod's shard workers. Scale out with pods, up to
  the journal's partition count.
- **Not covered by the tests:** rebalances between live pods, the day cutover, and the restart paths against a real
  Kafka (the simulation covers those against modelled Kafka). Run them against a real cluster, for example with
  Testcontainers, before relying on them.

## Test results

40 synthetic days per scenario: one journal partition, 40 CUSIPs, about 6,000 messages a day, 3% sequencer
re-sends. All 14 tests pass: the 8 scenarios below, the 5 `ShardWorkerTest` examples and `JournalPipelineTest`.

| Scenario | Output matches | State matches | Never goes backwards | Redis matches | Old owner fenced |
|---|---|---|---|---|---|
| Crashes after the Redis write or the Kafka commit | 40/40 | 40/40 | 40/40 | 40/40 | n/a |
| Old owner stalls after its Redis write | 40/40 | 40/40 | 40/40 | 40/40 | 40/40 |
| Old owner stalls before its Redis write | 40/40 | 40/40 | 40/40 | 40/40 | 40/40 |
| Redis loses recent writes (behind Kafka) | 40/40 | 40/40 | 40/40 | 40/40 | n/a |
| Redis wiped mid-day | 40/40 | 40/40 | 40/40 | 40/40 | n/a |
| BUG: no republish of C..S on resume | **14/40** | 40/40 | 40/40 | 40/40 | n/a |
| BUG: trust Redis when it is behind Kafka | **32/40** | **32/40** | **21/40** | **32/40** | n/a |
| BUG: no version check, sequencer re-sends | 40/40 | 40/40 | **25/40** | 40/40 | n/a |
