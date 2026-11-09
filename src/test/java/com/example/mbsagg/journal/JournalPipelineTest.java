package com.example.mbsagg.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.example.mbsagg.journal.config.BusinessCalendar;
import com.example.mbsagg.journal.config.MbsAggProperties;
import com.example.mbsagg.journal.redis.CheckpointStore;
import com.example.mbsagg.journal.support.TestRedis;
import com.example.mbsagg.journal.support.Views;

import tools.jackson.databind.JsonNode;

/**
 * The whole Spring Boot application, both roles in one context, against an embedded Kafka broker and a real Redis:
 * feeds go through the transactional sequencer into the journal, and the aggregator publishes the containers in its
 * {@code @Transactional} flushes.
 */
@SpringBootTest(properties = {"mbsagg.exit-on-listener-failure=false", "logging.level.org.apache.kafka=WARN",
        "logging.level.kafka=WARN", "logging.level.state.change.logger=WARN"})
@ActiveProfiles({"sequencer", "aggregator"})
@EmbeddedKafka(partitions = 2,
        topics = {"mbs.positions", "mbs.prices", "mbs.risk", "mbs.assumptions", "mbs.journal", "mbs.containers"},
        brokerProperties = {"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"})
@EnabledIf("com.example.mbsagg.journal.support.TestRedis#available")
class JournalPipelineTest {
    private static final String CUSIP = "3140X0001";
    private static final String INV1 = "P1|INV1|" + CUSIP;
    private static final String INV2 = "P1|INV2|" + CUSIP;

    @DynamicPropertySource
    static void redis(DynamicPropertyRegistry registry) {
        TestRedis redis = TestRedis.get();
        redis.reset();
        registry.add("spring.data.redis.host", () -> redis.host);
        registry.add("spring.data.redis.port", () -> redis.port);
    }

    @Autowired
    EmbeddedKafkaBroker broker;
    @Autowired
    BusinessCalendar calendar;
    @Autowired
    CheckpointStore store;
    @Autowired
    MbsAggProperties properties;

    @Test
    void feedsBecomeContainersAndAPriceFansOutToEveryPosition() throws Exception {
        String today = calendar.today();
        Map<String, JsonNode> latest = new HashMap<>();
        try (Producer<String, String> feeds = feedProducer(); Consumer<String, String> output = outputConsumer()) {
            feeds.send(new ProducerRecord<>("mbs.positions", CUSIP,
                    "{\"cusip\":\"" + CUSIP + "\",\"productId\":\"P1\",\"inventoryId\":\"INV1\",\"face\":1000}"));
            feeds.send(new ProducerRecord<>("mbs.positions", CUSIP,
                    "{\"cusip\":\"" + CUSIP + "\",\"productId\":\"P1\",\"inventoryId\":\"INV2\",\"face\":2000}"));
            feeds.send(new ProducerRecord<>("mbs.positions", "bad", "not json"));     // dead-lettered, not fatal
            feeds.send(new ProducerRecord<>("mbs.prices", CUSIP, "{\"cusip\":\"" + CUSIP + "\",\"px\":99.5}"));
            feeds.send(new ProducerRecord<>("mbs.risk", CUSIP, "{\"cusip\":\"" + CUSIP + "\",\"oad\":4.2}"));
            feeds.send(new ProducerRecord<>("mbs.assumptions", CUSIP, "{\"cusip\":\"" + CUSIP + "\",\"cpr\":6.0}"));
            feeds.flush();

            awaitContainers(output, latest, 99.5);
            assertThat(latest.get(INV1).get("position").get("face").asLong()).isEqualTo(1000);
            assertThat(latest.get(INV2).get("position").get("face").asLong()).isEqualTo(2000);

            // one price, every open position of the CUSIP
            feeds.send(new ProducerRecord<>("mbs.prices", CUSIP, "{\"cusip\":\"" + CUSIP + "\",\"px\":100.25}"));
            feeds.flush();
            awaitContainers(output, latest, 100.25);
        }

        // the Redis snapshot holds both containers
        ShardState snapshot = new ShardState();
        for (int p = 0; p < 2; p++) store.load(today, p, snapshot);
        assertThat(snapshot.views()).containsKeys(INV1, INV2);

        // the committed journal offsets carry the business date as metadata
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(properties.aggregator().groupId()).partitionsToOffsetAndMetadata().get();
            assertThat(committed).isNotEmpty();
            assertThat(committed.values()).allSatisfy(om -> assertThat(om.metadata()).isEqualTo(today));
        }
    }

    private static void awaitContainers(Consumer<String, String> output, Map<String, JsonNode> latest, double price) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ZERO).untilAsserted(() -> {
            for (ConsumerRecord<String, String> r : output.poll(Duration.ofMillis(200))) {
                latest.put(r.key(), Views.parse(r.value()));
            }
            for (String ck : List.of(INV1, INV2)) {
                JsonNode c = latest.get(ck);
                assertThat(c).as(ck).isNotNull();
                assertThat(c.get("complete").asBoolean()).as(ck + " complete").isTrue();
                assertThat(c.get("price").get("px").asDouble()).as(ck + " price").isEqualTo(price);
            }
        });
    }

    private Producer<String, String> feedProducer() {
        Map<String, Object> props = KafkaTestUtils.producerProps(broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<String, String>(props).createProducer();
    }

    private Consumer<String, String> outputConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-output", false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        c.subscribe(List.of("mbs.containers"));
        return c;
    }
}
