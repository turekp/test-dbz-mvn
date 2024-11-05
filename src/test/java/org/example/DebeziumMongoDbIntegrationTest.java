package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.MongoDbContainer;
import io.debezium.testing.testcontainers.util.PortResolver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import io.debezium.testing.testcontainers.DebeziumContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonDeserializer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
        "spring.data.mongodb.database=testdb",
        "spring.kafka.consumer.group-id=test-group",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
public class DebeziumMongoDbIntegrationTest {

    static final Network network = Network.newNetwork();

    @Container
    static MongoDbContainer mongoDBContainer = MongoDbContainer.node().name("mongo")
            .replicaSet("rs0")
            .portResolver(new PortResolver() {
                @Override
                public int resolveFreePort() {
                    return 27017;
                }

                @Override
                public void releasePort(int port) {

                }
            })
            .build()
            .withExposedPorts(27017)
            .withNetworkAliases("mongo")
            .withNetwork(network);

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer()
            .withNetwork(network)
            .withEmbeddedZookeeper()
            .withNetworkAliases("kafka")
            .withEnv("KAFKA_NUM_PARTITIONS", "3");

    @Container
    static DebeziumContainer debeziumContainer = new DebeziumContainer("debezium/connect:2.4.2.Final")
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(f -> System.out.println(" -- " + f.getUtf8StringWithoutLineEnding()))
            .withExposedPorts(8083)
            .dependsOn(kafkaContainer);

    @BeforeAll
    static void setUp() {
        mongoDBContainer.initReplicaSet(false, mongoDBContainer.getNamedAddress());
        ConnectorConfiguration config = ConnectorConfiguration.forMongoDbContainer(mongoDBContainer);
        debeziumContainer.registerConnector("mongo", config
                .with("tasks.max", "1")
                .with("key.converter.schemas.enable", "false")
                .with("value.converter.schemas.enable", "false")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("topic.prefix", "prefix")
                .with("sanitize.field.names", "true")
                .with("database.include.list", "testdb")
                .with("collection.include.list", "testdb.event_outbox")
                .with("skipped.operations", "d")
                .with("transforms", "dropPrefix")
                .with("transforms.dropPrefix.type", "org.apache.kafka.connect.transforms.RegexRouter")
                .with("transforms.dropPrefix.regex", "(.*)")
                .with("transforms.dropPrefix.replacement", "event_outbox")
                .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                .with("mongodb.connection.string", mongoDbUri(mongoDBContainer.getClientAddress().toString()))
        );
    }

    private static @NotNull String mongoDbUri(String address) {
        return "mongodb://" + address + "/testdb?replicaSet=rs0";
    }

    @DynamicPropertySource
    static void setProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> mongoDbUri("localhost:" + mongoDBContainer.getFirstMappedPort()));
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    @Autowired
    MongoTemplate mongoTemplate;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testOutboxToKafka() throws JsonProcessingException {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaContainer.getBootstrapServers(), "test-group", "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = (KafkaConsumer) new DefaultKafkaConsumerFactory<>(consumerProps).createConsumer();
        consumer.subscribe(Collections.singleton("event_outbox"));

        int aggregates = 10;
        int messagesPerAggregate = 10;
        IntStream.range(0, aggregates).forEach(i -> {
            UUID currentUuid = UUID.randomUUID();
            IntStream.range(0, messagesPerAggregate).forEach(j -> {
                mongoTemplate.remove(mongoTemplate.save(new Document(Map.of("aggregateId", currentUuid.toString(), "type", "test-event", "payload", "{}")), "event_outbox"), "event_outbox");
            });
        });

        Map<UUID, Integer> idToPartition = Maps.newHashMap();
        Iterator<ConsumerRecord<String, String>> record = KafkaTestUtils.getRecords(consumer).records("event_outbox").iterator();
        while (record.hasNext()) {
            ConsumerRecord<String, String> next = record.next();
            JsonNode node = objectMapper.readTree(next.value());
            JsonNode after = node.get("after");
            if (after.isTextual()) {
                after = objectMapper.readTree(after.textValue());
            }
            String aggregateId = after.get("aggregateId").textValue();
            idToPartition.compute(UUID.fromString(aggregateId), (k, p) -> {
                if (p!=null) assertEquals(p, next.partition(), "Partitions must be the same for any message related to " + aggregateId);
                return next.partition();
            });
        }

        System.out.println(idToPartition);
    }
}
