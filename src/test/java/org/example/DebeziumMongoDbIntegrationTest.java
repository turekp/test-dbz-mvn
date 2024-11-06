package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.MongoDbContainer;
import io.debezium.testing.testcontainers.util.PortResolver;
import okhttp3.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
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

    public static final Integer PARTITIONS = 6;

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer()
            .withNetwork(network)
            .withEmbeddedZookeeper()
            .withNetworkAliases("kafka")
            .withEnv("KAFKA_NUM_PARTITIONS", String.valueOf(PARTITIONS));

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
        String fullUrl = debeziumContainer.getTarget() + "/admin/loggers/io.debezium";
        Request request = new Request.Builder().url(fullUrl).put(RequestBody.create("{ \"level\" : \"TRACE\" }", MediaType.get("application/json; charset=utf-8"))).build();
        try (Response response = new OkHttpClient().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IllegalStateException(response.message());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
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
                // -- proposed addons
                .with("transforms", "outbox")
                .with("transforms.outbox.type", "io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter")
                .with("transforms.outbox.collection.expand.json.payload","true")
                .with("transforms.outbox.collection.field.event.key","aggregateId")
                .with("transforms.outbox.route.by.field", "aggregateType")
                // ... ignore value of aggregateType as we put all events in one topic
                .with("transforms.outbox.route.topic.regex", "(.*)")
                .with("transforms.outbox.route.topic.replacement", "event_outbox")
                // ... add these fields to the message, otherwise it is just the value of payload
                // ... this is a problem as we need to mirror the fields in the outbox collection which are not payload
                .with("transforms.outbox.collection.fields.additional.placement",
                        "aggregateType:envelope,aggregateId:envelope,eventType:envelope,testAutomation:envelope,createdDate:envelope")
                // --
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

        int aggregates = 50;
        int messagesPerAggregate = 10;
        IntStream.range(0, aggregates).forEach(i -> {
            UUID currentUuid = UUID.randomUUID();
            IntStream.range(0, messagesPerAggregate).forEach(j -> {
                Map<String, ?> outboxEvent = Map.of(
                        "aggregateId", currentUuid.toString(),
                        "aggregateType", "test-event",
                        "eventType", "CREATED",
                        "testAutomation", "false",
                        "createdDate", LocalDateTime.now().toString(),
                        "payload", Map.of("field", "value")
                );
                Document saved = mongoTemplate.save(new Document(outboxEvent), "event_outbox");
                mongoTemplate.remove(saved, "event_outbox");
            });
        });

        Map<UUID, Integer> idToPartition = Maps.newHashMap();
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(60));
        Iterator<ConsumerRecord<String, String>> record = records.records("event_outbox").iterator();
        while (record.hasNext()) {
            ConsumerRecord<String, String> next = record.next();
            JsonNode node = objectMapper.readTree(next.value());
            JsonNode after = node.get("after");
            if (after == null) {
                after = node;
            } else if (after.isTextual()) {
                after = objectMapper.readTree(after.textValue());
            }
            String aggregateId = after.get("aggregateId").textValue();
            Integer p = idToPartition.put(UUID.fromString(aggregateId), next.partition());
            if (p!=null) assertEquals(p, next.partition(), "Partitions must be the same for any message related to " + aggregateId);
        }

        assertThat(idToPartition.keySet()).as(() -> "must have received messages for aggregates").hasSize(aggregates);
        assertThat(new HashSet<>(idToPartition.values())).as(() -> "must have spread messages across partitions").hasSize(PARTITIONS);
    }
}
