package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ConsumerMain {
    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);
    private static final Map<String, Object> consumerProperties = new HashMap<>(6);

    static {
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AdminService.HOST);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "CG-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        Comment line 26 and uncomment line 27 to read all records despite commit
//        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
    }

    public static void main(String[] args) {
        try (var consumer = new KafkaConsumer<String, String>(consumerProperties)) {
            consumer.subscribe(AdminService.TOPIC_SET);
            while (true) {
                final var consumerRecords = consumer.poll(Duration.ofMillis(500));
                for (var record : consumerRecords) {
                    log.info("Received message to topic {}: {}", record.topic(), record.value());
                }
                if (!consumerRecords.iterator().hasNext()) {
                    break;
                }
            }
        }
    }
}