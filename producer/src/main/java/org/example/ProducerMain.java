package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProducerMain {
    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);
    private static final Map<String, Object> producerProperties = new HashMap<>(4);

    static {
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AdminService.HOST_PRODUCER);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "HomeWork3");
    }

    public static void main(String[] args) throws InterruptedException {
        final var adminService = new AdminService();
        adminService.reCreateTopics();

        final var recordsForTopic1 = generateProducerRecords(5, AdminService.TOPIC_1, " ");
        final var recordsForTopic2 = generateProducerRecords(5, AdminService.TOPIC_2, " ");
        final var recordsForTopic1Aborted = generateProducerRecords(2, AdminService.TOPIC_1, " aborted ");
        final var recordsForTopic2Aborted = generateProducerRecords(2, AdminService.TOPIC_2, " aborted ");

        try (final var producer = new KafkaProducer<String, String>(producerProperties)) {
            producer.initTransactions();
            sendInTransaction(recordsForTopic1, producer);
            Thread.sleep(1000);
            sendInTransaction(recordsForTopic2, producer);
            Thread.sleep(1000);
        }
        try (final var producer = new KafkaProducer<String, String>(producerProperties)) {
            producer.initTransactions();
            sendInTransactionAndAbortTransaction(recordsForTopic1Aborted, producer);
            Thread.sleep(1000);
            sendInTransactionAndAbortTransaction(recordsForTopic2Aborted, producer);
            Thread.sleep(1000);
        }
    }

    private static List<ProducerRecord<String, String>> generateProducerRecords(int amountOfRecordsToGenerate,
                                                                                String topicName,
                                                                                String specificString) {
        final var records = new ArrayList<ProducerRecord<String, String>>(amountOfRecordsToGenerate);
        for (int i = 0; i < amountOfRecordsToGenerate; i++) {
            final var value = String.format("generated%svalue for topic %s - %s", specificString, topicName, i);
            records.add(new ProducerRecord<>(topicName, value));
        }
        return records;
    }

    private static void sendInTransaction(Collection<ProducerRecord<String, String>> recordsToSend,
                                          KafkaProducer<String, String> kafkaProducer) {
        kafkaProducer.beginTransaction();
        log.info("Transaction started...");
        for (ProducerRecord<String, String> producerRecord : recordsToSend) {
            kafkaProducer.send(producerRecord);
        }
        log.info("Sent {} records...", recordsToSend.size());
        kafkaProducer.commitTransaction();
        log.info("Transaction committed...");
    }

    private static void sendInTransactionAndAbortTransaction(Collection<ProducerRecord<String, String>> recordsToSend,
                                                             KafkaProducer<String, String> kafkaProducer)
            throws InterruptedException {
        kafkaProducer.beginTransaction();
        log.info("Transaction started...");
        for (ProducerRecord<String, String> producerRecord : recordsToSend) {
            kafkaProducer.send(producerRecord);
        }
        log.info("Sent {} records...", recordsToSend.size());
        Thread.sleep(500);
        kafkaProducer.abortTransaction();
        log.info("Transaction aborted...");
    }
}
