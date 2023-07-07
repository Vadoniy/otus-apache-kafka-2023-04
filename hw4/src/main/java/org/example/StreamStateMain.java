package org.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamStateMain {
    private static final Logger log = LoggerFactory.getLogger(StreamStateMain.class);
    private static final String TOPIC_EVENTS = "events";
    private static final Map<String, Object> streamProperties = new HashMap<>(2);

    static {
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AdminService.HOST);
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "HomeWork4");
    }

    public static void main(String[] args) {
        final var admin = new AdminService();
        admin.removeAllTopics();
        admin.createTopics(List.of(new NewTopic(TOPIC_EVENTS, 2, (short) 1)));
        final var stringSerde = Serdes.String();
        final var builder = new StreamsBuilder();

        var twentySeconds = Duration.ofSeconds(10);
        var fiveMinutes = Duration.ofMinutes(5);
        KTable<Windowed<String>, Long> count = builder
                .stream(TOPIC_EVENTS, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> log.debug("Received message key-value: {}:{}", k, v))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(twentySeconds, fiveMinutes))
                .count();

        count.toStream()
                .foreach((k, v) -> log.info("Amount of records with key {}: {}", k.key(), v));

        final var topology = builder.build();

        try (var kafkaStreams = new KafkaStreams(topology, new StreamsConfig(streamProperties))) {
            log.info("App Started");
            kafkaStreams.start();
            Thread.sleep(fiveMinutes);
            log.info("Shutting down now");
        } catch (InterruptedException e) {
            throw new RuntimeException("**it happens", e);
        }
    }
}