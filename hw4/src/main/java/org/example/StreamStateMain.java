package org.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StreamStateMain {
    private static final Logger log = LoggerFactory.getLogger(StreamStateMain.class);
    private static final String TOPIC_EVENTS = "events";
    private static final String TOPIC_RESULT = "result";
    private static final String STORAGE_NAME = "statistic-by-key-store";
    private static final Map<String, Object> streamProperties = new HashMap<>(2);

    static {
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AdminService.HOST);
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "HomeWork4");
    }

    public static void main(String[] args) {
        final var admin = new AdminService();
        admin.removeAllTopics();
        admin.createTopics(
                List.of(new NewTopic(TOPIC_EVENTS, 2, (short) 1),
                        new NewTopic(TOPIC_RESULT, 2, (short) 1)));
        final var stringSerde = Serdes.String();
        final var builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(STORAGE_NAME), stringSerde, Serdes.Integer()));

        builder
                .stream(TOPIC_EVENTS, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> log.info("Received message key-value: {}:{}", k, v))
                .processValues(CountProcessor::new, STORAGE_NAME)
                .to(TOPIC_RESULT, Produced.with(Serdes.String(), Serdes.String()));

        final var topology = builder.build();

        try (var kafkaStreams = new KafkaStreams(topology, new StreamsConfig(streamProperties))) {
            log.info("App Started");
            kafkaStreams.start();
            Thread.sleep(60000);
            log.info("Shutting down now");
        } catch (InterruptedException e) {
            throw new RuntimeException("**it happens", e);
        }
    }

    public static class CountProcessor implements FixedKeyProcessor<String, String, String> {
        private FixedKeyProcessorContext<String, String> context;
        private KeyValueStore<String, Integer> store;

        @Override
        public void init(FixedKeyProcessorContext<String, String> context) {
            this.context = context;
            this.store = context.getStateStore(STORAGE_NAME);
        }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            final var key = record.key();
            Optional.ofNullable(store.get(key)).ifPresentOrElse(
                    counter -> store.put(key, counter + 1),
                    () -> store.put(key, 1));
            context.forward(record.withValue(String.valueOf(store.get(key))));
        }
    }
}