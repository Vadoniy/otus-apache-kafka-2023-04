package org.example;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AdminService {
    private static final Logger log = LoggerFactory.getLogger(AdminService.class);
    private static final Map<String, Object> adminProperties = new HashMap<>(1);
    public static final String TOPIC_1 = "topic1";
    public static final String TOPIC_2 = "topic2";
    public static final String HOST = "localhost:9091";
    public static final String HOST_PRODUCER = "localhost:9092";
    public static final Set<String> TOPIC_SET = Set.of(TOPIC_1, TOPIC_2);

    static {
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
    }

    public void reCreateTopics() {
        removeAllTopics();
        final var newTopics = TOPIC_SET.stream()
                .map(topicName -> new NewTopic(topicName, 1, (short) 1))
                .collect(Collectors.toSet());
        createTopics(newTopics);
    }

    public void removeAllTopics() {
        final var sb = new StringBuffer();
        try (var admin = Admin.create(adminProperties)) {
            admin.listTopics().names()
                    .thenApply(strings -> admin.deleteTopics(strings).topicNameValues())
                    .thenApply(sb::append)
                    .get();
        } catch (Exception e) {
            log.error("Failed to delete topics: {}", sb);
            throw new RuntimeException("Failed to delete topics");
        }
    }

    public KafkaFuture<Set<String>> getTopicsList() {
        try (var admin = Admin.create(adminProperties)) {
            return admin.listTopics().names();
        }
    }

    public void removeTopic(TopicCollection topicsToDelete) {
        try (var admin = Admin.create(adminProperties)) {
            admin.deleteTopics(topicsToDelete);
        } catch (Exception e) {
            log.error("Failed to delete topics: {}", topicsToDelete);
            throw new RuntimeException("Failed to delete topics");
        }
    }

    public void createTopics(Collection<NewTopic> newTopics) {
        try (var admin = Admin.create(adminProperties)) {
            admin.createTopics(newTopics);
        } catch (Exception e) {
            log.error("Failed to create topics: {}", newTopics);
            throw new RuntimeException("Failed to create topics");
        }
    }
}
