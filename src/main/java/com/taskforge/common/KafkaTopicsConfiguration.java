package com.taskforge.common;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicsConfiguration {
    @Bean NewTopic executeJobsTopic(@Value("${taskforge.kafka.topic-partitions:6}") int partitions,
                                   @Value("${taskforge.kafka.topic-replication-factor:1}") short replicationFactor) {
        return topic("taskforge.jobs.execute", partitions, replicationFactor);
    }

    @Bean NewTopic jobEventsTopic(@Value("${taskforge.kafka.topic-partitions:6}") int partitions,
                                  @Value("${taskforge.kafka.topic-replication-factor:1}") short replicationFactor) {
        return topic("taskforge.jobs.events", partitions, replicationFactor);
    }

    @Bean NewTopic deadLetterTopic(@Value("${taskforge.kafka.topic-partitions:6}") int partitions,
                                   @Value("${taskforge.kafka.topic-replication-factor:1}") short replicationFactor) {
        return topic("taskforge.jobs.dlq", partitions, replicationFactor);
    }

    private static NewTopic topic(String name, int partitions, short replicationFactor) {
        if (partitions < 1 || replicationFactor < 1) {
            throw new IllegalArgumentException("Kafka topic partitions and replication factor must be positive");
        }
        return new NewTopic(name, partitions, replicationFactor);
    }
}
