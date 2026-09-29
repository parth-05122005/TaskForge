package com.taskforge;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import io.lettuce.core.RedisClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker=true)
class InfrastructureContainersTest {
    @Container static final GenericContainer<?> redis=new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    @Container static final KafkaContainer kafka=new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Test void redisStoresEphemeralCoordinationValues(){
        var client=RedisClient.create("redis://"+redis.getHost()+":"+redis.getMappedPort(6379));
        try(var connection=client.connect()) {connection.sync().setex("taskforge:test:heartbeat",10,"alive");assertEquals("alive",connection.sync().get("taskforge:test:heartbeat"));}
        finally{client.shutdown();}
    }

    @Test void kafkaAcceptsPublishedJobMessages() throws Exception {
        Properties properties=new Properties();properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBootstrapServers());properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        try(KafkaProducer<String,String> producer=new KafkaProducer<>(properties)){
            RecordMetadata result=producer.send(new ProducerRecord<>("taskforge.jobs.execute","job-1","integration-ping")).get();
            assertEquals("taskforge.jobs.execute",result.topic());
        }
    }
}
