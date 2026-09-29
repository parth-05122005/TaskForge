package com.taskforge.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTopicsConfigurationTest {
    @Test void declaresExecutionEventsAndDeadLetterTopicsWithConfiguredTopology(){
        KafkaTopicsConfiguration configuration=new KafkaTopicsConfiguration();

        var execute=configuration.executeJobsTopic(12,(short)3);
        var events=configuration.jobEventsTopic(12,(short)3);
        var deadLetter=configuration.deadLetterTopic(12,(short)3);

        assertEquals("taskforge.jobs.execute",execute.name());
        assertEquals("taskforge.jobs.events",events.name());
        assertEquals("taskforge.jobs.dlq",deadLetter.name());
        for(var topic:java.util.List.of(execute,events,deadLetter)){
            assertEquals(12,topic.numPartitions());
            assertEquals((short)3,topic.replicationFactor());
        }
    }

    @Test void rejectsInvalidTopicTopology(){
        KafkaTopicsConfiguration configuration=new KafkaTopicsConfiguration();

        assertThrows(IllegalArgumentException.class,()->configuration.executeJobsTopic(0,(short)1));
        assertThrows(IllegalArgumentException.class,()->configuration.deadLetterTopic(6,(short)0));
    }
}
