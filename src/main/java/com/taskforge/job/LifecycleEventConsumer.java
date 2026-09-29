package com.taskforge.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="api",matchIfMissing=true)
public class LifecycleEventConsumer {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(LifecycleEventConsumer.class);
    private final ObjectMapper mapper;

    public LifecycleEventConsumer(ObjectMapper mapper){this.mapper=mapper;}

    @KafkaListener(topics="taskforge.jobs.events",groupId="taskforge-event-notifications")
    public void receive(String payload,Acknowledgment acknowledgment) {
        try {
            JobLifecycleEvent event=mapper.readValue(payload,JobLifecycleEvent.class);
            log.atInfo().addKeyValue("eventId",event.eventId()).addKeyValue("jobId",event.jobId())
                    .addKeyValue("executionId",event.executionId()).addKeyValue("workerId",event.workerId())
                    .addKeyValue("status",event.status()).log("Job lifecycle event consumed");
        } catch(JsonProcessingException malformed) {
            log.atError().setCause(malformed).log("Discarding malformed lifecycle event");
        } finally {
            acknowledgment.acknowledge();
        }
    }
}
