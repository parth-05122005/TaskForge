package com.taskforge.job;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {
    @SuppressWarnings({"unchecked","rawtypes"})
    private KafkaTemplate<String,String> kafkaTemplate(){return (KafkaTemplate)mock(KafkaTemplate.class);}

    @Test void marksOutboxEventDeliveredOnlyAfterBrokerAck() {
        OutboxRepository repository=mock(OutboxRepository.class);
        KafkaTemplate<String,String> kafka=kafkaTemplate();
        OutboxMessage pending=new OutboxMessage("taskforge.jobs.execute","job-9","{}");
        when(repository.lockPending(50)).thenReturn(List.of(pending));
        when(kafka.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.completedFuture(null));

        new OutboxPublisher(repository,kafka,2,32).publishPending();

        assertTrue(pending.isPublished());
        verify(kafka).send("taskforge.jobs.execute","job-9","{}");
    }

    @Test void leavesFailedPublishPendingAndSchedulesBackoff() {
        OutboxRepository repository=mock(OutboxRepository.class);
        KafkaTemplate<String,String> kafka=kafkaTemplate();
        OutboxMessage pending=new OutboxMessage("taskforge.jobs.execute","job-9","{}");
        when(repository.lockPending(50)).thenReturn(List.of(pending));
        when(kafka.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        new OutboxPublisher(repository,kafka,2,32).publishPending();

        assertFalse(pending.isPublished());
        assertEquals(1,pending.getAttemptCount());
        assertTrue(pending.getNextAttemptAt().isAfter(java.time.Instant.now()));
        assertNotNull(pending.getLastError());
    }
}
