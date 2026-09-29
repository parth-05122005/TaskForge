package com.taskforge.job;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.TimeUnit;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="api",matchIfMissing=true)
public class OutboxPublisher {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepository outbox;
    private final KafkaTemplate<String,String> kafka;
    private final long retryBaseSeconds;private final long retryMaxSeconds;
    public OutboxPublisher(OutboxRepository outbox,KafkaTemplate<String,String> kafka,@org.springframework.beans.factory.annotation.Value("${taskforge.retry.base-delay:2}") long retryBaseSeconds,@org.springframework.beans.factory.annotation.Value("${taskforge.retry.max-delay:256}") long retryMaxSeconds){this.outbox=outbox;this.kafka=kafka;if(retryBaseSeconds<1||retryMaxSeconds<retryBaseSeconds)throw new IllegalArgumentException("Retry delay settings must satisfy 1 <= base <= max");this.retryBaseSeconds=retryBaseSeconds;this.retryMaxSeconds=retryMaxSeconds;}

    @Scheduled(fixedDelayString="${taskforge.outbox-interval:250}")
    @Transactional
    public void publishPending(){
        for(OutboxMessage message:outbox.lockPending(50)){
            try {
                kafka.send(message.getTopic(),message.getMessageKey(),message.getPayload()).get(10,TimeUnit.SECONDS);
                message.markPublished();
            } catch (Exception e) {
                message.recordFailure(e.getMessage(),retryBaseSeconds,retryMaxSeconds);
                log.atWarn().addKeyValue("outboxId",message.getId()).setCause(e).log("Outbox publish failed; retry scheduled");
                break;
            }
        }
    }
}
