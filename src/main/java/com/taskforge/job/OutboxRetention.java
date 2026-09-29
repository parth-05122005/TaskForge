package com.taskforge.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Removes only Kafka-acknowledged transport records after the configured replay window. */
@Component
@ConditionalOnProperty(name = "taskforge.role", havingValue = "api", matchIfMissing = true)
public class OutboxRetention {
    private static final Logger log = LoggerFactory.getLogger(OutboxRetention.class);
    private static final int BATCH_SIZE = 1_000;

    private final OutboxRepository outbox;
    private final long retentionDays;

    public OutboxRetention(OutboxRepository outbox,
                           @org.springframework.beans.factory.annotation.Value("${taskforge.outbox-retention-days:14}") long retentionDays) {
        if (retentionDays < 1) throw new IllegalArgumentException("Outbox retention must be at least one day");
        this.outbox = outbox;
        this.retentionDays = retentionDays;
    }

    @Scheduled(fixedDelayString = "${taskforge.outbox-retention-interval:21600000}",
            initialDelayString = "${taskforge.outbox-retention-initial-delay:60000}")
    @Transactional
    public void prunePublished() {
        Instant cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        int removed = outbox.deletePublishedBefore(cutoff, BATCH_SIZE);
        if (removed > 0) log.info("Pruned {} acknowledged outbox records older than {} days", removed, retentionDays);
    }
}
