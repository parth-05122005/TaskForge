package com.taskforge.job;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OutboxRetentionTest {
    @Test void prunesAConfiguredWindowInBoundedBatches() {
        OutboxRepository repository = mock(OutboxRepository.class);
        Instant before = Instant.now();

        new OutboxRetention(repository, 14).prunePublished();

        Instant after = Instant.now();
        org.mockito.ArgumentCaptor<Instant> cutoff = org.mockito.ArgumentCaptor.forClass(Instant.class);
        verify(repository).deletePublishedBefore(cutoff.capture(), eq(1_000));
        assertTrue(cutoff.getValue().isAfter(before.minusSeconds(14L * 86_400 + 1)));
        assertTrue(cutoff.getValue().isBefore(after.minusSeconds(14L * 86_400 - 1)));
    }

    @Test void rejectsNonPositiveRetention() {
        assertThrows(IllegalArgumentException.class, () -> new OutboxRetention(mock(OutboxRepository.class), 0));
    }
}
