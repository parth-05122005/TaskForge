package com.taskforge.common;

import com.taskforge.job.JobRepository;
import com.taskforge.job.OutboxRepository;
import com.taskforge.worker.WorkerRepository;
import com.taskforge.worker.WorkerStatus;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Instant;

class TaskForgeMetricsTest {
    @Test void executionDurationUsesOneLowCardinalityTimer(){
        SimpleMeterRegistry registry=new SimpleMeterRegistry();
        TaskForgeMetrics metrics=new TaskForgeMetrics(registry,mock(JobRepository.class),mock(WorkerRepository.class));

        metrics.recordDuration(10);
        metrics.recordDuration(20);

        var timers=registry.find("taskforge.job.execution.duration").timers();
        assertEquals(1,timers.size());
        Timer timer=timers.iterator().next();
        assertEquals(2,timer.count());
        assertFalse(timer.getId().getTags().stream().anyMatch(tag->tag.getKey().equals("type")));
    }

    @Test void outboxGaugesExposePendingVolumeAndAge(){
        SimpleMeterRegistry registry=new SimpleMeterRegistry();
        OutboxRepository outbox=mock(OutboxRepository.class);
        when(outbox.countByPublishedAtIsNull()).thenReturn(3L);
        when(outbox.findOldestPendingCreatedAt()).thenReturn(Instant.now().minusSeconds(40));
        new TaskForgeMetrics(registry,mock(JobRepository.class),mock(WorkerRepository.class),outbox);

        assertEquals(3.0,registry.get("taskforge.outbox.pending").gauge().value());
        double oldestAge=registry.get("taskforge.outbox.oldest.pending.age.seconds").gauge().value();
        assertTrue(oldestAge>=40&&oldestAge<42,()->"Unexpected oldest pending age: "+oldestAge);
    }

    @Test void workerGaugesExposeGracefullyOfflineWorkers(){
        SimpleMeterRegistry registry=new SimpleMeterRegistry();
        WorkerRepository workers=mock(WorkerRepository.class);
        when(workers.countByStatus(WorkerStatus.OFFLINE)).thenReturn(2L);
        new TaskForgeMetrics(registry,mock(JobRepository.class),workers);

        assertEquals(2.0,registry.get("taskforge.workers.offline").gauge().value());
    }
}
