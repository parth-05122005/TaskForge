package com.taskforge.common;

import com.taskforge.job.JobRepository;
import com.taskforge.worker.WorkerRepository;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

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
}
