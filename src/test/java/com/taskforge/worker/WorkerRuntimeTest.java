package com.taskforge.worker;

import com.taskforge.job.JobExecutionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;

import static org.mockito.Mockito.*;

class WorkerRuntimeTest {
    @Test void shutdownMarksWorkerOfflineAndRemovesItsRedisHeartbeat() {
        WorkerRepository workers = mock(WorkerRepository.class);
        JobExecutionRepository executions = mock(JobExecutionRepository.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        WorkerRuntime runtime = new WorkerRuntime(workers, executions, redis);

        runtime.shutdown();

        verify(workers).markOffline(runtime.id());
        verify(redis).delete("taskforge:worker:" + runtime.id() + ":heartbeat");
    }

    @Test void heartbeatLeaseCeilingIncludesMaximumHandlerStopGrace() {
        WorkerRepository workers = mock(WorkerRepository.class);
        JobExecutionRepository executions = mock(JobExecutionRepository.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked") ValueOperations<String,String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        WorkerRuntime runtime = new WorkerRuntime(workers, executions, redis, 60);

        runtime.heartbeat();

        verify(executions).extendWorkerLeases(eq(runtime.id()), any(Instant.class), eq(80));
    }
}
