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

    @Test void redisHeartbeatExpiryExpandsWhenHeartbeatCadenceIsLongerThanDefaultThreshold() {
        WorkerRepository workers=mock(WorkerRepository.class);
        JobExecutionRepository executions=mock(JobExecutionRepository.class);
        StringRedisTemplate redis=mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked") ValueOperations<String,String> values=mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        WorkerRuntime runtime=new WorkerRuntime(workers,executions,redis,5,60_000,20);

        runtime.heartbeat();

        verify(values).set(eq("taskforge:worker:"+runtime.id()+":heartbeat"),anyString(),eq(java.time.Duration.ofSeconds(180)));
    }

    @Test void livenessThresholdUsesAtLeastThreeHeartbeatIntervals() {
        org.junit.jupiter.api.Assertions.assertEquals(20L,new WorkerLivenessSettings(5_000,20).deadAfterSeconds());
        org.junit.jupiter.api.Assertions.assertEquals(180L,new WorkerLivenessSettings(60_000,20).deadAfterSeconds());
    }

    @Test void recoveredWorkerRemainsOfflineAfterItsJobLeaseIsReconciled() {
        Worker worker=new Worker("offline-worker","host");
        worker.assign(7L);
        worker.markOffline();

        worker.recovered(java.time.Duration.ofSeconds(20));

        org.junit.jupiter.api.Assertions.assertEquals(WorkerStatus.OFFLINE,worker.getStatus());
        org.junit.jupiter.api.Assertions.assertNull(worker.getCurrentJobId());
    }

    @Test void handlerExitClearsOnlyTheWorkerAssignmentForThatJob(){
        WorkerRepository workers=mock(WorkerRepository.class);
        Worker assigned=new Worker("worker-1","host");assigned.assign(42L);
        when(workers.findById("worker-1")).thenReturn(java.util.Optional.of(assigned));
        WorkerRuntime runtime=new WorkerRuntime(workers,mock(JobExecutionRepository.class),mock(StringRedisTemplate.class));

        runtime.handlerExited(99L);
        org.junit.jupiter.api.Assertions.assertEquals(WorkerStatus.BUSY,assigned.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(42L,assigned.getCurrentJobId());

        // The runtime's generated ID is unique, so explicitly map it to the
        // worker record to verify the successful, matching completion path.
        when(workers.findById(runtime.id())).thenReturn(java.util.Optional.of(assigned));
        runtime.handlerExited(42L);
        org.junit.jupiter.api.Assertions.assertEquals(WorkerStatus.ONLINE,assigned.getStatus());
        org.junit.jupiter.api.Assertions.assertNull(assigned.getCurrentJobId());
    }
}
