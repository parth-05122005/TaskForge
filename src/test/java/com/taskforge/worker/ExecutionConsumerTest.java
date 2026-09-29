package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import com.taskforge.job.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.support.Acknowledgment;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ExecutionConsumerTest {
    @Test void durableCancellationRequestInterruptsCooperativeHandlerAndAcknowledgesAfterFinish() throws Exception {
        ExecutionWorkflowService workflow=mock(ExecutionWorkflowService.class);
        when(workflow.claim(any(JobMessage.class),eq("worker-1"))).thenReturn("lease");
        when(workflow.isCancellationRequested(9L)).thenReturn(true);
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        StringRedisTemplate redis=mock(StringRedisTemplate.class);
        JobHandler blocking=blockingHandler("BLOCKING");
        ValueOperations<String,String> values=mock(ValueOperations.class);when(redis.opsForValue()).thenReturn(values);when(values.setIfAbsent(anyString(),anyString(),any(Duration.class))).thenReturn(true);
        ExecutionConsumer consumer=new ExecutionConsumer(redis,new DemoHandlers(java.util.List.of(blocking)),runtime,workflow,new ObjectMapper());
        Acknowledgment acknowledgment=mock(Acknowledgment.class);

        try {
            consumer.receive("{\"jobId\":9,\"executionId\":99,\"runNumber\":1,\"type\":\"BLOCKING\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":5}",acknowledgment);
            verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.CANCELLED),eq(false),contains("cancellation"));
            verify(acknowledgment).acknowledge();
        } finally {consumer.shutdown();}
    }

    @Test void timeoutInterruptsCooperativeHandlerAndRecordsTimeoutBeforeAcknowledgement() throws Exception {
        ExecutionWorkflowService workflow=mock(ExecutionWorkflowService.class);
        when(workflow.claim(any(JobMessage.class),eq("worker-1"))).thenReturn("lease");
        when(workflow.isCancellationRequested(9L)).thenReturn(false);
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        StringRedisTemplate redis=mock(StringRedisTemplate.class);
        ValueOperations<String,String> values=mock(ValueOperations.class);when(redis.opsForValue()).thenReturn(values);when(values.setIfAbsent(anyString(),anyString(),any(Duration.class))).thenReturn(true);
        ExecutionConsumer consumer=new ExecutionConsumer(redis,new DemoHandlers(java.util.List.of(blockingHandler("BLOCKING"))),runtime,workflow,new ObjectMapper());
        Acknowledgment acknowledgment=mock(Acknowledgment.class);

        try {
            consumer.receive("{\"jobId\":9,\"executionId\":99,\"runNumber\":1,\"type\":\"BLOCKING\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":1}",acknowledgment);
            verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.TIMEOUT),eq(true),contains("timeout"));
            verify(acknowledgment).acknowledge();
        } finally {consumer.shutdown();}
    }

    @Test void handlerIgnoringInterruptGetsTerminalNoRetryOutcomeAfterBoundedGrace() throws Exception {
        ExecutionWorkflowService workflow=mock(ExecutionWorkflowService.class);
        when(workflow.claim(any(JobMessage.class),eq("worker-1"))).thenReturn("lease");
        when(workflow.isCancellationRequested(9L)).thenReturn(false);
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        StringRedisTemplate redis=mock(StringRedisTemplate.class);
        ValueOperations<String,String> values=mock(ValueOperations.class);when(redis.opsForValue()).thenReturn(values);when(values.setIfAbsent(anyString(),anyString(),any(Duration.class))).thenReturn(true);
        CountDownLatch releaseHandler=new CountDownLatch(1);
        JobHandler nonCooperative=new JobHandler(){public String type(){return "NON_COOPERATIVE";}public void execute(JobMessage ignored){while(releaseHandler.getCount()>0){try{releaseHandler.await();}catch(InterruptedException ignoredInterrupt){/* emulate unsafe handler */}}}};
        ExecutionConsumer consumer=new ExecutionConsumer(redis,new DemoHandlers(java.util.List.of(nonCooperative)),runtime,workflow,new ObjectMapper(),1);
        Acknowledgment acknowledgment=mock(Acknowledgment.class);
        try {
            consumer.receive("{\"jobId\":9,\"executionId\":99,\"runNumber\":1,\"type\":\"NON_COOPERATIVE\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":1}",acknowledgment);
            var ordered=inOrder(workflow,acknowledgment);
            ordered.verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.TIMEOUT),eq(false),contains("ignored timeout interruption"));
            ordered.verify(acknowledgment).acknowledge();
        } finally {releaseHandler.countDown();consumer.shutdown();}
    }

    private static JobHandler blockingHandler(String type){return new JobHandler(){public String type(){return type;}public void execute(JobMessage ignored)throws InterruptedException{Thread.sleep(10_000);}};}
}
