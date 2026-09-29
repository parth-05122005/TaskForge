package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import com.taskforge.job.JobStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ExecutionConsumerTest {
    @Test void malformedMessageIsDeadLetteredBeforeSourceAcknowledgement() throws Exception {
        ExecutionWorkflowService workflow=mock(ExecutionWorkflowService.class);
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        KafkaTemplate<String,String> kafka=mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        ExecutionConsumer consumer=new ExecutionConsumer(mock(StringRedisTemplate.class),new DemoHandlers(java.util.List.of()),runtime,workflow,new ObjectMapper(),kafka,5);
        Acknowledgment acknowledgment=mock(Acknowledgment.class);
        try {
            consumer.receive("{not-json",acknowledgment);
            var order=inOrder(kafka,acknowledgment);
            order.verify(kafka).send(any(ProducerRecord.class));
            order.verify(acknowledgment).acknowledge();
            var record=org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafka).send(record.capture());
            assertEquals("taskforge.jobs.dlq",record.getValue().topic());
            assertEquals("{not-json",record.getValue().value());
            assertTrue(new String(record.getValue().headers().lastHeader("taskforge-dlq-reason").value(),java.nio.charset.StandardCharsets.UTF_8).startsWith("malformed JSON"));
        } finally {consumer.shutdown();}
    }

    @Test void failedDeadLetterPublishLeavesMalformedSourceRecordUnacknowledged() {
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        KafkaTemplate<String,String> kafka=mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        ExecutionConsumer consumer=new ExecutionConsumer(mock(StringRedisTemplate.class),new DemoHandlers(java.util.List.of()),runtime,mock(ExecutionWorkflowService.class),new ObjectMapper(),kafka,5);
        Acknowledgment acknowledgment=mock(Acknowledgment.class);
        try {
            consumer.receive("{not-json",acknowledgment);
            verify(acknowledgment,never()).acknowledge();
            verify(acknowledgment).nack(Duration.ofSeconds(1));
        } finally {consumer.shutdown();}
    }

    @Test void nullKafkaValueIsDeadLetteredAsAnExplicitNullRecord() throws Exception {
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        KafkaTemplate<String,String> kafka=mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        ExecutionConsumer consumer=new ExecutionConsumer(mock(StringRedisTemplate.class),new DemoHandlers(java.util.List.of()),runtime,mock(ExecutionWorkflowService.class),new ObjectMapper(),kafka,5);
        Acknowledgment acknowledgment=mock(Acknowledgment.class);
        try {
            consumer.receive(null,acknowledgment);
            var record=org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafka).send(record.capture());
            assertEquals("null",record.getValue().value());
            verify(acknowledgment).acknowledge();
        } finally {consumer.shutdown();}
    }

    @Test void timeoutBeyondSupportedLimitIsDeadLetteredBeforeClaiming() throws Exception {
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        KafkaTemplate<String,String> kafka=mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        ExecutionWorkflowService workflow=mock(ExecutionWorkflowService.class);
        ExecutionConsumer consumer=new ExecutionConsumer(mock(StringRedisTemplate.class),new DemoHandlers(java.util.List.of()),runtime,workflow,new ObjectMapper(),kafka,5);
        Acknowledgment acknowledgment=mock(Acknowledgment.class);
        String payload="{\"jobId\":9,\"executionId\":99,\"runNumber\":1,\"type\":\"REPORT\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":86401}";
        try {
            consumer.receive(payload,acknowledgment);
            verify(workflow,never()).claim(any(JobMessage.class),anyString());
            verify(kafka).send(any(ProducerRecord.class));
            verify(acknowledgment).acknowledge();
        } finally {consumer.shutdown();}
    }

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
            verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.CANCELLED),eq(false),contains("cancellation"),eq(false));
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
            verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.TIMEOUT),eq(true),contains("timeout"),eq(false));
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
        Acknowledgment secondAcknowledgment=mock(Acknowledgment.class);
        try {
            consumer.receive("{\"jobId\":9,\"executionId\":99,\"runNumber\":1,\"type\":\"NON_COOPERATIVE\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":1}",acknowledgment);
            var ordered=inOrder(workflow,acknowledgment);
            ordered.verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.TIMEOUT),eq(false),contains("ignored timeout interruption"),eq(true));
            ordered.verify(acknowledgment).acknowledge();
            consumer.receive("{\"jobId\":10,\"executionId\":100,\"runNumber\":1,\"type\":\"NON_COOPERATIVE\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":1}",secondAcknowledgment);
            verify(workflow,never()).claim(argThat(message->message.jobId().equals(10L)),anyString());
            verify(secondAcknowledgment).nack(Duration.ofSeconds(1));
            releaseHandler.countDown();
            verify(runtime,timeout(1000)).handlerExited(9L);
        } finally {releaseHandler.countDown();consumer.shutdown();}
    }

    @Test void handlerFailureTypesControlWhetherTheAttemptCanRetry() throws Exception {
        assertHandlerFailurePolicy(new PermanentJobException("invalid account"),false);
        assertHandlerFailurePolicy(new RetryableJobException("temporary service outage"),true);
        assertHandlerFailurePolicy(new IllegalArgumentException("invalid payload"),false);
    }

    private static void assertHandlerFailurePolicy(RuntimeException failure,boolean retryable) throws Exception {
        ExecutionWorkflowService workflow=mock(ExecutionWorkflowService.class);
        when(workflow.claim(any(JobMessage.class),eq("worker-1"))).thenReturn("lease");
        WorkerRuntime runtime=mock(WorkerRuntime.class);when(runtime.id()).thenReturn("worker-1");
        StringRedisTemplate redis=mock(StringRedisTemplate.class);
        ValueOperations<String,String> values=mock(ValueOperations.class);when(redis.opsForValue()).thenReturn(values);when(values.setIfAbsent(anyString(),anyString(),any(Duration.class))).thenReturn(true);
        JobHandler handler=new JobHandler(){public String type(){return "POLICY_TEST";}public void execute(JobMessage ignored){throw failure;}};
        ExecutionConsumer consumer=new ExecutionConsumer(redis,new DemoHandlers(java.util.List.of(handler)),runtime,workflow,new ObjectMapper());
        Acknowledgment acknowledgment=mock(Acknowledgment.class);
        try {
            consumer.receive("{\"jobId\":9,\"executionId\":99,\"runNumber\":1,\"type\":\"POLICY_TEST\",\"payload\":\"{}\",\"priority\":\"HIGH\",\"attempt\":1,\"timeoutSeconds\":5}",acknowledgment);
            verify(workflow).finish(any(JobMessage.class),eq("worker-1"),eq("lease"),eq(JobStatus.FAILED),eq(retryable),eq(failure.getMessage()));
            verify(acknowledgment).acknowledge();
        } finally {consumer.shutdown();}
    }

    private static JobHandler blockingHandler(String type){return new JobHandler(){public String type(){return type;}public void execute(JobMessage ignored)throws InterruptedException{Thread.sleep(10_000);}};}
}
