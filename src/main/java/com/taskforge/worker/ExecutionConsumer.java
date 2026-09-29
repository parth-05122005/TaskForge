package com.taskforge.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.*;
import jakarta.annotation.PreDestroy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="worker")
public class ExecutionConsumer {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ExecutionConsumer.class);
    private final StringRedisTemplate redis;private final DemoHandlers handlers;private final WorkerRuntime runtime;private final ExecutionWorkflowService workflow;private final ObjectMapper mapper;private final KafkaTemplate<String,String> kafka;private final long handlerStopGraceSeconds;
    private final Semaphore executionSlot=new Semaphore(1);
    private final ThreadPoolExecutor executionPool=new ThreadPoolExecutor(1,1,0L,TimeUnit.MILLISECONDS,new SynchronousQueue<>(),task->{Thread thread=new Thread(task,"taskforge-handler");thread.setDaemon(true);return thread;},new ThreadPoolExecutor.AbortPolicy());
    public ExecutionConsumer(StringRedisTemplate redis,DemoHandlers handlers,WorkerRuntime runtime,ExecutionWorkflowService workflow,ObjectMapper mapper){this(redis,handlers,runtime,workflow,mapper,null,5);}
    public ExecutionConsumer(StringRedisTemplate redis,DemoHandlers handlers,WorkerRuntime runtime,ExecutionWorkflowService workflow,ObjectMapper mapper,long handlerStopGraceSeconds){this(redis,handlers,runtime,workflow,mapper,null,handlerStopGraceSeconds);}
    @org.springframework.beans.factory.annotation.Autowired
    public ExecutionConsumer(StringRedisTemplate redis,DemoHandlers handlers,WorkerRuntime runtime,ExecutionWorkflowService workflow,ObjectMapper mapper,KafkaTemplate<String,String> kafka,@org.springframework.beans.factory.annotation.Value("${taskforge.handler-stop-grace-seconds:5}") long handlerStopGraceSeconds){if(handlerStopGraceSeconds<1||handlerStopGraceSeconds>60)throw new IllegalArgumentException("Handler stop grace must be between 1 and 60 seconds");this.redis=redis;this.handlers=handlers;this.runtime=runtime;this.workflow=workflow;this.mapper=mapper;this.kafka=kafka;this.handlerStopGraceSeconds=handlerStopGraceSeconds;}

    @KafkaListener(topics="taskforge.jobs.execute",groupId="taskforge-workers")
    public void receive(String payload,Acknowledgment acknowledgment){
        if(payload==null){deadLetterMalformed(null,acknowledgment,"null Kafka record value");return;}
        JobMessage message;
        try {message=mapper.readValue(payload,JobMessage.class);}
        catch(JsonProcessingException malformed){deadLetterMalformed(payload,acknowledgment,"malformed JSON: "+malformed.getOriginalMessage());return;}
        if(!hasValidExecutionFields(message)){deadLetterMalformed(payload,acknowledgment,"missing or out-of-range required execution fields");return;}
        if(!executionSlot.tryAcquire()){acknowledgment.nack(Duration.ofSeconds(1));return;}
        String key="taskforge:lock:job:"+message.jobId(),token=UUID.randomUUID().toString();
        boolean redisAvailable=true;
        boolean locked=false;
        try {locked=Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key,token,Duration.ofSeconds(Math.max(30,message.timeoutSeconds()+20L))));}
        catch(RuntimeException unavailable) {redisAvailable=false;log.warn("Redis lock unavailable; continuing with PostgreSQL claim as the ownership authority",unavailable);}
        if(redisAvailable&&!locked){executionSlot.release();acknowledgment.nack(Duration.ofSeconds(1));return;}
        boolean handlerOwnsExecutionSlot=false;
        try {
            String leaseToken=workflow.claim(message,runtime.id());
            if(leaseToken==null){if(workflow.isRunning(message.executionId()))acknowledgment.nack(Duration.ofSeconds(1));else acknowledgment.acknowledge();return;}
            log.atInfo().addKeyValue("jobId",message.jobId()).addKeyValue("executionId",message.executionId()).addKeyValue("workerId",runtime.id()).addKeyValue("attempt",message.attempt()).log("Worker claimed execution");
            CountDownLatch handlerEnded=new CountDownLatch(1);java.util.concurrent.atomic.AtomicBoolean handlerStarted=new java.util.concurrent.atomic.AtomicBoolean();java.util.concurrent.atomic.AtomicBoolean preventHandlerStart=new java.util.concurrent.atomic.AtomicBoolean();Object handlerStartGate=new Object();
            java.util.concurrent.atomic.AtomicBoolean slotReleased=new java.util.concurrent.atomic.AtomicBoolean();
            Future<?> running=executionPool.submit((Callable<Void>)()->{
                synchronized(handlerStartGate){if(preventHandlerStart.get()){handlerEnded.countDown();releaseExecutionSlot(slotReleased);return null;}handlerStarted.set(true);}
                try{handlers.get(message.type()).execute(message);return null;}finally{handlerEnded.countDown();releaseExecutionSlot(slotReleased);}
            });
            handlerOwnsExecutionSlot=true;
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(message.timeoutSeconds());
            try {
                boolean finished=false;
                while(!finished) {
                    long remaining=deadline-System.nanoTime();
                    long waitMillis=Math.max(1,Math.min(250,TimeUnit.NANOSECONDS.toMillis(Math.max(1,remaining))));
                    try {
                        running.get(waitMillis,TimeUnit.MILLISECONDS);
                        workflow.finish(message,runtime.id(),leaseToken,JobStatus.SUCCESS,false,null);
                        finished=true;
                    } catch(TimeoutException pending) {
                        if(workflow.isCancellationRequested(message.jobId())) {
                            boolean stopped=interruptAndAwait(running,handlerStarted,preventHandlerStart,handlerStartGate,handlerEnded,slotReleased);
                            String detail=stopped?"Execution stopped after cancellation request":"Handler ignored cancellation interruption; execution was marked cancelled and will not be retried automatically";
                            if(!stopped)log.atError().addKeyValue("jobId",message.jobId()).addKeyValue("executionId",message.executionId()).addKeyValue("workerId",runtime.id()).log("Handler did not stop during cancellation grace period");
                            workflow.finish(message,runtime.id(),leaseToken,JobStatus.CANCELLED,false,detail);
                            finished=true;
                        } else if(System.nanoTime()>=deadline) {
                            boolean stopped=interruptAndAwait(running,handlerStarted,preventHandlerStart,handlerStartGate,handlerEnded,slotReleased);
                            String detail=stopped?"Execution exceeded timeout of "+message.timeoutSeconds()+" seconds":"Handler ignored timeout interruption; execution was marked terminal and will not be retried automatically. Isolate this handler in a process to guarantee termination.";
                            if(!stopped)log.atError().addKeyValue("jobId",message.jobId()).addKeyValue("executionId",message.executionId()).addKeyValue("workerId",runtime.id()).log("Handler did not stop during timeout grace period; disabling automatic retry for this run");
                            workflow.finish(message,runtime.id(),leaseToken,JobStatus.TIMEOUT,stopped,detail);
                            finished=true;
                        }
                    } catch(ExecutionException e) {
                        Throwable cause=e.getCause();boolean permanent=cause instanceof IllegalArgumentException;
                        workflow.finish(message,runtime.id(),leaseToken,JobStatus.FAILED,!permanent,cause.getMessage());
                        finished=true;
                    }
                }
            } catch(InterruptedException e) {
                running.cancel(true);Thread.currentThread().interrupt();
                acknowledgment.nack(Duration.ofSeconds(1));return;
            }
            acknowledgment.acknowledge();
        } catch(RuntimeException e) {
            acknowledgment.nack(Duration.ofSeconds(1));
        } finally {
            if(locked)try {
                String script="if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
                redis.execute(new org.springframework.data.redis.core.script.DefaultRedisScript<>(script,Long.class),List.of(key),token);
            } catch(RuntimeException unavailable) {log.warn("Redis lock release failed; its TTL will expire",unavailable);}
            if(!handlerOwnsExecutionSlot)executionSlot.release();
        }
    }

    private void deadLetterMalformed(String payload,Acknowledgment acknowledgment,String reason){
        if(kafka==null){log.error("Cannot dead-letter malformed execution message because KafkaTemplate is unavailable; leaving source record unacknowledged");acknowledgment.nack(Duration.ofSeconds(1));return;}
        try{
            String key=poisonKey(payload);
            if(payload!=null)try{var tree=mapper.readTree(payload);if(tree!=null&&tree.hasNonNull("jobId")){String jobId=tree.get("jobId").asText();key=jobId.substring(0,Math.min(200,jobId.length()));}}catch(JsonProcessingException ignored){/* Keep the stable opaque key for invalid JSON. */}
            ProducerRecord<String,String> record=new ProducerRecord<>("taskforge.jobs.dlq",key,payload==null?"null":payload);
            record.headers().add("taskforge-dlq-reason",reason.substring(0,Math.min(500,reason.length())).getBytes(StandardCharsets.UTF_8));
            record.headers().add("taskforge-dlq-worker-id",runtime.id().getBytes(StandardCharsets.UTF_8));
            record.headers().add("taskforge-dlq-failed-at",java.time.Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            kafka.send(record).get(10,TimeUnit.SECONDS);
            log.atError().addKeyValue("workerId",runtime.id()).addKeyValue("reason",reason).log("Malformed execution message durably sent to dead-letter topic");
            acknowledgment.acknowledge();
        }catch(Exception failure){
            log.atError().setCause(failure).addKeyValue("workerId",runtime.id()).log("Failed to dead-letter malformed execution message; source record will be retried");
            acknowledgment.nack(Duration.ofSeconds(1));
        }
    }

    private static String poisonKey(String payload){
        try{return "poison-"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest((payload==null?"<null>":payload).getBytes(StandardCharsets.UTF_8)));}
        catch(NoSuchAlgorithmException impossible){throw new IllegalStateException("SHA-256 is required by the Java runtime",impossible);}
    }

    private static boolean hasValidExecutionFields(JobMessage message){
        if(message.jobId()==null||message.jobId()<1||message.executionId()==null||message.executionId()<1||message.runNumber()<1||message.type()==null||message.type().isBlank()||message.type().length()>JobMessage.MAX_JOB_TYPE_LENGTH||message.payload()==null||message.attempt()<1||message.attempt()>JobMessage.MAX_ATTEMPTS||message.timeoutSeconds()<1||message.timeoutSeconds()>JobMessage.MAX_TIMEOUT_SECONDS||message.priority()==null)return false;
        try{JobPriority.valueOf(message.priority());return true;}catch(IllegalArgumentException invalidPriority){return false;}
    }

    private boolean interruptAndAwait(Future<?> running,java.util.concurrent.atomic.AtomicBoolean handlerStarted,java.util.concurrent.atomic.AtomicBoolean preventHandlerStart,Object handlerStartGate,CountDownLatch handlerEnded,java.util.concurrent.atomic.AtomicBoolean slotReleased)throws InterruptedException{
        synchronized(handlerStartGate){
            if(!handlerStarted.get()){preventHandlerStart.set(true);running.cancel(true);releaseExecutionSlot(slotReleased);return true;}
            running.cancel(true);
        }
        return handlerEnded.await(handlerStopGraceSeconds,TimeUnit.SECONDS);
    }

    private void releaseExecutionSlot(java.util.concurrent.atomic.AtomicBoolean released){if(released.compareAndSet(false,true))executionSlot.release();}

    @PreDestroy public void shutdown(){executionPool.shutdownNow();}
}
