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

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="worker")
public class ExecutionConsumer {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ExecutionConsumer.class);
    private final StringRedisTemplate redis;private final DemoHandlers handlers;private final WorkerRuntime runtime;private final ExecutionWorkflowService workflow;private final ObjectMapper mapper;
    private final ExecutorService executionPool=Executors.newCachedThreadPool();
    public ExecutionConsumer(StringRedisTemplate redis,DemoHandlers handlers,WorkerRuntime runtime,ExecutionWorkflowService workflow,ObjectMapper mapper){this.redis=redis;this.handlers=handlers;this.runtime=runtime;this.workflow=workflow;this.mapper=mapper;}

    @KafkaListener(topics="taskforge.jobs.execute",groupId="taskforge-workers")
    public void receive(String payload,Acknowledgment acknowledgment){
        JobMessage message;
        try {message=mapper.readValue(payload,JobMessage.class);}
        catch(JsonProcessingException malformed){log.atError().setCause(malformed).log("Discarding malformed execution message");acknowledgment.acknowledge();return;}
        if(message.jobId()==null||message.executionId()==null||message.runNumber()<0||message.type()==null||message.type().isBlank()||message.payload()==null||message.attempt()<1||message.timeoutSeconds()<1){log.atError().addKeyValue("jobId",message.jobId()).addKeyValue("executionId",message.executionId()).log("Discarding incomplete execution message");acknowledgment.acknowledge();return;}
        String key="taskforge:lock:job:"+message.jobId(),token=UUID.randomUUID().toString();
        boolean redisAvailable=true;
        boolean locked=false;
        try {locked=Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key,token,Duration.ofSeconds(Math.max(30,message.timeoutSeconds()+20L))));}
        catch(RuntimeException unavailable) {redisAvailable=false;log.warn("Redis lock unavailable; continuing with PostgreSQL claim as the ownership authority",unavailable);}
        if(redisAvailable&&!locked){acknowledgment.nack(Duration.ofSeconds(1));return;}
        try {
            String leaseToken=workflow.claim(message,runtime.id());
            if(leaseToken==null){if(workflow.isRunning(message.executionId()))acknowledgment.nack(Duration.ofSeconds(1));else acknowledgment.acknowledge();return;}
            log.atInfo().addKeyValue("jobId",message.jobId()).addKeyValue("executionId",message.executionId()).addKeyValue("workerId",runtime.id()).addKeyValue("attempt",message.attempt()).log("Worker claimed execution");
            CountDownLatch handlerEnded=new CountDownLatch(1);
            Future<?> running=executionPool.submit((Callable<Void>)()->{try{handlers.get(message.type()).execute(message);return null;}finally{handlerEnded.countDown();}});
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
                            running.cancel(true);
                            handlerEnded.await();
                            workflow.finish(message,runtime.id(),leaseToken,JobStatus.CANCELLED,false,"Execution stopped after cancellation request");
                            finished=true;
                        } else if(System.nanoTime()>=deadline) {
                            running.cancel(true);
                            handlerEnded.await();
                            workflow.finish(message,runtime.id(),leaseToken,JobStatus.TIMEOUT,true,"Execution exceeded timeout of "+message.timeoutSeconds()+" seconds");
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
        }
    }

    @PreDestroy public void shutdown(){executionPool.shutdownNow();}
}
