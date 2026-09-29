package com.taskforge.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.net.InetAddress;
import java.util.UUID;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="worker")
public class WorkerRuntime {
    private final WorkerRepository workers;private final com.taskforge.job.JobExecutionRepository executions;private final org.springframework.data.redis.core.StringRedisTemplate redis;
    private final String workerId;
    public WorkerRuntime(WorkerRepository workers,com.taskforge.job.JobExecutionRepository executions,org.springframework.data.redis.core.StringRedisTemplate redis){
        this.workers=workers;this.executions=executions;this.redis=redis;
        String host=System.getenv().getOrDefault("HOSTNAME",localHostname());
        this.workerId=host+"-"+UUID.randomUUID();
        workers.save(new Worker(workerId,host));
    }
    private static String localHostname(){try{return InetAddress.getLocalHost().getHostName();}catch(Exception e){return "taskforge-worker";}}
    public String id(){return workerId;}
    @Scheduled(fixedDelayString="${taskforge.heartbeat-interval:5000}") @org.springframework.transaction.annotation.Transactional public void heartbeat(){var now=java.time.Instant.now();workers.touch(workerId);executions.extendWorkerLeases(workerId,now.plusSeconds(30));try{redis.opsForValue().set("taskforge:worker:"+workerId+":heartbeat",now.toString(),java.time.Duration.ofSeconds(20));}catch(RuntimeException e){org.slf4j.LoggerFactory.getLogger(WorkerRuntime.class).warn("Redis heartbeat write failed; PostgreSQL heartbeat remains available",e);}}
}
