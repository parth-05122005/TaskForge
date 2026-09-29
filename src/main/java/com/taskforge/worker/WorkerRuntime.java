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
    private final int leaseExtraSeconds;
    private final WorkerLivenessSettings liveness;
    private final String workerId;
    public WorkerRuntime(WorkerRepository workers,com.taskforge.job.JobExecutionRepository executions,org.springframework.data.redis.core.StringRedisTemplate redis){
        this(workers,executions,redis,5,new WorkerLivenessSettings(5_000,20));
    }
    public WorkerRuntime(WorkerRepository workers,com.taskforge.job.JobExecutionRepository executions,org.springframework.data.redis.core.StringRedisTemplate redis,@Value("${taskforge.handler-stop-grace-seconds:5}") long handlerStopGraceSeconds){
        this(workers,executions,redis,handlerStopGraceSeconds,new WorkerLivenessSettings(5_000,20));
    }
    @org.springframework.beans.factory.annotation.Autowired
    public WorkerRuntime(WorkerRepository workers,com.taskforge.job.JobExecutionRepository executions,org.springframework.data.redis.core.StringRedisTemplate redis,@Value("${taskforge.handler-stop-grace-seconds:5}") long handlerStopGraceSeconds,@Value("${taskforge.heartbeat-interval:5000}") long heartbeatIntervalMillis,@Value("${taskforge.worker-dead-after-seconds:20}") long workerDeadAfterSeconds){
        this(workers,executions,redis,handlerStopGraceSeconds,new WorkerLivenessSettings(heartbeatIntervalMillis,workerDeadAfterSeconds));
    }
    private WorkerRuntime(WorkerRepository workers,com.taskforge.job.JobExecutionRepository executions,org.springframework.data.redis.core.StringRedisTemplate redis,long handlerStopGraceSeconds,WorkerLivenessSettings liveness){
        if(handlerStopGraceSeconds<1||handlerStopGraceSeconds>60)throw new IllegalArgumentException("Handler stop grace must be between 1 and 60 seconds");
        this.workers=workers;this.executions=executions;this.redis=redis;this.liveness=liveness;
        this.leaseExtraSeconds=Math.toIntExact(handlerStopGraceSeconds+20);
        String host=System.getenv().getOrDefault("HOSTNAME",localHostname());
        this.workerId=host+"-"+UUID.randomUUID();
        workers.save(new Worker(workerId,host));
    }
    private static String localHostname(){try{return InetAddress.getLocalHost().getHostName();}catch(Exception e){return "taskforge-worker";}}
    public String id(){return workerId;}
    @org.springframework.transaction.annotation.Transactional
    public void handlerExited(Long jobId){
        workers.findById(workerId)
                .filter(worker->worker.getStatus()==WorkerStatus.BUSY&&java.util.Objects.equals(worker.getCurrentJobId(),jobId))
                .ifPresent(Worker::idle);
    }
    @Scheduled(fixedDelayString="${taskforge.heartbeat-interval:5000}") @org.springframework.transaction.annotation.Transactional public void heartbeat(){var now=java.time.Instant.now();workers.touch(workerId);executions.extendWorkerLeases(workerId,now.plusSeconds(30),leaseExtraSeconds);try{redis.opsForValue().set("taskforge:worker:"+workerId+":heartbeat",now.toString(),liveness.deadAfter());}catch(RuntimeException e){org.slf4j.LoggerFactory.getLogger(WorkerRuntime.class).warn("Redis heartbeat write failed; PostgreSQL heartbeat remains available",e);}}
    @jakarta.annotation.PreDestroy public void shutdown(){
        try{workers.markOffline(workerId);}catch(RuntimeException failure){org.slf4j.LoggerFactory.getLogger(WorkerRuntime.class).warn("Could not mark worker offline during shutdown",failure);}
        try{redis.delete("taskforge:worker:"+workerId+":heartbeat");}catch(RuntimeException failure){org.slf4j.LoggerFactory.getLogger(WorkerRuntime.class).warn("Could not remove worker Redis heartbeat during shutdown",failure);}
    }
}
