package com.taskforge.worker;

import com.taskforge.job.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="api",matchIfMissing=true)
public class LeaseRecovery {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(LeaseRecovery.class);
    private final JobExecutionRepository executions;private final JobRepository jobs;private final WorkerRepository workers;private final JobEventService events;private final org.springframework.data.redis.core.StringRedisTemplate redis;private final OutboxRepository outbox;private final ObjectMapper mapper;private final com.taskforge.common.TaskForgeMetrics metrics;
    public LeaseRecovery(JobExecutionRepository executions,JobRepository jobs,WorkerRepository workers,JobEventService events,org.springframework.data.redis.core.StringRedisTemplate redis,OutboxRepository outbox,ObjectMapper mapper,com.taskforge.common.TaskForgeMetrics metrics){this.executions=executions;this.jobs=jobs;this.workers=workers;this.events=events;this.redis=redis;this.outbox=outbox;this.mapper=mapper;this.metrics=metrics;}

    @Scheduled(fixedDelayString="${taskforge.recovery-interval:5000}")
    @Transactional
    public void recoverExpiredLeases(){
        Instant cutoff=Instant.now().minusSeconds(10);
        for(JobExecution execution:executions.lockExpiredLeases(cutoff,100)){
            Job job=jobs.findByIdForUpdate(execution.getJob().getId()).orElse(null);
            if(job==null||job.getStatus()!=JobStatus.RUNNING)continue;
            String workerId=execution.getWorkerId();
            execution.finish(JobStatus.FAILED,"Worker lease expired; execution is being recovered");
            boolean retry=!job.isCancellationRequested()&&job.getAttemptCount()<=job.getMaxRetries();
            if(job.isCancellationRequested()) {
                job.transition(JobStatus.CANCELLED);
            } else if(retry) {
                job.transition(JobStatus.RETRYING);job.setNextRunAt(Instant.now().plusSeconds(2));metrics.retried();
            } else if(job.getScheduleType()==ScheduleType.CRON) {
                job.setNextRunAt(job.nextCronRunAfter(Instant.now()));job.transition(JobStatus.SCHEDULED);
            } else {
                job.transition(JobStatus.FAILED);
            }
            workers.findById(workerId).ifPresent(w->{if(java.util.Objects.equals(w.getCurrentJobId(),job.getId()))w.recovered();});
            JobStatus outcome=job.isCancellationRequested()?JobStatus.CANCELLED:(retry?JobStatus.RETRYING:(job.getScheduleType()==ScheduleType.CRON?JobStatus.SCHEDULED:JobStatus.FAILED));
            events.record(job.getId(),execution.getId(),outcome,workerId,"Worker lease expired; recovery applied");
            log.atWarn().addKeyValue("jobId",job.getId()).addKeyValue("executionId",execution.getId()).addKeyValue("workerId",workerId).addKeyValue("attempt",execution.getAttemptNumber()).addKeyValue("outcome",outcome).log("Expired worker lease recovered");
            if(!retry&&!job.isCancellationRequested()) {
                metrics.failed();
                try {outbox.save(new OutboxMessage("taskforge.jobs.dlq",String.valueOf(job.getId()),mapper.writeValueAsString(new DeadLetterMessage(job.getId(),execution.getId(),workerId,job.getType(),execution.getAttemptNumber(),execution.getErrorMessage(),Instant.now()))));}
                catch(JsonProcessingException e){throw new IllegalStateException("Unable to serialize recovered dead-letter event",e);}
            }
        }
        Instant deadBefore=Instant.now().minusSeconds(20);
        markDeadIfHeartbeatExpired(WorkerStatus.BUSY,deadBefore);
        markDeadIfHeartbeatExpired(WorkerStatus.ONLINE,deadBefore);
    }
    private void markDeadIfHeartbeatExpired(WorkerStatus status,Instant before){for(Worker worker:workers.findByStatusAndLastHeartbeatBefore(status,before)){try{if(Boolean.TRUE.equals(redis.hasKey("taskforge:worker:"+worker.getId()+":heartbeat")))continue;}catch(RuntimeException unavailable){/* PostgreSQL timestamp is the fallback signal. */}worker.markDead();}}
}
