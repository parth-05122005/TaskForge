package com.taskforge.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="api",matchIfMissing=true)
public class Scheduler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(Scheduler.class);
    private final JobRepository jobs;
    private final JobExecutionRepository executions;
    private final OutboxRepository outbox;
    private final JobEventService events;
    private final ObjectMapper mapper;

    public Scheduler(JobRepository jobs,JobExecutionRepository executions,OutboxRepository outbox,JobEventService events,ObjectMapper mapper){
        this.jobs=jobs;this.executions=executions;this.outbox=outbox;this.events=events;this.mapper=mapper;
    }

    @Scheduled(fixedDelayString="${taskforge.scheduler-interval:1000}")
    @Transactional
    public void dispatchDue(){
        for(Job job:jobs.lockDue(Instant.now(),50)){
            if(job.getStatus()==JobStatus.SCHEDULED)job.beginScheduledRun();
            job.transition(JobStatus.QUEUED);
            int attempt=job.nextAttempt();
            JobExecution execution=executions.save(new JobExecution(job,job.getRunNumber(),attempt));
            JobMessage message=new JobMessage(job.getId(),execution.getId(),job.getRunNumber(),job.getType(),job.getPayload(),job.getPriority().name(),attempt,job.getTimeoutSeconds());
            try {
                outbox.save(new OutboxMessage("taskforge.jobs.execute",String.valueOf(job.getId()),mapper.writeValueAsString(message)));
            } catch(JsonProcessingException e) {
                throw new IllegalStateException("Unable to serialize executable job",e);
            }
            events.record(job.getId(),execution.getId(),JobStatus.QUEUED,null,"Queued for execution");
            log.atInfo().addKeyValue("jobId",job.getId()).addKeyValue("executionId",execution.getId()).addKeyValue("attempt",attempt).log("Job durably queued to outbox");
        }
    }
}
