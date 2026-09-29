package com.taskforge.worker;

import com.taskforge.job.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;

@Service
public class ExecutionWorkflowService {
    private final JobRepository jobs;private final JobExecutionRepository executions;private final WorkerRepository workers;private final JobEventService events;private final OutboxRepository outbox;private final com.fasterxml.jackson.databind.ObjectMapper mapper;private final com.taskforge.common.TaskForgeMetrics metrics;private final long retryBaseSeconds;private final long retryMaxSeconds;
    public ExecutionWorkflowService(JobRepository jobs,JobExecutionRepository executions,WorkerRepository workers,JobEventService events,OutboxRepository outbox,com.fasterxml.jackson.databind.ObjectMapper mapper,com.taskforge.common.TaskForgeMetrics metrics){this(jobs,executions,workers,events,outbox,mapper,metrics,2,256);}
    @org.springframework.beans.factory.annotation.Autowired
    public ExecutionWorkflowService(JobRepository jobs,JobExecutionRepository executions,WorkerRepository workers,JobEventService events,OutboxRepository outbox,com.fasterxml.jackson.databind.ObjectMapper mapper,com.taskforge.common.TaskForgeMetrics metrics,@org.springframework.beans.factory.annotation.Value("${taskforge.retry.base-delay:2}") long retryBaseSeconds,@org.springframework.beans.factory.annotation.Value("${taskforge.retry.max-delay:256}") long retryMaxSeconds){this.jobs=jobs;this.executions=executions;this.workers=workers;this.events=events;this.outbox=outbox;this.mapper=mapper;this.metrics=metrics;if(retryBaseSeconds<1||retryMaxSeconds<retryBaseSeconds)throw new IllegalArgumentException("Retry delay settings must satisfy 1 <= base <= max");this.retryBaseSeconds=retryBaseSeconds;this.retryMaxSeconds=retryMaxSeconds;}

    @Transactional
    public String claim(JobMessage message,String workerId){
        // Keep the lock order consistent with finish() and lease recovery:
        // execution first, then job. Reversing this order can deadlock when a
        // recovery sweep and a redelivered Kafka message race on the same run.
        JobExecution execution=executions.findByIdForUpdate(message.executionId()).orElse(null);
        if(execution==null||execution.getStatus()!=JobStatus.QUEUED||execution.getRunNumber()!=message.runNumber()||execution.getAttemptNumber()!=message.attempt()||!java.util.Objects.equals(execution.getJob().getId(),message.jobId()))return null;
        Job job=jobs.findByIdForUpdate(message.jobId()).orElse(null);
        if(job==null||job.getStatus()!=JobStatus.QUEUED||job.getRunNumber()!=message.runNumber())return null;
        String token=UUID.randomUUID().toString();
        int executionClaimed=executions.claimQueued(message.executionId(),message.jobId(),message.runNumber(),workerId,token,message.timeoutSeconds()+15);
        if(executionClaimed==0)return null;
        int jobClaimed=jobs.claimQueued(message.jobId());
        if(jobClaimed!=1)throw new IllegalStateException("Execution was claimed but its job was not queued; rolling back claim");
        Worker worker=workers.findById(workerId).orElseThrow(()->new IllegalStateException("Worker registration is missing"));
        worker.assign(message.jobId());
        events.record(message.jobId(),message.executionId(),JobStatus.RUNNING,workerId,"Worker claimed execution");
        return token;
    }

    @Transactional
    public boolean finish(JobMessage message,String workerId,String leaseToken,JobStatus executionResult,boolean retryable,String error){
        JobExecution execution=executions.findByIdForUpdate(message.executionId()).orElseThrow();
        Job job=jobs.findByIdForUpdate(message.jobId()).orElseThrow();
        if(execution.getStatus()!=JobStatus.RUNNING||!leaseToken.equals(execution.getLeaseToken())||execution.getLeaseUntil().isBefore(Instant.now()))return false;
        Worker worker=workers.findById(workerId).orElseThrow();
        boolean success=executionResult==JobStatus.SUCCESS;
        boolean retry=!success&&retryable&&message.attempt()<=job.getMaxRetries()&&!job.isCancellationRequested();
        execution.finish(executionResult,error);
        if(execution.getDurationMs()!=null)metrics.recordDuration(execution.getDurationMs());
        if(job.isCancellationRequested()){
            job.transition(JobStatus.CANCELLED);
        } else if(success){
            if(job.getScheduleType()==ScheduleType.CRON){job.setNextRunAt(job.nextCronRunAfter(Instant.now()));job.transition(JobStatus.SCHEDULED);}
            else job.transition(JobStatus.SUCCESS);
        } else if(retry){
            if(executionResult==JobStatus.TIMEOUT){job.transition(JobStatus.TIMEOUT);job.transition(JobStatus.RETRYING);}
            else job.transition(JobStatus.RETRYING);
            long seconds=com.taskforge.common.RetryBackoff.seconds(message.attempt(),retryBaseSeconds,retryMaxSeconds);
            job.setNextRunAt(Instant.now().plusSeconds(seconds));
        } else if(executionResult==JobStatus.TIMEOUT){
            job.transition(JobStatus.TIMEOUT);
            // A timeout whose handler did not stop (or exhausted its retry
            // budget) is terminal for this job until an operator triggers it.
            // Never automatically overlap a later cron firing with old code.
            job.transition(JobStatus.FAILED);
        } else if(job.getScheduleType()==ScheduleType.CRON){job.setNextRunAt(job.nextCronRunAfter(Instant.now()));job.transition(JobStatus.SCHEDULED);}
        else job.transition(JobStatus.FAILED);
        worker.idle();
        String detail=error==null?(success?"Execution completed":"Execution failed"):error;
        events.record(job.getId(),execution.getId(),executionResult,workerId,detail);
        if(job.getStatus()!=executionResult)events.record(job.getId(),execution.getId(),job.getStatus(),workerId,"Job state changed after attempt");
        if(success)metrics.succeeded();else metrics.failed();if(executionResult==JobStatus.TIMEOUT)metrics.timedOut();
        if(retry)metrics.retried();
        if(!success&&!retry&&!job.isCancellationRequested())try{outbox.save(new OutboxMessage("taskforge.jobs.dlq",String.valueOf(job.getId()),mapper.writeValueAsString(new DeadLetterMessage(job.getId(),execution.getId(),workerId,message.type(),message.attempt(),error,Instant.now()))));}catch(com.fasterxml.jackson.core.JsonProcessingException e){throw new IllegalStateException("Unable to serialize dead-letter event",e);}
        return true;
    }

    @Transactional(readOnly=true)
    public boolean isRunning(Long executionId){return executions.findById(executionId).map(e->e.getStatus()==JobStatus.RUNNING).orElse(false);}

    @Transactional(readOnly=true)
    public boolean isCancellationRequested(Long jobId){return jobs.findById(jobId).map(Job::isCancellationRequested).orElse(false);}
}
