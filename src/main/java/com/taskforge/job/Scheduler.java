package com.taskforge.job;
import org.springframework.data.domain.PageRequest;import org.springframework.scheduling.annotation.Scheduled;import org.springframework.stereotype.Component;import org.springframework.transaction.annotation.Transactional;import java.time.Instant;import java.util.*;
@Component @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="api",matchIfMissing=true) public class Scheduler {
 private final JobRepository jobs;private final JobExecutionRepository executions;private final org.springframework.kafka.core.KafkaTemplate<String,JobMessage> kafka;
 public Scheduler(JobRepository j,JobExecutionRepository e,org.springframework.kafka.core.KafkaTemplate<String,JobMessage> k){jobs=j;executions=e;kafka=k;}
 @Scheduled(fixedDelayString="${taskforge.scheduler-interval:1000}") @Transactional public void dispatchDue(){for(Job j:jobs.lockDue(Instant.now(),PageRequest.of(0,50))){try{j.transition(JobStatus.QUEUED);int attempt=j.nextAttempt();JobExecution x=executions.save(new JobExecution(j,attempt));kafka.send("taskforge.jobs.execute",String.valueOf(j.getId()),new JobMessage(j.getId(),x.getId(),j.getType(),j.getPayload(),j.getPriority().name(),attempt,j.getTimeoutSeconds()));}catch(RuntimeException ex){throw ex;}}}
}
