package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.auth.User;
import com.taskforge.common.TaskForgeMetrics;
import com.taskforge.job.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ExecutionWorkflowServiceTest {
    JobRepository jobs;JobExecutionRepository executions;WorkerRepository workers;JobEventService events;OutboxRepository outbox;TaskForgeMetrics metrics;ExecutionWorkflowService service;
    @BeforeEach void setup(){jobs=mock(JobRepository.class);executions=mock(JobExecutionRepository.class);workers=mock(WorkerRepository.class);events=mock(JobEventService.class);outbox=mock(OutboxRepository.class);metrics=mock(TaskForgeMetrics.class);service=new ExecutionWorkflowService(jobs,executions,workers,events,outbox,new ObjectMapper().findAndRegisterModules(),metrics);}
    private Job job(JobStatus status,int maxRetries){Job j=new Job(new User("workerflow@example.com","hash"),"flow",null,"REPORT","{}",ScheduleType.ONE_TIME,null,Instant.now(),JobPriority.MEDIUM,maxRetries,30);if(status==JobStatus.QUEUED||status==JobStatus.RUNNING){j.beginScheduledRun();j.transition(JobStatus.QUEUED);if(status==JobStatus.RUNNING)j.transition(JobStatus.RUNNING);}return j;}
    private JobExecution execution(Job job,String token){JobExecution e=mock(JobExecution.class);when(e.getStatus()).thenReturn(JobStatus.RUNNING);when(e.getLeaseToken()).thenReturn(token);when(e.getLeaseUntil()).thenReturn(Instant.now().plusSeconds(60));when(e.getDurationMs()).thenReturn(12L);when(e.getId()).thenReturn(22L);return e;}
    private JobMessage message(int attempt){return new JobMessage(11L,22L,1,"REPORT","{}","HIGH",attempt,30);}

    @Test void claimAtomicallyClaimsJobAndExecutionOnce(){
        Job queued=job(JobStatus.QUEUED,2),executionJob=mock(Job.class);when(executionJob.getId()).thenReturn(11L);
        JobExecution queuedExecution=mock(JobExecution.class);when(queuedExecution.getStatus()).thenReturn(JobStatus.QUEUED);when(queuedExecution.getRunNumber()).thenReturn(1);when(queuedExecution.getJob()).thenReturn(executionJob);
        when(executions.findByIdForUpdate(22L)).thenReturn(Optional.of(queuedExecution));when(jobs.findByIdForUpdate(11L)).thenReturn(Optional.of(queued));when(executions.claimQueued(eq(22L),eq(11L),eq(1),eq("worker-1"),anyString(),anyInt())).thenReturn(1);when(jobs.claimQueued(11L)).thenReturn(1);when(workers.findById("worker-1")).thenReturn(Optional.of(new Worker("worker-1","host")));
        String token=service.claim(message(1),"worker-1");assertNotNull(token);verify(jobs).claimQueued(11L);
        when(jobs.findByIdForUpdate(11L)).thenReturn(Optional.of(queued));assertNull(service.claim(message(1),"worker-2"));
        verify(executions,times(1)).claimQueued(eq(22L),eq(11L),eq(1),eq("worker-1"),anyString(),anyInt());
    }

    @Test void transientFailureSchedulesRetryWithPersistedEvent(){
        Job running=job(JobStatus.RUNNING,3);JobExecution execution=execution(running,"lease");Worker worker=new Worker("worker-1","host");
        when(jobs.findById(11L)).thenReturn(Optional.of(running));when(jobs.findByIdForUpdate(11L)).thenReturn(Optional.of(running));when(executions.findByIdForUpdate(22L)).thenReturn(Optional.of(execution));when(workers.findById("worker-1")).thenReturn(Optional.of(worker));
        assertTrue(service.finish(message(1),"worker-1","lease",JobStatus.FAILED,true,"temporary"));
        assertEquals(JobStatus.RETRYING,running.getStatus());assertEquals(WorkerStatus.ONLINE,worker.getStatus());verify(events,times(2)).record(any(),eq(22L),any(),eq("worker-1"),anyString());verify(metrics).retried();
    }

    @Test void exhaustedFailureWritesDeadLetterOutboxRecord(){
        Job running=job(JobStatus.RUNNING,0);JobExecution execution=execution(running,"lease");
        when(jobs.findById(11L)).thenReturn(Optional.of(running));when(jobs.findByIdForUpdate(11L)).thenReturn(Optional.of(running));when(executions.findByIdForUpdate(22L)).thenReturn(Optional.of(execution));when(workers.findById("worker-1")).thenReturn(Optional.of(new Worker("worker-1","host")));
        service.finish(message(1),"worker-1","lease",JobStatus.FAILED,false,"permanent");
        assertEquals(JobStatus.FAILED,running.getStatus());verify(outbox).save(argThat(o->o.getTopic().equals("taskforge.jobs.dlq")));
    }

    @Test void nonCooperativeCronTimeoutFailsClosedInsteadOfStartingAnOverlappingOccurrence(){
        Job cron=new Job(new User("cron-timeout@example.com","hash"),"cron timeout",null,"REPORT","{}",ScheduleType.CRON,"0 */5 * * * *","UTC",Instant.now(),JobPriority.MEDIUM,3,30);
        cron.beginScheduledRun();cron.transition(JobStatus.QUEUED);cron.transition(JobStatus.RUNNING);
        JobExecution execution=execution(cron,"lease");
        when(jobs.findById(11L)).thenReturn(Optional.of(cron));when(jobs.findByIdForUpdate(11L)).thenReturn(Optional.of(cron));when(executions.findByIdForUpdate(22L)).thenReturn(Optional.of(execution));when(workers.findById("worker-1")).thenReturn(Optional.of(new Worker("worker-1","host")));

        assertTrue(service.finish(message(1),"worker-1","lease",JobStatus.TIMEOUT,false,"handler ignored interruption"));

        assertEquals(JobStatus.FAILED,cron.getStatus());
        verify(outbox).save(argThat(o->o.getTopic().equals("taskforge.jobs.dlq")));
    }

    @Test void staleLeaseCannotOverwriteRecoveredExecution(){
        Job running=job(JobStatus.RUNNING,2);JobExecution execution=execution(running,"new-token");
        when(jobs.findById(11L)).thenReturn(Optional.of(running));when(jobs.findByIdForUpdate(11L)).thenReturn(Optional.of(running));when(executions.findByIdForUpdate(22L)).thenReturn(Optional.of(execution));
        assertFalse(service.finish(message(1),"worker-1","old-token",JobStatus.SUCCESS,false,null));verify(jobs,never()).save(any());verify(events,never()).record(any(),any(),any(),any(),any());
    }
}
