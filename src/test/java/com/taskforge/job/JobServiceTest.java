package com.taskforge.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.auth.User;
import com.taskforge.auth.UserRepository;
import com.taskforge.common.TaskForgeMetrics;
import com.taskforge.common.AdminAuditService;
import com.taskforge.worker.WorkerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobServiceTest {
    private JobRepository jobs;
    private JobExecutionRepository executions;
    private WorkerRepository workers;
    private UserRepository users;
    private JobEventService events;
    private JobService service;
    private Job ownedJob;

    @BeforeEach void setUp(){
        jobs=mock(JobRepository.class);
        users=mock(UserRepository.class);
        executions=mock(JobExecutionRepository.class);
        workers=mock(WorkerRepository.class);
        events=mock(JobEventService.class);
        TaskForgeMetrics metrics=mock(TaskForgeMetrics.class);
        AdminAuditService adminAudit=mock(AdminAuditService.class);
        service=new JobService(jobs,users,new ObjectMapper(),executions,events,metrics,adminAudit,workers);
        ownedJob=mock(Job.class);
        when(ownedJob.getOwner()).thenReturn(new User("owner@example.com","hash"));
        when(jobs.findById(7L)).thenReturn(Optional.of(ownedJob));
    }

    @Test void ownerCanReadTheirJob(){assertSame(ownedJob,service.get(7L,"owner@example.com",false));}

    @Test void anotherUserCannotReadJobByGuessingItsId(){assertThrows(java.util.NoSuchElementException.class,()->service.get(7L,"other@example.com",false));}

    @Test void adminCanReadAnyJob(){assertSame(ownedJob,service.get(7L,"admin@example.com",true));}

    @Test void cannotTriggerJobWhileAWorkerStillRunsItsPreviousHandler(){
        when(jobs.findByIdForUpdate(7L)).thenReturn(Optional.of(ownedJob));
        when(workers.existsByCurrentJobIdAndStatus(7L,com.taskforge.worker.WorkerStatus.BUSY)).thenReturn(true);

        assertThrows(IllegalStateException.class,()->service.trigger(7L,"owner@example.com",false));

        verify(ownedJob,never()).triggerNow();
        verify(events,never()).record(any(),any(),any(),any(),any());
    }

    @Test void rejectsPayloadLargerThanKafkaSafeByteLimitBeforePersistence(){
        var oversized=new JobService.CreateJob("large",null,"REPORT",java.util.Map.of("data","x".repeat(JobMessage.MAX_PAYLOAD_BYTES)),ScheduleType.IMMEDIATE,null,null,"UTC",JobPriority.MEDIUM,1,30);

        var failure=assertThrows(IllegalArgumentException.class,()->service.create("owner@example.com",oversized));

        assertTrue(failure.getMessage().contains(Integer.toString(JobMessage.MAX_PAYLOAD_BYTES)));
        verify(users,never()).findByEmail(anyString());
        verify(jobs,never()).save(any(Job.class));

        Job scheduled=new Job(new User("owner@example.com","hash"),"original",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,java.time.Instant.now(),JobPriority.MEDIUM,1,30);
        when(jobs.findByIdForUpdate(7L)).thenReturn(Optional.of(scheduled));
        assertThrows(IllegalArgumentException.class,()->service.update(7L,"owner@example.com",false,oversized));
        assertEquals("original",scheduled.getName());
        verify(events,never()).record(any(),any(),any(),any(),any());
    }

    @Test void deleteLocksExecutionsBeforeTheJobToMatchWorkerClaimLockOrder(){
        when(ownedJob.getStatus()).thenReturn(JobStatus.CANCELLED);
        when(jobs.findByIdForUpdate(7L)).thenReturn(Optional.of(ownedJob));

        service.delete(7L,"owner@example.com",false);

        var order=inOrder(jobs,executions,events);
        order.verify(jobs).findById(7L);
        order.verify(executions).lockAllByJobId(7L);
        order.verify(jobs).findByIdForUpdate(7L);
        order.verify(events).deleteByJobId(7L);
        order.verify(executions).deleteByJobId(7L);
        order.verify(jobs).delete(ownedJob);
    }
}
