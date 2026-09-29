package com.taskforge.job;

import com.taskforge.auth.User;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class JobLifecycleTest {
    private Job job() {
        return new Job(new User("test@example.com", "hash"), "test", null, "REPORT", "{}",
                ScheduleType.IMMEDIATE, null, Instant.now(), JobPriority.MEDIUM, 2, 30);
    }

    @Test void allowsQueueRunSuccessPath() {
        Job job = job();
        job.transition(JobStatus.QUEUED);
        job.transition(JobStatus.RUNNING);
        job.transition(JobStatus.SUCCESS);
        assertEquals(JobStatus.SUCCESS, job.getStatus());
    }

    @Test void rejectsTerminalStateReopen() {
        Job job = job();
        job.transition(JobStatus.QUEUED);
        job.transition(JobStatus.RUNNING);
        job.transition(JobStatus.FAILED);
        assertThrows(IllegalStateException.class, () -> job.transition(JobStatus.QUEUED));
    }

    @Test void allowsRetryFromRunningAndRequeue() {
        Job job = job();
        job.transition(JobStatus.QUEUED);
        job.transition(JobStatus.RUNNING);
        job.transition(JobStatus.RETRYING);
        job.transition(JobStatus.QUEUED);
        assertEquals(JobStatus.QUEUED, job.getStatus());
    }

    @Test void manuallyTriggersCompletedJobAsANewScheduledRun() {
        Job job = job();
        job.transition(JobStatus.QUEUED);
        job.transition(JobStatus.RUNNING);
        job.transition(JobStatus.SUCCESS);

        job.triggerNow();

        assertEquals(JobStatus.SCHEDULED, job.getStatus());
        assertNotNull(job.getNextRunAt());
        assertEquals(0, job.getAttemptCount());
    }

    @Test void manualTriggerBringsAFutureScheduledRunForward() {
        Job job=new Job(new User("future@example.com","hash"),"future",null,"REPORT","{}",ScheduleType.ONE_TIME,null,Instant.now().plusSeconds(3600),JobPriority.MEDIUM,1,30);

        job.triggerNow();

        assertEquals(JobStatus.SCHEDULED,job.getStatus());
        assertTrue(job.getNextRunAt().isBefore(Instant.now().plusSeconds(5)));
    }

    @Test void runningCancellationIsRecordedAsARequest() {
        Job job = job();
        job.transition(JobStatus.QUEUED);
        job.transition(JobStatus.RUNNING);

        job.requestCancellation();

        assertEquals(JobStatus.RUNNING, job.getStatus());
        assertTrue(job.isCancellationRequested());
    }

    @Test void editingCreatedJobWithAScheduleMakesItRunnable() {
        Job job=new Job(new User("created@example.com","hash"),"draft",null,"REPORT","{}",ScheduleType.ONE_TIME,null,null,JobPriority.MEDIUM,1,30);
        assertEquals(JobStatus.CREATED,job.getStatus());

        job.updateDetails("scheduled",null,"REPORT","{}",ScheduleType.ONE_TIME,null,"UTC",Instant.now().plusSeconds(60),JobPriority.HIGH,2,45);

        assertEquals(JobStatus.SCHEDULED,job.getStatus());
        assertNotNull(job.getNextRunAt());
    }

    @Test void eachCronOccurrenceStartsAFreshRetryBudget() {
        Instant now=Instant.now();
        Job job=new Job(new User("cron@example.com","hash"),"recurring",null,"REPORT","{}",ScheduleType.CRON,"0 * * * * *","UTC",CronSupport.next("0 * * * * *","UTC",now),JobPriority.MEDIUM,2,30);
        job.beginScheduledRun();
        assertEquals(1,job.nextAttempt());
        job.transition(JobStatus.QUEUED);job.transition(JobStatus.RUNNING);job.transition(JobStatus.SUCCESS);
        job.setNextRunAt(job.nextCronRunAfter(Instant.now()));job.transition(JobStatus.SCHEDULED);

        job.beginScheduledRun();

        assertEquals(2,job.getRunNumber());
        assertEquals(0,job.getAttemptCount());
        assertEquals(1,job.nextAttempt());
    }
}
