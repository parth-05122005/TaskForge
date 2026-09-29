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
}
