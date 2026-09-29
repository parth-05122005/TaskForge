package com.taskforge.job;
import java.time.Instant;
public record JobLifecycleEvent(Long eventId,Long jobId,Long executionId,JobStatus status,String workerId,String detail,Instant occurredAt){}
