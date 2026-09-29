package com.taskforge.job;
import java.time.Instant;
public record EventView(Long id,Long jobId,Long executionId,JobStatus status,String workerId,String detail,Instant createdAt){public static EventView of(JobEvent e){return new EventView(e.getId(),e.getJobId(),e.getExecutionId(),e.getStatus(),e.getWorkerId(),e.getDetail(),e.getCreatedAt());}}
