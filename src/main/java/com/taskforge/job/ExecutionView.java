package com.taskforge.job;
import java.time.Instant;
public record ExecutionView(Long executionId,int runNumber,int attemptNumber,JobStatus status,String workerId,Instant startedAt,Instant completedAt,Long durationMs,String errorMessage){public static ExecutionView of(JobExecution e){return new ExecutionView(e.getId(),e.getRunNumber(),e.getAttemptNumber(),e.getStatus(),e.getWorkerId(),e.getStartedAt(),e.getCompletedAt(),e.getDurationMs(),e.getErrorMessage());}}
