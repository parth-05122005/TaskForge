package com.taskforge.job;
import java.time.Instant;
public record DeadLetterMessage(Long jobId,Long executionId,String workerId,String type,int attempt,String error,Instant failedAt){}
