package com.taskforge.job;
public record JobMessage(Long jobId,Long executionId,int runNumber,String type,String payload,String priority,int attempt,int timeoutSeconds){
 public static final int MAX_JOB_TYPE_LENGTH=80;
 public static final int MAX_RETRIES=20;
 public static final int MAX_ATTEMPTS=MAX_RETRIES+1;
 public static final int MAX_TIMEOUT_SECONDS=86_400;
}
