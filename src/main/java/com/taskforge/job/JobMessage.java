package com.taskforge.job;
public record JobMessage(Long jobId,Long executionId,String type,String payload,String priority,int attempt,int timeoutSeconds){}
