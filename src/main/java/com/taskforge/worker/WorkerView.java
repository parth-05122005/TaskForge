package com.taskforge.worker;
import java.time.Instant;
public record WorkerView(String workerId,String hostname,WorkerStatus status,Instant lastHeartbeat,Long currentJobId,Instant registeredAt){public static WorkerView of(Worker w){return new WorkerView(w.getId(),w.getHostname(),w.getStatus(),w.getLastHeartbeat(),w.getCurrentJobId(),w.getRegisteredAt());}}
