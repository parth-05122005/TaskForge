package com.taskforge.worker;

import java.time.Duration;

/** Keeps Redis heartbeat expiry and PostgreSQL dead-worker detection on one timing policy. */
public final class WorkerLivenessSettings {
    private final long heartbeatIntervalMillis;
    private final long deadAfterSeconds;

    public WorkerLivenessSettings(long heartbeatIntervalMillis,long configuredDeadAfterSeconds) {
        if(heartbeatIntervalMillis<100||heartbeatIntervalMillis>300_000)throw new IllegalArgumentException("Worker heartbeat interval must be between 100 and 300000 milliseconds");
        if(configuredDeadAfterSeconds<1||configuredDeadAfterSeconds>3_600)throw new IllegalArgumentException("Worker dead threshold must be between 1 and 3600 seconds");
        this.heartbeatIntervalMillis=heartbeatIntervalMillis;
        long threeIntervals=(heartbeatIntervalMillis*3+999)/1_000;
        this.deadAfterSeconds=Math.max(configuredDeadAfterSeconds,threeIntervals);
    }

    public long heartbeatIntervalMillis(){return heartbeatIntervalMillis;}
    public long deadAfterSeconds(){return deadAfterSeconds;}
    public Duration deadAfter(){return Duration.ofSeconds(deadAfterSeconds);}
}
