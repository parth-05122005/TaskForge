package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;

@Component
public class LongRunningDemoJobHandler implements JobHandler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(LongRunningDemoJobHandler.class);
    private final ObjectMapper mapper;
    public LongRunningDemoJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "DEMO_LONG_RUNNING_TASK";}
    @Override public void execute(JobMessage job)throws InterruptedException {
        var payload=JobPayloads.object(mapper,job);long duration=Math.max(0,Math.min(60_000,payload.path("durationMs").asLong(1_000)));long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(duration);
        while(System.nanoTime()<deadline){long left=java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline-System.nanoTime());Thread.sleep(Math.max(1,Math.min(250,left)));}
        log.atInfo().addKeyValue("jobId",job.jobId()).addKeyValue("executionId",job.executionId()).addKeyValue("durationMs",duration).log("Long-running demo task completed");
    }
}
