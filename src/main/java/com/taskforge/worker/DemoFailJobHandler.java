package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;

@Component
public class DemoFailJobHandler implements JobHandler {
    private final ObjectMapper mapper;
    public DemoFailJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "DEMO_FAIL";}
    @Override public void execute(JobMessage job){
        var payload=JobPayloads.object(mapper,job);
        if(payload.has("failAttempts")){
            int failAttempts=payload.path("failAttempts").asInt(-1);
            if(failAttempts<0||failAttempts>JobMessage.MAX_RETRIES)
                throw new IllegalArgumentException("failAttempts must be between 0 and "+JobMessage.MAX_RETRIES);
            if(job.attempt()<=failAttempts)
                throw new RetryableJobException("Requested demo failure for attempt "+job.attempt());
            return;
        }
        if(payload.path("fail").asBoolean(true))throw new IllegalStateException("Requested demo transient failure");
    }
}
