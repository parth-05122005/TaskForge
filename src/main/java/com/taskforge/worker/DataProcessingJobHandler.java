package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;

@Component
public class DataProcessingJobHandler implements JobHandler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(DataProcessingJobHandler.class);
    private final ObjectMapper mapper;
    public DataProcessingJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "DATA_PROCESSING";}
    @Override public void execute(JobMessage job){var payload=JobPayloads.object(mapper,job);var items=payload.path("items");int count=items.isArray()?items.size():payload.size();log.atInfo().addKeyValue("jobId",job.jobId()).addKeyValue("executionId",job.executionId()).addKeyValue("itemCount",count).log("Simulated data-processing task completed");}
}
