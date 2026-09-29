package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;

@Component
public class ReportGenerationJobHandler implements JobHandler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ReportGenerationJobHandler.class);
    private final ObjectMapper mapper;
    public ReportGenerationJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "REPORT_GENERATION";}
    @Override public void execute(JobMessage job){var payload=JobPayloads.object(mapper,job);String report=payload.path("reportType").asText("SUMMARY");log.atInfo().addKeyValue("jobId",job.jobId()).addKeyValue("executionId",job.executionId()).addKeyValue("reportType",report).log("Simulated report-generation task completed");}
}
