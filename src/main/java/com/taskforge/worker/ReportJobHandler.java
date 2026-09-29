package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;

@Component
public class ReportJobHandler implements JobHandler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(ReportJobHandler.class);
    private final ObjectMapper mapper;
    public ReportJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "REPORT";}
    @Override public void execute(JobMessage job){String report=JobPayloads.object(mapper,job).path("reportType").asText("GENERAL");log.atInfo().addKeyValue("jobId",job.jobId()).addKeyValue("executionId",job.executionId()).addKeyValue("reportType",report).log("Simulated report generated");}
}
