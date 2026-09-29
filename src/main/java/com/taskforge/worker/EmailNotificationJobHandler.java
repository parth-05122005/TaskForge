package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;

@Component
public class EmailNotificationJobHandler implements JobHandler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(EmailNotificationJobHandler.class);
    private final ObjectMapper mapper;
    public EmailNotificationJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "EMAIL_NOTIFICATION";}
    @Override public void execute(JobMessage job){var payload=JobPayloads.object(mapper,job);String recipient=payload.path("to").asText(payload.path("recipient").asText("demo-recipient"));if(recipient.isBlank())throw new IllegalArgumentException("Email recipient cannot be blank");log.atInfo().addKeyValue("jobId",job.jobId()).addKeyValue("executionId",job.executionId()).addKeyValue("recipientConfigured",true).log("Simulated email notification accepted");}
}
