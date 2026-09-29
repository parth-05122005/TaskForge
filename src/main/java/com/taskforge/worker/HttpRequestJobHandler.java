package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.springframework.stereotype.Component;
import java.net.URI;

@Component
public class HttpRequestJobHandler implements JobHandler {
    private static final org.slf4j.Logger log=org.slf4j.LoggerFactory.getLogger(HttpRequestJobHandler.class);
    private final ObjectMapper mapper;
    public HttpRequestJobHandler(ObjectMapper mapper){this.mapper=mapper;}
    @Override public String type(){return "HTTP_REQUEST";}
    @Override public void execute(JobMessage job){var payload=JobPayloads.object(mapper,job);String url=payload.path("url").asText("");if(!url.isBlank())try{String scheme=URI.create(url).getScheme();if(!"http".equalsIgnoreCase(scheme)&&!"https".equalsIgnoreCase(scheme))throw new IllegalArgumentException("HTTP job URL must use http or https");}catch(IllegalArgumentException e){throw new IllegalArgumentException("HTTP job URL is invalid",e);}log.atInfo().addKeyValue("jobId",job.jobId()).addKeyValue("executionId",job.executionId()).log("Simulated HTTP-request task completed; no external request was sent");}
}
