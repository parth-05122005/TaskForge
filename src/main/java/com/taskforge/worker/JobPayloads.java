package com.taskforge.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;

final class JobPayloads {
    private JobPayloads() {}

    static JsonNode object(ObjectMapper mapper,JobMessage message) {
        try {
            JsonNode payload=mapper.readTree(message.payload());
            if(payload==null||!payload.isObject())throw new IllegalArgumentException("Job payload must be a JSON object");
            return payload;
        } catch(JsonProcessingException e) {
            throw new IllegalArgumentException("Job payload must be valid JSON",e);
        }
    }
}
