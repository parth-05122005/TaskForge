package com.taskforge.job;

import com.fasterxml.jackson.core.JsonProcessingException;import com.fasterxml.jackson.databind.ObjectMapper;import org.springframework.stereotype.Service;

@Service public class JobEventService {
    private final JobEventRepository events;private final OutboxRepository outbox;private final ObjectMapper mapper;
    public JobEventService(JobEventRepository events,OutboxRepository outbox,ObjectMapper mapper){this.events=events;this.outbox=outbox;this.mapper=mapper;}
    public JobEvent record(Long jobId,Long executionId,JobStatus status,String workerId,String detail){
        JobEvent event=events.save(new JobEvent(jobId,executionId,status,workerId,detail));
        try{var payload=new JobLifecycleEvent(event.getId(),jobId,executionId,status,workerId,detail,event.getCreatedAt());outbox.save(new OutboxMessage("taskforge.jobs.events",String.valueOf(jobId),mapper.writeValueAsString(payload)));}
        catch(JsonProcessingException e){throw new IllegalStateException("Unable to serialize job lifecycle event",e);}
        return event;
    }
    public void deleteByJobId(Long jobId){events.deleteByJobId(jobId);}
}
