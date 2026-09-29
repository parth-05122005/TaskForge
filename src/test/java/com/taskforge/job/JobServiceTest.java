package com.taskforge.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.auth.User;
import com.taskforge.auth.UserRepository;
import com.taskforge.common.TaskForgeMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobServiceTest {
    private JobRepository jobs;
    private JobService service;
    private Job ownedJob;

    @BeforeEach void setUp(){
        jobs=mock(JobRepository.class);
        UserRepository users=mock(UserRepository.class);
        JobExecutionRepository executions=mock(JobExecutionRepository.class);
        JobEventService events=mock(JobEventService.class);
        TaskForgeMetrics metrics=mock(TaskForgeMetrics.class);
        service=new JobService(jobs,users,new ObjectMapper(),executions,events,metrics);
        ownedJob=mock(Job.class);
        when(ownedJob.getOwner()).thenReturn(new User("owner@example.com","hash"));
        when(jobs.findById(7L)).thenReturn(Optional.of(ownedJob));
    }

    @Test void ownerCanReadTheirJob(){assertSame(ownedJob,service.get(7L,"owner@example.com",false));}

    @Test void anotherUserCannotReadJobByGuessingItsId(){assertThrows(java.util.NoSuchElementException.class,()->service.get(7L,"other@example.com",false));}

    @Test void adminCanReadAnyJob(){assertSame(ownedJob,service.get(7L,"admin@example.com",true));}
}
