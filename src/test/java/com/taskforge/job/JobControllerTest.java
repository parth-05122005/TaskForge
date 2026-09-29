package com.taskforge.job;

import com.taskforge.auth.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobControllerTest {
    private JobService service;
    private JobController controller;

    @BeforeEach void setUp() {service=mock(JobService.class);controller=new JobController(service);}

    @Test void createUsesAuthenticatedOwnerAndReturnsDto() {
        var user=UsernamePasswordAuthenticationToken.authenticated("owner@example.com",null,List.of(new SimpleGrantedAuthority("ROLE_USER")));
        var request=new JobService.CreateJob("report",null,"REPORT",java.util.Map.of(),ScheduleType.IMMEDIATE,null,null,"UTC",JobPriority.HIGH,1,30);
        Job job=new Job(new User("owner@example.com","hash"),"report",null,"REPORT","{}",ScheduleType.IMMEDIATE,null,Instant.now(),JobPriority.HIGH,1,30);
        when(service.create("owner@example.com",request)).thenReturn(job);

        JobView result=controller.create(request,user);

        assertEquals("report",result.name());
        assertEquals(JobStatus.SCHEDULED,result.status());
        verify(service).create("owner@example.com",request);
    }

    @Test void listRejectsOutOfRangePageBeforeCallingService() {
        var user=UsernamePasswordAuthenticationToken.authenticated("owner@example.com",null,List.of(new SimpleGrantedAuthority("ROLE_USER")));
        assertThrows(IllegalArgumentException.class,()->controller.list(user,-1,20,"updatedAt,desc",null));
        verifyNoInteractions(service);
    }

    @Test void listRejectsUnapprovedSortProperty() {
        var user=UsernamePasswordAuthenticationToken.authenticated("owner@example.com",null,List.of(new SimpleGrantedAuthority("ROLE_USER")));
        assertThrows(IllegalArgumentException.class,()->controller.list(user,0,20,"owner.passwordHash,asc",null));
        verifyNoInteractions(service);
    }

    @Test void listRejectsUnsupportedSortDirectionAndExtraFields() {
        var user=UsernamePasswordAuthenticationToken.authenticated("owner@example.com",null,List.of(new SimpleGrantedAuthority("ROLE_USER")));

        assertThrows(IllegalArgumentException.class,()->controller.list(user,0,20,"updatedAt,sideways",null));
        assertThrows(IllegalArgumentException.class,()->controller.list(user,0,20,"updatedAt,desc,name",null));
        verifyNoInteractions(service);
    }

    @Test void executionHistoryRejectsInvalidPageBounds() {
        var user=UsernamePasswordAuthenticationToken.authenticated("owner@example.com",null,List.of(new SimpleGrantedAuthority("ROLE_USER")));

        assertThrows(IllegalArgumentException.class,()->controller.history(7L,user,-1,20));
        assertThrows(IllegalArgumentException.class,()->controller.history(7L,user,0,101));
        verifyNoInteractions(service);
    }
}
