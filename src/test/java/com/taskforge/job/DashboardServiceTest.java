package com.taskforge.job;

import com.taskforge.auth.User;
import com.taskforge.auth.UserRepository;
import com.taskforge.worker.WorkerRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DashboardServiceTest {
    @Test void ownerEventFeedUsesCursorWithoutLoadingJobsOrWorkers(){
        JobRepository jobs=mock(JobRepository.class);JobEventRepository events=mock(JobEventRepository.class);WorkerRepository workers=mock(WorkerRepository.class);UserRepository users=mock(UserRepository.class);
        User owner=mock(User.class);when(owner.getId()).thenReturn(42L);when(users.findByEmail("owner@example.com")).thenReturn(Optional.of(owner));
        JobEvent event=mock(JobEvent.class);when(event.getId()).thenReturn(92L);
        when(events.findAfterCursorOrRecentForOwner(eq(42L),eq(91L),any(),any())).thenReturn(List.of(event));

        List<EventView> result=new DashboardService(jobs,events,workers,users).events("owner@example.com",false,null,91L);

        assertEquals(1,result.size());assertEquals(92L,result.get(0).id());
        var cursorQuery=org.mockito.ArgumentCaptor.forClass(java.time.Instant.class);
        verify(events).findAfterCursorOrRecentForOwner(eq(42L),eq(91L),cursorQuery.capture(),any());
        assertTrue(java.time.Duration.between(cursorQuery.getValue(),java.time.Instant.now()).toSeconds()>=119);
        assertTrue(java.time.Duration.between(cursorQuery.getValue(),java.time.Instant.now()).toSeconds()<=121);
        verify(events,never()).findTop200ByIdGreaterThanOrderByIdAsc(anyLong());
        verifyNoInteractions(jobs,workers);
    }

    @Test void ownerIncrementalFeedUsesEventIdCursorAndOwnerScope(){
        JobRepository jobs=mock(JobRepository.class);JobEventRepository events=mock(JobEventRepository.class);WorkerRepository workers=mock(WorkerRepository.class);UserRepository users=mock(UserRepository.class);
        User owner=mock(User.class);when(owner.getId()).thenReturn(42L);when(users.findByEmail("owner@example.com")).thenReturn(Optional.of(owner));
        when(jobs.findByOwnerId(eq(42L),any())).thenReturn(Page.empty());when(events.findAfterCursorOrRecentForOwner(eq(42L),eq(91L),any(),any())).thenReturn(List.of());

        DashboardSnapshot snapshot=new DashboardService(jobs,events,workers,users).snapshot("owner@example.com",false,null,91L);

        assertFalse(snapshot.admin());assertTrue(snapshot.workers().isEmpty());verify(events).findAfterCursorOrRecentForOwner(eq(42L),eq(91L),any(),any());verify(events,never()).findTop200ByIdGreaterThanOrderByIdAsc(anyLong());verifyNoInteractions(workers);
        verify(jobs).countGroupedByOwnerStatus(42L);verify(jobs,never()).countByOwnerAndStatus(anyLong(),any());
        assertEquals(0L,snapshot.counts().get(JobStatus.SUCCESS.name()));
    }

    @Test void initialSnapshotUsesLookbackWindowForEventBootstrap(){
        JobRepository jobs=mock(JobRepository.class);JobEventRepository events=mock(JobEventRepository.class);WorkerRepository workers=mock(WorkerRepository.class);UserRepository users=mock(UserRepository.class);
        User owner=mock(User.class);when(owner.getId()).thenReturn(42L);when(users.findByEmail("admin@example.com")).thenReturn(Optional.of(owner));
        when(jobs.findAll(any(org.springframework.data.domain.Pageable.class))).thenReturn(new PageImpl<>(List.of()));
        when(events.findTop200ByCreatedAtGreaterThanEqualOrderByIdAsc(any())).thenReturn(List.of());
        when(workers.findAllByOrderByLastHeartbeatDesc(any(org.springframework.data.domain.Pageable.class))).thenReturn(Page.empty());

        DashboardSnapshot snapshot=new DashboardService(jobs,events,workers,users).snapshot("admin@example.com",true,null,null);

        assertTrue(snapshot.admin());assertEquals(0,snapshot.totalWorkers());verify(jobs).countGroupedByStatus();verify(events).findTop200ByCreatedAtGreaterThanEqualOrderByIdAsc(any());verify(workers).findAllByOrderByLastHeartbeatDesc(org.springframework.data.domain.PageRequest.of(0,25));
    }
}
