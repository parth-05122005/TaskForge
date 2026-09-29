package com.taskforge.common;

import com.taskforge.auth.User;
import com.taskforge.auth.UserRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class AdminAuditServiceTest {
    @AfterEach void clearRequestContext() { RequestContextHolder.resetRequestAttributes(); }

    @Test void recordsActorAndTargetResource() {
        UserRepository users = mock(UserRepository.class);
        AdminAuditLogRepository audit = mock(AdminAuditLogRepository.class);
        User actor = mock(User.class);
        when(actor.getId()).thenReturn(7L);
        when(users.findByEmail("admin@example.com")).thenReturn(Optional.of(actor));

        new AdminAuditService(users, audit).record("ADMIN@example.com", "JOB", "42", "JOB_TRIGGERED", "Manual execution requested");

        ArgumentCaptor<AdminAuditLog> entry = ArgumentCaptor.forClass(AdminAuditLog.class);
        verify(audit).save(entry.capture());
        assertEquals(7L, entry.getValue().getActorUserId());
        assertEquals("JOB", entry.getValue().getResourceType());
        assertEquals("42", entry.getValue().getResourceId());
        assertEquals("JOB_TRIGGERED", entry.getValue().getAction());
    }
}
