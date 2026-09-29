package com.taskforge.common;

import com.taskforge.auth.AdminService;
import com.taskforge.auth.User;
import com.taskforge.auth.UserRepository;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AdminServiceTest {
    @Test void disablingUserWritesAuditEntryInSameServiceOperation() {
        UserRepository users=mock(UserRepository.class);
        AdminAuditService audit=mock(AdminAuditService.class);
        User actor=mock(User.class),target=mock(User.class);
        when(actor.getId()).thenReturn(1L);
        when(target.getId()).thenReturn(2L);
        when(target.isEnabled()).thenReturn(true);
        when(target.getRole()).thenReturn("USER");
        when(users.findByEmail("admin@example.com")).thenReturn(Optional.of(actor));
        when(users.findById(2L)).thenReturn(Optional.of(target));
        when(users.disableIfEnabled(2L)).thenReturn(1);

        new AdminService(users,audit).disable(2L,"ADMIN@example.com");

        verify(users).disableIfEnabled(2L);
        verify(audit).record("ADMIN@example.com","USER","2","USER_DISABLED","Account disabled by administrator");
    }

    @Test void lastEnabledAdminCannotBeDisabled() {
        UserRepository users=mock(UserRepository.class);
        AdminAuditService audit=mock(AdminAuditService.class);
        User admin=mock(User.class);
        when(admin.getRole()).thenReturn("ADMIN");
        when(admin.isEnabled()).thenReturn(true);
        when(users.findByEmail("admin@example.com")).thenReturn(Optional.of(admin));
        when(users.findById(1L)).thenReturn(Optional.of(admin));
        when(users.lockEnabledAdmins()).thenReturn(java.util.List.of(admin));
        when(users.existsByIdAndEnabledTrue(1L)).thenReturn(true);

        assertThrows(IllegalStateException.class,()->new AdminService(users,audit).disable(1L,"admin@example.com"));
        verify(users,never()).disableIfEnabled(1L);
        verifyNoInteractions(audit);
    }
}
