package com.taskforge.job;

import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class DashboardControllerTest {
    @Test void snapshotRejectsInvalidWorkerPaging(){
        DashboardService dashboard=mock(DashboardService.class);
        DashboardController controller=new DashboardController(dashboard);
        var admin=UsernamePasswordAuthenticationToken.authenticated("admin@example.com",null,List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));

        assertThrows(IllegalArgumentException.class,()->controller.snapshot(admin,null,null,-1,25));
        assertThrows(IllegalArgumentException.class,()->controller.snapshot(admin,null,null,0,101));
        assertThrows(IllegalArgumentException.class,()->controller.snapshot(admin,null,null,0,25,-1,25));
        assertThrows(IllegalArgumentException.class,()->controller.snapshot(admin,null,null,0,25,0,101));

        verifyNoInteractions(dashboard);
    }
}
