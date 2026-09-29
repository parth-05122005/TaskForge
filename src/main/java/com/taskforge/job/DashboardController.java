package com.taskforge.job;

import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;

@RestController @RequestMapping("/api/dashboard") @io.swagger.v3.oas.annotations.security.SecurityRequirement(name="bearerAuth")
public class DashboardController {
    private final DashboardService dashboard;
    public DashboardController(DashboardService dashboard){this.dashboard=dashboard;}
    @GetMapping("/snapshot") public DashboardSnapshot snapshot(Authentication auth,@RequestParam(required=false) Instant since){boolean admin=auth.getAuthorities().stream().anyMatch(a->a.getAuthority().equals("ROLE_ADMIN"));return dashboard.snapshot(auth.getName(),admin,since);}
}
