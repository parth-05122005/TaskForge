package com.taskforge.job;

import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;

@RestController @RequestMapping("/api/dashboard") @io.swagger.v3.oas.annotations.security.SecurityRequirement(name="bearerAuth")
public class DashboardController {
    private final DashboardService dashboard;
    public DashboardController(DashboardService dashboard){this.dashboard=dashboard;}
    public DashboardSnapshot snapshot(Authentication auth,Instant since,Long afterId,int workerPage,int workerSize){return snapshot(auth,since,afterId,workerPage,workerSize,0,25,null);}
    public DashboardSnapshot snapshot(Authentication auth,Instant since,Long afterId,int workerPage,int workerSize,int jobPage,int jobSize){return snapshot(auth,since,afterId,workerPage,workerSize,jobPage,jobSize,null);}
    @GetMapping("/snapshot") public DashboardSnapshot snapshot(Authentication auth,@RequestParam(required=false) Instant since,@RequestParam(required=false) Long afterId,@RequestParam(defaultValue="0") int workerPage,@RequestParam(defaultValue="25") int workerSize,@RequestParam(defaultValue="0") int jobPage,@RequestParam(defaultValue="25") int jobSize,@RequestParam(required=false) JobStatus status){if(afterId!=null&&afterId<0)throw new IllegalArgumentException("afterId must be nonnegative");if(workerPage<0||workerSize<1||workerSize>100)throw new IllegalArgumentException("workerPage must be nonnegative and workerSize must be between 1 and 100");if(jobPage<0||jobSize<1||jobSize>100)throw new IllegalArgumentException("jobPage must be nonnegative and jobSize must be between 1 and 100");boolean admin=auth.getAuthorities().stream().anyMatch(a->a.getAuthority().equals("ROLE_ADMIN"));return dashboard.snapshot(auth.getName(),admin,since,afterId,workerPage,workerSize,jobPage,jobSize,status);}
    @GetMapping("/events") public java.util.List<EventView> events(Authentication auth,@RequestParam(required=false) Instant since,@RequestParam(required=false) Long afterId){if(afterId!=null&&afterId<0)throw new IllegalArgumentException("afterId must be nonnegative");boolean admin=auth.getAuthorities().stream().anyMatch(a->a.getAuthority().equals("ROLE_ADMIN"));return dashboard.events(auth.getName(),admin,since,afterId);}
}
