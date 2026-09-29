package com.taskforge.job;
import jakarta.validation.Valid;import org.springframework.data.domain.*;import org.springframework.security.core.Authentication;import org.springframework.web.bind.annotation.*;import java.util.*;
@RestController @RequestMapping("/api/jobs") public class JobController {
 private final JobService service; public JobController(JobService s){service=s;}
 @PostMapping JobView create(@Valid @RequestBody JobService.CreateJob body,Authentication a){return JobView.of(service.create(a.getName(),body));}
 @GetMapping Page<JobView> list(Authentication a,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size,@RequestParam(defaultValue="createdAt,desc") String sort){if(size<1||size>100)throw new IllegalArgumentException("size must be between 1 and 100");String[] bits=sort.split(",");Sort s=Sort.by(bits.length>1&&bits[1].equalsIgnoreCase("asc")?Sort.Direction.ASC:Sort.Direction.DESC,bits[0]);return service.list(a.getName(),a.getAuthorities().stream().anyMatch(x->x.getAuthority().equals("ROLE_ADMIN")),PageRequest.of(page,size,s)).map(JobView::of);}
 @GetMapping("/{id}") JobView get(@PathVariable Long id,Authentication a){return JobView.of(service.get(id,a.getName(),isAdmin(a)));}
 @PostMapping("/{id}/cancel") JobView cancel(@PathVariable Long id,Authentication a){return JobView.of(service.cancel(id,a.getName(),isAdmin(a)));}
 @GetMapping("/{id}/executions") List<Map<String,Object>> history(@PathVariable Long id,Authentication a){return service.history(id,a.getName(),isAdmin(a)).stream().map(e->Map.<String,Object>of("executionId",e.getId(),"attemptNumber",e.getAttemptNumber(),"status",e.getStatus(),"workerId",e.getWorkerId()==null?"":e.getWorkerId())).toList();}
 private boolean isAdmin(Authentication a){return a.getAuthorities().stream().anyMatch(x->x.getAuthority().equals("ROLE_ADMIN"));}
}
