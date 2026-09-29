package com.taskforge.common;

import com.taskforge.auth.AdminService;import com.taskforge.job.*;import com.taskforge.worker.*;
import org.springframework.data.domain.*;import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController @RequestMapping("/api/admin") @io.swagger.v3.oas.annotations.security.SecurityRequirement(name="bearerAuth")
public class AdminController {
    private final JobRepository jobs;private final WorkerRepository workers;private final AdminService admin;private final AdminAuditLogRepository audit;
    public AdminController(JobRepository jobs,WorkerRepository workers,AdminService admin,AdminAuditLogRepository audit){this.jobs=jobs;this.workers=workers;this.admin=admin;this.audit=audit;}
    @GetMapping("/statistics") public Map<String,Object> statistics(){Map<String,Long> jobCounts=new LinkedHashMap<>();for(JobStatus status:JobStatus.values())jobCounts.put(status.name(),0L);jobs.countGroupedByStatus().forEach(row->jobCounts.put(row.getStatus().name(),row.getTotal()));Map<String,Long> workerCounts=new LinkedHashMap<>();for(WorkerStatus status:WorkerStatus.values())workerCounts.put(status.name(),0L);workers.countGroupedByStatus().forEach(row->workerCounts.put(row.getStatus().name(),row.getTotal()));return Map.of("jobs",jobCounts,"workers",workerCounts);}
    @GetMapping("/jobs") public Page<JobView> jobs(@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size,@RequestParam(required=false) JobStatus status){if(page<0||size<1||size>100)throw new IllegalArgumentException("Invalid page/size");Pageable p=PageRequest.of(page,size,Sort.by(Sort.Direction.DESC,"updatedAt"));return (status==null?jobs.findAll(p):jobs.findByStatus(status,p)).map(JobView::of);}
    @GetMapping("/workers") public List<WorkerView> workers(){return workers.findAllByOrderByLastHeartbeatDesc().stream().map(WorkerView::of).toList();}
    @GetMapping("/audit") public Page<AdminAuditView> audit(@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="50") int size,@RequestParam(required=false) String resourceType,@RequestParam(required=false) String resourceId){if(page<0||size<1||size>200)throw new IllegalArgumentException("Invalid page/size");Pageable p=PageRequest.of(page,size,Sort.by(Sort.Direction.DESC,"createdAt"));Page<AdminAuditLog> rows=resourceType!=null&&resourceId!=null?audit.findByResourceTypeAndResourceId(resourceType,resourceId,p):audit.findAll(p);return rows.map(AdminAuditView::of);}
    @PostMapping("/users/{id}/disable") @ResponseStatus(org.springframework.http.HttpStatus.NO_CONTENT) public void disable(@PathVariable Long id,org.springframework.security.core.Authentication authentication){admin.disable(id,authentication.getName());}
}
