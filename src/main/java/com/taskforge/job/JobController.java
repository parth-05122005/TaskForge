package com.taskforge.job;
import jakarta.validation.Valid;import org.springframework.data.domain.*;import org.springframework.security.core.Authentication;import org.springframework.web.bind.annotation.*;import java.util.*;
@RestController @RequestMapping("/api/jobs") @io.swagger.v3.oas.annotations.security.SecurityRequirement(name="bearerAuth") public class JobController {
 private final JobService service; public JobController(JobService s){service=s;}
 @PostMapping JobView create(@Valid @RequestBody JobService.CreateJob body,Authentication a){return JobView.of(service.create(a.getName(),body));}
 @GetMapping Page<JobView> list(Authentication a,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size,@RequestParam(defaultValue="updatedAt,desc") String sort,@RequestParam(required=false) JobStatus status){
  if(size<1||size>100||page<0)throw new IllegalArgumentException("page must be nonnegative and size must be between 1 and 100");
  String[] bits=sort==null?new String[0]:sort.split(",",-1);
  if(bits.length<1||bits.length>2||bits[0].isBlank())throw new IllegalArgumentException("sort must be a field and optional asc or desc direction");
  String field=bits[0].trim();
  if(!java.util.Set.of("createdAt","updatedAt","priority","nextRunAt","status","name").contains(field))throw new IllegalArgumentException("Unsupported sort field: "+field);
  String direction=bits.length==1?"desc":bits[1].trim().toLowerCase(java.util.Locale.ROOT);
  if(!direction.equals("asc")&&!direction.equals("desc"))throw new IllegalArgumentException("Unsupported sort direction: "+direction);
  Sort s=Sort.by(direction.equals("asc")?Sort.Direction.ASC:Sort.Direction.DESC,field);
  return service.list(a.getName(),isAdmin(a),status,PageRequest.of(page,size,s)).map(JobView::of);
 }
 @GetMapping("/{id}") JobView get(@PathVariable Long id,Authentication a){return JobView.of(service.get(id,a.getName(),isAdmin(a)));}
 @PostMapping("/{id}/cancel") JobView cancel(@PathVariable Long id,Authentication a){return JobView.of(service.cancel(id,a.getName(),isAdmin(a)));}
 @PutMapping("/{id}") JobView update(@PathVariable Long id,@Valid @RequestBody JobService.CreateJob body,Authentication a){return JobView.of(service.update(id,a.getName(),isAdmin(a),body));}
 @DeleteMapping("/{id}") @org.springframework.web.bind.annotation.ResponseStatus(org.springframework.http.HttpStatus.NO_CONTENT) void delete(@PathVariable Long id,Authentication a){service.delete(id,a.getName(),isAdmin(a));}
 @PostMapping("/{id}/trigger") JobView trigger(@PathVariable Long id,Authentication a){return JobView.of(service.trigger(id,a.getName(),isAdmin(a)));}
 @GetMapping("/{id}/executions") Page<ExecutionView> history(@PathVariable Long id,Authentication a,@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size){
  if(page<0||size<1||size>100)throw new IllegalArgumentException("page must be nonnegative and size must be between 1 and 100");
  Pageable pageable=PageRequest.of(page,size,Sort.by(Sort.Order.desc("runNumber"),Sort.Order.desc("attemptNumber")));
  return service.history(id,a.getName(),isAdmin(a),pageable).map(ExecutionView::of);
 }
 private boolean isAdmin(Authentication a){return a.getAuthorities().stream().anyMatch(x->x.getAuthority().equals("ROLE_ADMIN"));}
}
