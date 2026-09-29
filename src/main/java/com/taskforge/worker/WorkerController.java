package com.taskforge.worker;

import org.springframework.web.bind.annotation.*;
import java.util.*;
import org.springframework.data.domain.*;

@RestController @RequestMapping("/api/workers") @io.swagger.v3.oas.annotations.security.SecurityRequirement(name="bearerAuth")
public class WorkerController {
    private final WorkerRepository workers;
    public WorkerController(WorkerRepository workers){this.workers=workers;}
    @GetMapping public Page<WorkerView> all(@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size,@RequestParam(required=false) WorkerStatus status){
        if(page<0||size<1||size>100)throw new IllegalArgumentException("page must be nonnegative and size must be between 1 and 100");
        Pageable pageable=PageRequest.of(page,size);
        return (status==null?workers.findAllByOrderByLastHeartbeatDesc(pageable):workers.findByStatusOrderByLastHeartbeatDesc(status,pageable)).map(WorkerView::of);
    }
    @GetMapping("/{id}") public WorkerView get(@PathVariable String id){return WorkerView.of(workers.findById(id).orElseThrow(()->new NoSuchElementException("Worker not found")));}
}
