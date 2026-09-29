package com.taskforge.worker;

import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController @RequestMapping("/api/workers") @io.swagger.v3.oas.annotations.security.SecurityRequirement(name="bearerAuth")
public class WorkerController {
    private final WorkerRepository workers;
    public WorkerController(WorkerRepository workers){this.workers=workers;}
    @GetMapping public List<WorkerView> all(){return workers.findAllByOrderByLastHeartbeatDesc().stream().map(WorkerView::of).toList();}
    @GetMapping("/{id}") public WorkerView get(@PathVariable String id){return WorkerView.of(workers.findById(id).orElseThrow(()->new NoSuchElementException("Worker not found")));}
}
