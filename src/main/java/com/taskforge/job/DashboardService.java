package com.taskforge.job;

import com.taskforge.auth.UserRepository;
import com.taskforge.worker.*;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import java.time.Instant;import java.util.*;

@Service
public class DashboardService {
    private final JobRepository jobs;private final JobEventRepository events;private final WorkerRepository workers;private final UserRepository users;
    public DashboardService(JobRepository jobs,JobEventRepository events,WorkerRepository workers,UserRepository users){this.jobs=jobs;this.events=events;this.workers=workers;this.users=users;}
    public DashboardSnapshot snapshot(String email,boolean admin,Instant since){
        var user=users.findByEmail(email).orElseThrow();
        var counts=new LinkedHashMap<String,Long>();
        for(JobStatus s:JobStatus.values())counts.put(s.name(),admin?jobs.countByStatus(s):jobs.countByOwnerAndStatus(user.getId(),s));
        var jobPage=admin?jobs.findAll(PageRequest.of(0,100,Sort.by(Sort.Direction.DESC,"updatedAt"))):jobs.findByOwnerId(user.getId(),PageRequest.of(0,100,Sort.by(Sort.Direction.DESC,"updatedAt")));
        var recent=since==null?Instant.now().minusSeconds(3600):since;
        var eventPage=admin?events.findTop200ByCreatedAtGreaterThanEqualOrderByCreatedAtAsc(recent):events.findRecentForOwner(user.getId(),recent,PageRequest.of(0,200));
        var workerViews=admin?workers.findAllByOrderByLastHeartbeatDesc().stream().map(WorkerView::of).toList():List.<WorkerView>of();
        return new DashboardSnapshot(Instant.now(),admin,Map.copyOf(counts),jobPage.map(JobView::of).getContent(),workerViews,eventPage.stream().map(EventView::of).toList());
    }
}
