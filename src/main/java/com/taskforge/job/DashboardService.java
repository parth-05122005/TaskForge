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
    public List<EventView> events(String email,boolean admin,Instant since,Long afterId){
        var user=users.findByEmail(email).orElseThrow();
        return loadEvents(user.getId(),admin,since,afterId);
    }
    public DashboardSnapshot snapshot(String email,boolean admin,Instant since,Long afterId){
        return snapshot(email,admin,since,afterId,0,25);
    }
    public DashboardSnapshot snapshot(String email,boolean admin,Instant since,Long afterId,int workerPage,int workerSize){
        var user=users.findByEmail(email).orElseThrow();
        var counts=new LinkedHashMap<String,Long>();
        for(JobStatus status:JobStatus.values())counts.put(status.name(),0L);
        var groupedCounts=admin?jobs.countGroupedByStatus():jobs.countGroupedByOwnerStatus(user.getId());
        groupedCounts.forEach(row->counts.put(row.getStatus().name(),row.getTotal()));
        var jobPage=admin?jobs.findAll(PageRequest.of(0,100,Sort.by(Sort.Direction.DESC,"updatedAt"))):jobs.findByOwnerId(user.getId(),PageRequest.of(0,100,Sort.by(Sort.Direction.DESC,"updatedAt")));
        var workerResults=admin?workers.findAllByOrderByLastHeartbeatDesc(PageRequest.of(workerPage,workerSize)):org.springframework.data.domain.Page.<Worker>empty();
        var workerViews=workerResults.map(WorkerView::of).getContent();
        return new DashboardSnapshot(Instant.now(),admin,Map.copyOf(counts),jobPage.map(JobView::of).getContent(),workerViews,loadEvents(user.getId(),admin,since,afterId),workerPage,workerSize,workerResults.getTotalElements());
    }
    private List<EventView> loadEvents(Long ownerId,boolean admin,Instant since,Long afterId){
        var recent=since==null?Instant.now().minusSeconds(3600):since;
        var page=afterId!=null
                ?(admin?events.findTop200ByIdGreaterThanOrderByIdAsc(afterId):events.findAfterIdForOwner(ownerId,afterId,PageRequest.of(0,200)))
                :(admin?events.findTop200ByCreatedAtGreaterThanEqualOrderByIdAsc(recent):events.findRecentForOwner(ownerId,recent,PageRequest.of(0,200)));
        return page.stream().map(EventView::of).toList();
    }
}
