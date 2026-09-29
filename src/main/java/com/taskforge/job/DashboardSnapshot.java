package com.taskforge.job;
import com.taskforge.worker.WorkerView;
import java.time.Instant;import java.util.List;import java.util.Map;
public record DashboardSnapshot(Instant serverTime,boolean admin,Map<String,Long> counts,List<JobView> jobs,List<WorkerView> workers,List<EventView> events){}
