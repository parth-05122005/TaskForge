package com.taskforge.common;

import com.taskforge.job.JobRepository;import com.taskforge.job.JobStatus;import com.taskforge.worker.WorkerRepository;import com.taskforge.worker.WorkerStatus;
import io.micrometer.core.instrument.*;import org.springframework.stereotype.Component;

@Component
public class TaskForgeMetrics {
    private final Counter jobsCreated;private final Counter jobsSucceeded;private final Counter jobsFailed;private final Counter jobsRetried;private final Counter jobsTimedOut;private final Timer executionDuration;
    public TaskForgeMetrics(MeterRegistry registry,JobRepository jobs,WorkerRepository workers){
        jobsCreated=Counter.builder("taskforge.jobs.created").description("Jobs accepted by the API").register(registry);
        jobsSucceeded=Counter.builder("taskforge.jobs.completed").tag("result","success").description("Successful job attempts").register(registry);
        jobsFailed=Counter.builder("taskforge.jobs.completed").tag("result","failure").description("Failed job attempts").register(registry);
        jobsRetried=Counter.builder("taskforge.jobs.retried").description("Attempts scheduled for retry").register(registry);
        jobsTimedOut=Counter.builder("taskforge.jobs.timed_out").description("Timed out execution attempts").register(registry);
        Gauge.builder("taskforge.jobs.queue.depth",jobs,r->r.countByStatus(JobStatus.QUEUED)).description("Jobs currently queued").register(registry);
        Gauge.builder("taskforge.workers.active",workers,r->r.countByStatus(WorkerStatus.ONLINE)+r.countByStatus(WorkerStatus.BUSY)).description("Workers with recent heartbeats").register(registry);
        Gauge.builder("taskforge.workers.dead",workers,r->r.countByStatus(WorkerStatus.DEAD)).description("Workers marked dead after missed heartbeats").register(registry);
        Gauge.builder("taskforge.workers.busy",workers,r->r.countByStatus(WorkerStatus.BUSY)).description("Workers currently assigned to a job").register(registry);
        Gauge.builder("taskforge.jobs.retrying",jobs,r->r.countByStatus(JobStatus.RETRYING)).description("Jobs waiting for a retry attempt").register(registry);
        executionDuration=Timer.builder("taskforge.job.execution.duration").description("Duration of job execution attempts").register(registry);
    }
    public void created(){afterCommit(jobsCreated::increment);}public void succeeded(){afterCommit(jobsSucceeded::increment);}public void failed(){afterCommit(jobsFailed::increment);}public void retried(){afterCommit(jobsRetried::increment);}public void timedOut(){afterCommit(jobsTimedOut::increment);}
    public void recordDuration(long millis){afterCommit(()->executionDuration.record(millis,java.util.concurrent.TimeUnit.MILLISECONDS));}
    private void afterCommit(Runnable update){if(org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive())org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(new org.springframework.transaction.support.TransactionSynchronization(){@Override public void afterCommit(){update.run();}});else update.run();}
}
