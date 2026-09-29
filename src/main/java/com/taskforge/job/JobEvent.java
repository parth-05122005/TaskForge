package com.taskforge.job;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "job_events", indexes = @Index(name = "ix_job_event_created", columnList = "created_at,id"))
public class JobEvent {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(name = "job_id", nullable = false) private Long jobId;
    @Column(name = "execution_id") private Long executionId;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 24) private JobStatus status;
    @Column(name = "worker_id",length=100) private String workerId;
    @Column(length = 500) private String detail;
    @Column(name = "created_at", nullable = false, updatable = false) private Instant createdAt = Instant.now();
    protected JobEvent() {}
    public JobEvent(Long jobId, Long executionId, JobStatus status, String workerId, String detail) { this.jobId=jobId;this.executionId=executionId;this.status=status;this.workerId=workerId;this.detail=detail; }
    public Long getId(){return id;} public Long getJobId(){return jobId;} public Long getExecutionId(){return executionId;} public JobStatus getStatus(){return status;} public String getWorkerId(){return workerId;} public String getDetail(){return detail;} public Instant getCreatedAt(){return createdAt;}
}
