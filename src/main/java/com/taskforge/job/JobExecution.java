package com.taskforge.job;
import jakarta.persistence.*; import java.time.Instant;
@Entity @Table(name="job_executions",indexes=@Index(name="ix_execution_job",columnList="job_id,attempt_number"),uniqueConstraints=@UniqueConstraint(name="uq_execution_attempt",columnNames={"job_id","attempt_number"}))
public class JobExecution {
 @Id @GeneratedValue(strategy=GenerationType.IDENTITY) private Long id; @ManyToOne(optional=false,fetch=FetchType.LAZY) private Job job; private String workerId; @Column(name="attempt_number") private int attemptNumber;
 @Enumerated(EnumType.STRING) private JobStatus status; private Instant startedAt; private Instant completedAt; private Long durationMs; @Column(length=2000) private String errorMessage;
 protected JobExecution(){} public JobExecution(Job job,int attempt){this.job=job;this.attemptNumber=attempt;this.status=JobStatus.QUEUED;}
 public Long getId(){return id;} public Job getJob(){return job;} public int getAttemptNumber(){return attemptNumber;} public JobStatus getStatus(){return status;} public String getWorkerId(){return workerId;}
 public void start(String worker){status=JobStatus.RUNNING;workerId=worker;startedAt=Instant.now();} public void finish(JobStatus result,String error){status=result;completedAt=Instant.now();durationMs=startedAt==null?0:completedAt.toEpochMilli()-startedAt.toEpochMilli();errorMessage=error;}
}
