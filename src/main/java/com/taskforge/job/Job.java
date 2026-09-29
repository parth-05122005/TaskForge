package com.taskforge.job;
import com.taskforge.auth.User; import jakarta.persistence.*; import java.time.Instant;
@Entity @Table(name="jobs",indexes={@Index(name="ix_jobs_due",columnList="status,next_run_at"),@Index(name="ix_jobs_owner",columnList="owner_id,created_at")})
public class Job {
 @Id @GeneratedValue(strategy=GenerationType.IDENTITY) private Long id;
 @ManyToOne(optional=false,fetch=FetchType.LAZY) private User owner;
 @Column(nullable=false,length=160) private String name; @Column(length=2000) private String description;
 @Column(nullable=false,length=80) private String type; @Column(nullable=false,columnDefinition="text") private String payload;
 @Enumerated(EnumType.STRING) @Column(nullable=false) private ScheduleType scheduleType; private String cronExpression; @Column(name="next_run_at") private Instant nextRunAt;
 @Enumerated(EnumType.STRING) @Column(nullable=false) private JobPriority priority;
 @Enumerated(EnumType.STRING) @Column(nullable=false) private JobStatus status;
 private int maxRetries; private int timeoutSeconds; private int attemptCount;
 @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt; @Column(name="updated_at",nullable=false) private Instant updatedAt;
 protected Job() {}
 public Job(User owner,String name,String description,String type,String payload,ScheduleType scheduleType,String cronExpression,Instant nextRunAt,JobPriority priority,int maxRetries,int timeoutSeconds){this.owner=owner;this.name=name;this.description=description;this.type=type;this.payload=payload;this.scheduleType=scheduleType;this.cronExpression=cronExpression;this.nextRunAt=nextRunAt;this.priority=priority;this.maxRetries=maxRetries;this.timeoutSeconds=timeoutSeconds;this.status=nextRunAt==null?JobStatus.CREATED:JobStatus.SCHEDULED;this.createdAt=Instant.now();this.updatedAt=createdAt;}
 @PreUpdate void touch(){updatedAt=Instant.now();}
 public Long getId(){return id;} public User getOwner(){return owner;} public String getName(){return name;} public String getDescription(){return description;} public String getType(){return type;} public String getPayload(){return payload;} public ScheduleType getScheduleType(){return scheduleType;} public String getCronExpression(){return cronExpression;} public Instant getNextRunAt(){return nextRunAt;} public JobPriority getPriority(){return priority;} public JobStatus getStatus(){return status;} public int getMaxRetries(){return maxRetries;} public int getTimeoutSeconds(){return timeoutSeconds;} public int getAttemptCount(){return attemptCount;} public Instant getCreatedAt(){return createdAt;}
 public void transition(JobStatus target){boolean ok=switch(status){case CREATED->target==JobStatus.SCHEDULED||target==JobStatus.QUEUED||target==JobStatus.CANCELLED;case SCHEDULED->target==JobStatus.QUEUED||target==JobStatus.CANCELLED;case QUEUED->target==JobStatus.RUNNING||target==JobStatus.CANCELLED;case RUNNING->target==JobStatus.SUCCESS||target==JobStatus.FAILED||target==JobStatus.RETRYING||target==JobStatus.TIMEOUT||target==JobStatus.CANCELLED;case RETRYING->target==JobStatus.QUEUED||target==JobStatus.CANCELLED;case TIMEOUT->target==JobStatus.RETRYING||target==JobStatus.FAILED;default->false;};if(!ok)throw new IllegalStateException("Invalid job transition: "+status+" -> "+target);status=target;updatedAt=Instant.now();}
 public int nextAttempt(){return ++attemptCount;} public void setNextRunAt(Instant at){nextRunAt=at;}
}
