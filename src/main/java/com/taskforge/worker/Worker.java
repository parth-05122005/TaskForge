package com.taskforge.worker;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name="workers",indexes={@Index(name="ix_worker_heartbeat",columnList="status,last_heartbeat"),@Index(name="ix_worker_current_job",columnList="current_job_id")})
public class Worker {
    @Id @Column(length=100) private String id;
    @Column(nullable=false,length=160) private String hostname;
    @Enumerated(EnumType.STRING) @Column(nullable=false,length=16) private WorkerStatus status;
    @Column(name="last_heartbeat",nullable=false) private Instant lastHeartbeat;
    @Column(name="current_job_id") private Long currentJobId;
    @Column(name="registered_at",nullable=false,updatable=false) private Instant registeredAt;
    @Column(name="updated_at",nullable=false) private Instant updatedAt;
    protected Worker(){}
    public Worker(String id,String hostname){this.id=id;this.hostname=hostname;this.status=WorkerStatus.ONLINE;this.lastHeartbeat=Instant.now();this.registeredAt=lastHeartbeat;this.updatedAt=lastHeartbeat;}
    public String getId(){return id;} public String getHostname(){return hostname;} public WorkerStatus getStatus(){return status;} public Instant getLastHeartbeat(){return lastHeartbeat;} public Long getCurrentJobId(){return currentJobId;} public Instant getRegisteredAt(){return registeredAt;}
    public void heartbeat(){lastHeartbeat=Instant.now();updatedAt=lastHeartbeat;if(status==WorkerStatus.DEAD||status==WorkerStatus.OFFLINE)status=WorkerStatus.ONLINE;}
    public void assign(Long jobId){currentJobId=jobId;status=WorkerStatus.BUSY;heartbeat();}
    public void idle(){currentJobId=null;status=WorkerStatus.ONLINE;heartbeat();}
    public void markDead(){status=WorkerStatus.DEAD;updatedAt=Instant.now();}
    public void markOffline(){status=WorkerStatus.OFFLINE;updatedAt=Instant.now();}
    public void recovered(java.time.Duration deadAfter){currentJobId=null;if(status!=WorkerStatus.OFFLINE)status=lastHeartbeat.isBefore(Instant.now().minus(deadAfter))?WorkerStatus.DEAD:WorkerStatus.ONLINE;updatedAt=Instant.now();}
}
