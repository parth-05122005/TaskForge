package com.taskforge.worker;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;import java.util.List;
public interface WorkerRepository extends JpaRepository<Worker,String>{
 long countByStatus(WorkerStatus status);
 List<Worker> findAllByOrderByLastHeartbeatDesc();
 List<Worker> findByStatusAndLastHeartbeatBefore(WorkerStatus status,Instant before);
 @org.springframework.data.jpa.repository.Modifying @org.springframework.data.jpa.repository.Query(value="update workers set last_heartbeat=now(),updated_at=now(),status=case when status='DEAD' then case when current_job_id is null then 'ONLINE' else 'BUSY' end else status end where id=:id",nativeQuery=true) int touch(@org.springframework.data.repository.query.Param("id") String id);
}
