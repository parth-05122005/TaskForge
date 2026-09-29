package com.taskforge.worker;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import java.time.Instant;import java.util.List;
public interface WorkerRepository extends JpaRepository<Worker,String>{
 interface StatusCount {WorkerStatus getStatus();long getTotal();}
 long countByStatus(WorkerStatus status);
 @org.springframework.data.jpa.repository.Query("select w.status as status,count(w) as total from Worker w group by w.status") List<StatusCount> countGroupedByStatus();
 Page<Worker> findAllByOrderByLastHeartbeatDesc(Pageable pageable);
 Page<Worker> findByStatusOrderByLastHeartbeatDesc(WorkerStatus status,Pageable pageable);
 List<Worker> findByStatusAndLastHeartbeatBefore(WorkerStatus status,Instant before);
 @org.springframework.data.jpa.repository.Modifying @org.springframework.data.jpa.repository.Query(value="update workers set last_heartbeat=now(),updated_at=now(),status=case when status='DEAD' then case when current_job_id is null then 'ONLINE' else 'BUSY' end else status end where id=:id",nativeQuery=true) int touch(@org.springframework.data.repository.query.Param("id") String id);
}
