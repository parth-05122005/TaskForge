package com.taskforge.job;
import org.springframework.data.domain.*; import org.springframework.data.jpa.repository.*; import org.springframework.data.repository.query.Param; import jakarta.persistence.LockModeType; import java.time.Instant; import java.util.*;
public interface JobRepository extends JpaRepository<Job,Long>{
 interface StatusCount {JobStatus getStatus();long getTotal();}
 Page<Job> findByOwnerId(Long ownerId,Pageable pageable);Page<Job> findByOwnerIdAndStatus(Long ownerId,JobStatus status,Pageable pageable);Page<Job> findByStatus(JobStatus status,Pageable pageable); long countByStatus(JobStatus status);
 @Query("select j.status as status,count(j) as total from Job j group by j.status") List<StatusCount> countGroupedByStatus();
 @Query("select j.status as status,count(j) as total from Job j where j.owner.id=:ownerId group by j.status") List<StatusCount> countGroupedByOwnerStatus(@Param("ownerId") Long ownerId);
 @Query(value="select * from jobs where status in ('SCHEDULED','RETRYING') and next_run_at <= :now order by case priority when 'CRITICAL' then 0 when 'HIGH' then 1 when 'MEDIUM' then 2 else 3 end,next_run_at asc limit :batchSize for update skip locked",nativeQuery=true) List<Job> lockDue(@Param("now") Instant now,@Param("batchSize") int batchSize);
 @Modifying @Query(value="update jobs set status='RUNNING', updated_at=now(),version=version+1 where id=:jobId and status='QUEUED'",nativeQuery=true) int claimQueued(@Param("jobId") Long jobId);
 @Lock(LockModeType.PESSIMISTIC_WRITE) @Query("select j from Job j where j.id=:jobId") Optional<Job> findByIdForUpdate(@Param("jobId") Long jobId);
 @Query("select count(j) from Job j where j.owner.id=:ownerId and j.status=:status") long countByOwnerAndStatus(@Param("ownerId") Long ownerId,@Param("status") JobStatus status);
 List<Job> findTop100ByOwnerIdOrderByUpdatedAtDesc(Long ownerId);
}
