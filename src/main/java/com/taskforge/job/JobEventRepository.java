package com.taskforge.job;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;
import java.util.List;
public interface JobEventRepository extends JpaRepository<JobEvent,Long> {
 void deleteByJobId(Long jobId);
    List<JobEvent> findTop200ByCreatedAtGreaterThanEqualOrderByCreatedAtAsc(Instant since);
    List<JobEvent> findTop200ByJobIdOrderByCreatedAtDesc(Long jobId);
    @org.springframework.data.jpa.repository.Query("select e from JobEvent e where e.createdAt >= :since and e.jobId in (select j.id from Job j where j.owner.id = :ownerId) order by e.createdAt asc")
    List<JobEvent> findRecentForOwner(@org.springframework.data.repository.query.Param("ownerId") Long ownerId,@org.springframework.data.repository.query.Param("since") Instant since,org.springframework.data.domain.Pageable pageable);
}
