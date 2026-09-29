package com.taskforge.job;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.*;
public interface JobExecutionRepository extends JpaRepository<JobExecution,Long>{List<JobExecution> findByJobIdOrderByAttemptNumberDesc(Long jobId);}
