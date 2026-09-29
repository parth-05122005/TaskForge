package com.taskforge.job;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {
    @Query(value="select * from outbox_messages where published_at is null and (next_attempt_at is null or next_attempt_at<=now()) order by created_at asc limit :batchSize for update skip locked",nativeQuery=true)
    List<OutboxMessage> lockPending(@Param("batchSize") int batchSize);
}
