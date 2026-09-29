package com.taskforge.job;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.List;
import java.time.Instant;

public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {
    @Query(value="select * from outbox_messages where published_at is null and (next_attempt_at is null or next_attempt_at<=now()) order by created_at asc limit :batchSize for update skip locked",nativeQuery=true)
    List<OutboxMessage> lockPending(@Param("batchSize") int batchSize);

    @Modifying
    @Query(value="with expired as (select id from outbox_messages where published_at is not null and published_at < :cutoff order by published_at asc limit :batchSize for update skip locked) delete from outbox_messages where id in (select id from expired)",nativeQuery=true)
    int deletePublishedBefore(@Param("cutoff") Instant cutoff, @Param("batchSize") int batchSize);
}
