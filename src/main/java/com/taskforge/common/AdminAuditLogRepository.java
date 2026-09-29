package com.taskforge.common;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface AdminAuditLogRepository extends JpaRepository<AdminAuditLog,Long> {
    Page<AdminAuditLog> findByResourceTypeAndResourceId(String resourceType, String resourceId, Pageable pageable);
}
