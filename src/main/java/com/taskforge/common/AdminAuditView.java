package com.taskforge.common;

import java.time.Instant;

public record AdminAuditView(Long id, Long actorUserId, Long targetUserId, String resourceType,
                             String resourceId, String action, String detail, String requestId,
                             Instant createdAt) {
    public static AdminAuditView of(AdminAuditLog row) {
        return new AdminAuditView(row.getId(), row.getActorUserId(), row.getTargetUserId(),
                row.getResourceType(), row.getResourceId(), row.getAction(), row.getDetail(),
                row.getRequestId(), row.getCreatedAt());
    }
}
