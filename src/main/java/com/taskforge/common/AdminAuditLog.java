package com.taskforge.common;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name="admin_audit_log",indexes={@Index(name="ix_admin_audit_created",columnList="created_at"),@Index(name="ix_admin_audit_target",columnList="target_user_id,created_at")})
public class AdminAuditLog {
    @Id @GeneratedValue(strategy=GenerationType.IDENTITY) private Long id;
    @Column(name="actor_user_id",nullable=false) private Long actorUserId;
    @Column(name="target_user_id",nullable=false) private Long targetUserId;
    @Column(nullable=false,length=80) private String action;
    @Column(length=500) private String detail;
    @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt=Instant.now();

    protected AdminAuditLog(){}
    public AdminAuditLog(Long actorUserId,Long targetUserId,String action,String detail){this.actorUserId=actorUserId;this.targetUserId=targetUserId;this.action=action;this.detail=detail;}
}
