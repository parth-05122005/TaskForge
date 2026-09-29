package com.taskforge.common;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name="admin_audit_log",indexes={@Index(name="ix_admin_audit_created",columnList="created_at"),@Index(name="ix_admin_audit_target",columnList="target_user_id,created_at"),@Index(name="ix_admin_audit_resource",columnList="resource_type,resource_id,created_at")})
public class AdminAuditLog {
    @Id @GeneratedValue(strategy=GenerationType.IDENTITY) private Long id;
    @Column(name="actor_user_id",nullable=false) private Long actorUserId;
    @Column(name="target_user_id") private Long targetUserId;
    @Column(name="resource_type",length=40) private String resourceType;
    @Column(name="resource_id",length=100) private String resourceId;
    @Column(name="request_id",length=64) private String requestId;
    @Column(nullable=false,length=80) private String action;
    @Column(length=500) private String detail;
    @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt=Instant.now();

    protected AdminAuditLog(){}
    public AdminAuditLog(Long actorUserId,Long targetUserId,String action,String detail){this.actorUserId=actorUserId;this.targetUserId=targetUserId;this.resourceType="USER";this.resourceId=targetUserId==null?null:String.valueOf(targetUserId);this.action=action;this.detail=detail;}
    public AdminAuditLog(Long actorUserId,String resourceType,String resourceId,String action,String detail,String requestId){this.actorUserId=actorUserId;this.resourceType=resourceType;this.resourceId=resourceId;this.action=action;this.detail=detail;this.requestId=requestId;}
    public Long getActorUserId(){return actorUserId;}
    public Long getId(){return id;}
    public Long getTargetUserId(){return targetUserId;}
    public String getResourceType(){return resourceType;}
    public String getResourceId(){return resourceId;}
    public String getAction(){return action;}
    public String getDetail(){return detail;}
    public String getRequestId(){return requestId;}
    public Instant getCreatedAt(){return createdAt;}
}
