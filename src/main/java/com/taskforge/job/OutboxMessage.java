package com.taskforge.job;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "outbox_messages", indexes = @Index(name = "ix_outbox_pending", columnList = "published_at,created_at"))
public class OutboxMessage {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(nullable = false, length = 200) private String topic;
    @Column(nullable = false, length = 200) private String messageKey;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Column(name = "created_at", nullable = false, updatable = false) private Instant createdAt = Instant.now();
    @Column(name = "published_at") private Instant publishedAt;
    @Column(name="attempt_count",nullable=false) private int attemptCount;
    @Column(name="next_attempt_at") private Instant nextAttemptAt;
    @Column(name="last_error",length=500) private String lastError;

    protected OutboxMessage() {}
    public OutboxMessage(String topic, String messageKey, String payload) { this.topic = topic; this.messageKey = messageKey; this.payload = payload; }
    public Long getId() { return id; }
    public String getTopic() { return topic; }
    public String getMessageKey() { return messageKey; }
    public String getPayload() { return payload; }
    public int getAttemptCount(){return attemptCount;}
    public Instant getNextAttemptAt(){return nextAttemptAt;}
    public String getLastError(){return lastError;}
    public boolean isPublished() { return publishedAt != null; }
    public void markPublished() { publishedAt = Instant.now(); }
    public void recordFailure(String error,long baseDelaySeconds,long maxDelaySeconds){attemptCount++;long delay=com.taskforge.common.RetryBackoff.seconds(attemptCount,baseDelaySeconds,maxDelaySeconds);nextAttemptAt=Instant.now().plusSeconds(delay);lastError=error==null?"publish failure":error.substring(0,Math.min(500,error.length()));}
}
