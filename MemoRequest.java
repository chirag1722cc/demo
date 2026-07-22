package com.uailm.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "memo_request", indexes = {
    @Index(name = "idx_memo_status",   columnList = "status"),
    @Index(name = "idx_memo_customer", columnList = "customer_id")
})
@Data
@NoArgsConstructor
public class MemoRequest {

    @Id
    @Column(name = "job_id", length = 36)
    private String jobId;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(name = "request_details", columnDefinition = "NVARCHAR(MAX)")
    private String requestDetails;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private JobStatus status;

    // Raw memo content from LLM — stored as-is, displayed as-is on UI
    @Column(name = "result_content", columnDefinition = "NVARCHAR(MAX)")
    private String resultContent;

    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}
