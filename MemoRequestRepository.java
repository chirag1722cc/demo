package com.uailm.model;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface MemoRequestRepository extends JpaRepository<MemoRequest, String> {

    // Jobs stuck IN_PROGRESS beyond cutoff time — for timeout watchdog
    @Query("SELECT m FROM MemoRequest m WHERE m.status = 'IN_PROGRESS' AND m.updatedAt < :cutoff")
    List<MemoRequest> findStalledJobs(@Param("cutoff") LocalDateTime cutoff);

    // Update status only (PENDING → IN_PROGRESS)
    @Modifying
    @Transactional
    @Query("UPDATE MemoRequest m SET m.status = :status, m.updatedAt = :now WHERE m.jobId = :jobId")
    void updateStatus(@Param("jobId") String jobId,
                      @Param("status") JobStatus status,
                      @Param("now") LocalDateTime now);

    // Update on terminal state (COMPLETED / FAILED)
    @Modifying
    @Transactional
    @Query("""
        UPDATE MemoRequest m SET
            m.status       = :status,
            m.resultContent = :result,
            m.errorMessage  = :error,
            m.completedAt   = :now,
            m.updatedAt     = :now
        WHERE m.jobId = :jobId
    """)
    void updateCompletion(@Param("jobId") String jobId,
                          @Param("status") JobStatus status,
                          @Param("result") String result,
                          @Param("error") String error,
                          @Param("now") LocalDateTime now);
}
