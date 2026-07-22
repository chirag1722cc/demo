package com.uailm.service;

import com.uailm.model.JobStatus;
import com.uailm.model.MemoRequest;
import com.uailm.model.MemoRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobStatusService {

    private final MemoRequestRepository repository;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${app.job.timeout-minutes}")
    private int timeoutMinutes;

    @Value("${app.redis.status-ttl-hours}")
    private int cacheTtlHours;

    private static final String CACHE_PREFIX = "uailm:status:";

    // ── Create ────────────────────────────────────────────────────────────────

    public MemoRequest createJob(String jobId, String customerId, String requestDetails) {
        MemoRequest job = new MemoRequest();
        job.setJobId(jobId);
        job.setCustomerId(customerId);
        job.setRequestDetails(requestDetails);
        job.setStatus(JobStatus.PENDING);
        repository.save(job);
        writeCache(jobId, JobStatus.PENDING, null, null);
        log.info("Job created jobId={} customerId={}", jobId, customerId);
        return job;
    }

    // ── Status transitions ────────────────────────────────────────────────────

    // Called when ACK received from ucldd — step 4 in use case
    public void markInProgress(String jobId) {
        repository.updateStatus(jobId, JobStatus.IN_PROGRESS, LocalDateTime.now());
        writeCache(jobId, JobStatus.IN_PROGRESS, null, null);
        log.info("Job IN_PROGRESS jobId={}", jobId);
    }

    // Called when RESPONSE received from ucldd — step 8 in use case
    public void markCompleted(String jobId, String resultContent) {
        repository.updateCompletion(jobId, JobStatus.COMPLETED, resultContent, null, LocalDateTime.now());
        writeCache(jobId, JobStatus.COMPLETED, resultContent, null);
        log.info("Job COMPLETED jobId={}", jobId);
    }

    public void markFailed(String jobId, String errorMessage) {
        repository.updateCompletion(jobId, JobStatus.FAILED, null, errorMessage, LocalDateTime.now());
        writeCache(jobId, JobStatus.FAILED, null, errorMessage);
        log.warn("Job FAILED jobId={} reason={}", jobId, errorMessage);
    }

    // ── Poll — Angular calls GET /status/{jobId} ──────────────────────────────

    public Optional<JobStatusResponse> getStatus(String jobId) {
        // Try Redis first — avoids ASQL hit on every poll (~80 polls per 20 min job)
        String key = CACHE_PREFIX + jobId;
        Map<Object, Object> cached = redisTemplate.opsForHash().entries(key);
        if (!cached.isEmpty()) {
            return Optional.of(JobStatusResponse.fromCache(jobId, cached));
        }

        // Fallback to ASQL (cache miss — e.g. Redis eviction or first poll)
        return repository.findById(jobId).map(job -> {
            writeCache(jobId, job.getStatus(), job.getResultContent(), job.getErrorMessage());
            return JobStatusResponse.fromEntity(job);
        });
    }

    // ── Timeout watchdog — every 5 min ───────────────────────────────────────

    @Scheduled(fixedDelay = 300_000)
    public void timeoutStalledJobs() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(timeoutMinutes);
        List<MemoRequest> stalled = repository.findStalledJobs(cutoff);
        if (!stalled.isEmpty()) {
            log.warn("Timing out {} stalled jobs", stalled.size());
        }
        for (MemoRequest job : stalled) {
            markFailed(job.getJobId(), "Timed out after " + timeoutMinutes + " minutes");
        }
    }

    // ── Internal cache write ──────────────────────────────────────────────────

    private void writeCache(String jobId, JobStatus status, String result, String error) {
        String key = CACHE_PREFIX + jobId;
        redisTemplate.opsForHash().put(key, "status",        status.name());
        redisTemplate.opsForHash().put(key, "resultContent", result != null ? result : "");
        redisTemplate.opsForHash().put(key, "errorMessage",  error  != null ? error  : "");
        redisTemplate.opsForHash().put(key, "updatedAt",     LocalDateTime.now().toString());
        redisTemplate.expire(key, cacheTtlHours, TimeUnit.HOURS);
    }
}
