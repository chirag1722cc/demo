package com.uailm.controller;

import com.uailm.service.JobStatusResponse;
import com.uailm.service.JobStatusService;
import com.uailm.service.OutboundStreamService;
import com.uailm.service.SseEmitterRegistry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/credit-memo")
@Slf4j
@RequiredArgsConstructor
@CrossOrigin(origins = "${app.cors.allowed-origins}")
public class CreditMemoController {

    private final OutboundStreamService outboundStreamService;
    private final JobStatusService jobStatusService;
    private final SseEmitterRegistry sseEmitterRegistry;

    // ─────────────────────────────────────────────────────────────────────────
    // Step 1+2: User clicks "Start New Memo"
    // Creates job, publishes to outbound stream, returns jobId
    // ─────────────────────────────────────────────────────────────────────────
    @PostMapping("/generate")
    public ResponseEntity<Map<String, String>> generate(@Valid @RequestBody GenerateRequest request) {
        String jobId = UUID.randomUUID().toString();

        try {
            // Create job in ASQL (PENDING) + warm Redis cache
            jobStatusService.createJob(jobId, request.getCustomerId(), request.getRequestDetails());

            // XADD to outbound stream → ucldd picks this up
            outboundStreamService.publish(jobId, request.getCustomerId(), request.getRequestDetails());

            log.info("New memo job created jobId={} customerId={}", jobId, request.getCustomerId());

            return ResponseEntity.ok(Map.of(
                "jobId",  jobId,
                "status", "PENDING"
            ));

        } catch (Exception e) {
            log.error("Failed to create memo job jobId={}", jobId, e);
            jobStatusService.markFailed(jobId, "Failed to publish request");
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to submit request. Please try again."));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SSE subscription — Angular opens this immediately after receiving jobId
    // Stays open for up to 25 min waiting for ACK push (step 5) and RESPONSE push (step 9)
    // If SSE drops, Angular falls back to polling /status
    // ─────────────────────────────────────────────────────────────────────────
    @GetMapping(value = "/events/{jobId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter subscribe(@PathVariable String jobId) {
        // 25 min timeout — slightly beyond expected LLM time
        SseEmitter emitter = new SseEmitter(25 * 60 * 1000L);
        sseEmitterRegistry.register(jobId, emitter);
        log.info("SSE connection opened jobId={}", jobId);

        // Send current status immediately on connect so Angular isn't blank
        jobStatusService.getStatus(jobId).ifPresent(status -> {
            try {
                emitter.send(SseEmitter.event().name("STATUS").data(status));
            } catch (Exception e) {
                log.warn("Failed to send initial status on SSE connect jobId={}", jobId);
            }
        });

        return emitter;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Polling fallback — Angular polls this if SSE drops
    // Reads from Redis cache first, falls back to ASQL
    // ─────────────────────────────────────────────────────────────────────────
    @GetMapping("/status/{jobId}")
    public ResponseEntity<JobStatusResponse> getStatus(@PathVariable String jobId) {
        return jobStatusService.getStatus(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    // ── Request DTO ───────────────────────────────────────────────────────────
    @Data
    public static class GenerateRequest {
        @NotBlank(message = "customerId is required")
        private String customerId;

        @NotBlank(message = "requestDetails is required")
        private String requestDetails;
    }
}
