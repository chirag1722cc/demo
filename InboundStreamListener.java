package com.uailm.listener;

import com.uailm.model.InboundMessage;
import com.uailm.service.JobStatusService;
import com.uailm.service.MemoNotificationPublisher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Listens on credit-memo-response-stream (inbound from ucldd).
 *
 * Two message types arrive:
 *   ACK      → ucldd received request, LLM processing started
 *               → update DB to IN_PROGRESS, push SSE ACK to Angular
 *
 *   RESPONSE → LLM finished, memo content ready (~20 min later)
 *               → update DB to COMPLETED/FAILED, push SSE result to Angular
 *
 * Consumer group ensures only ONE instance processes each message
 * even when multiple ASP instances are running.
 * HOSTNAME env var (set by Azure) gives each instance a unique consumer name.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class InboundStreamListener
        implements StreamListener<String, MapRecord<String, String, String>> {

    private final RedisTemplate<String, String> redisTemplate;
    private final JobStatusService jobStatusService;
    private final MemoNotificationPublisher notificationPublisher;
    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer;

    @Value("${app.redis.inbound-stream}")
    private String inboundStream;

    @Value("${app.redis.consumer-group}")
    private String consumerGroup;

    @Value("${app.redis.consumer-name}")
    private String consumerName;   // = HOSTNAME — unique per Azure ASP instance

    // ── Init ─────────────────────────────────────────────────────────────────

    @PostConstruct
    public void start() {
        // Create consumer group (idempotent — safe on restart)
        try {
            redisTemplate.opsForStream()
                .createGroup(inboundStream, ReadOffset.latest(), consumerGroup);
            log.info("Consumer group created: {}", consumerGroup);
        } catch (Exception e) {
            log.info("Consumer group already exists: {}", consumerGroup);
        }

        // Subscribe — container pushes messages to onMessage()
        listenerContainer.receive(
            Consumer.from(consumerGroup, consumerName),
            StreamOffset.create(inboundStream, ReadOffset.lastConsumed()),
            this
        );

        listenerContainer.start();
        log.info("Inbound stream listener started | stream={} group={} consumer={}",
            inboundStream, consumerGroup, consumerName);
    }

    // ── Message handler ───────────────────────────────────────────────────────

    @Override
    public void onMessage(MapRecord<String, String, String> record) {
        String jobId = null;
        try {
            Map<String, String> body = record.getValue();
            jobId = body.get("jobId");
            String type = body.getOrDefault("type", InboundMessage.TYPE_RESPONSE);

            log.info("Inbound message received type={} jobId={} recordId={}", type, jobId, record.getId());

            if (InboundMessage.TYPE_ACK.equals(type)) {
                handleAck(jobId);
            } else if (InboundMessage.TYPE_RESPONSE.equals(type)) {
                handleResponse(jobId, body);
            } else {
                log.warn("Unknown inbound message type={} jobId={}", type, jobId);
            }

            // XACK — remove from PEL, tells Redis this instance processed it
            redisTemplate.opsForStream().acknowledge(inboundStream, consumerGroup, record.getId());
            log.debug("XACK jobId={} recordId={}", jobId, record.getId());

        } catch (Exception e) {
            // No XACK → message stays in PEL → recovered on next restart via ReadOffset.lastConsumed()
            log.error("Failed to process inbound message jobId={} recordId={}", jobId, record.getId(), e);
        }
    }

    // ── Step 4 — ACK from ucldd ───────────────────────────────────────────────

    private void handleAck(String jobId) {
        // Update DB: PENDING → IN_PROGRESS
        jobStatusService.markInProgress(jobId);

        // Publish to Redis Pub/Sub → all instances receive → correct instance pushes SSE to Angular
        // Step 5: Angular UI flips from "Submitting" → "In Process"
        notificationPublisher.publish(jobId, "ACK", null, null);
    }

    // ── Step 8 — Final RESPONSE from ucldd ───────────────────────────────────

    private void handleResponse(String jobId, Map<String, String> body) {
        boolean success = Boolean.parseBoolean(body.getOrDefault("success", "false"));

        if (success) {
            String resultContent = body.get("resultContent");
            // Update DB: IN_PROGRESS → COMPLETED, store raw memo content
            jobStatusService.markCompleted(jobId, resultContent);
            // Step 9: Angular UI flips from "In Process" → "Completed" + shows memo
            notificationPublisher.publish(jobId, "COMPLETED", resultContent, null);
        } else {
            String errorMessage = body.getOrDefault("errorMessage", "Unknown error from ucldd");
            jobStatusService.markFailed(jobId, errorMessage);
            notificationPublisher.publish(jobId, "FAILED", null, errorMessage);
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────

    @PreDestroy
    public void stop() {
        listenerContainer.stop();
        log.info("Inbound stream listener stopped");
    }
}
