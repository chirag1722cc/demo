package com.uailm.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Publishes SSE notification events to Redis Pub/Sub channel.
 *
 * Why needed: SSE emitters are stored in-memory per instance.
 * In a multi-instance ASP deployment, the stream listener may fire on Instance A
 * but Angular's SSE connection is on Instance B.
 * Publishing to Redis Pub/Sub ensures ALL instances receive the event —
 * only the one holding the emitter will push to Angular.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MemoNotificationPublisher {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.redis.notification-channel}")
    private String notificationChannel;

    public void publish(String jobId, String eventType, String resultContent, String errorMessage) {
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("jobId",         jobId);
            payload.put("eventType",     eventType);     // ACK | COMPLETED | FAILED
            payload.put("uiMessage",     toUiMessage(eventType));
            if (resultContent != null) payload.put("resultContent", resultContent);
            if (errorMessage  != null) payload.put("errorMessage",  errorMessage);

            String json = objectMapper.writeValueAsString(payload);
            redisTemplate.convertAndSend(notificationChannel, json);

            log.info("Published Pub/Sub notification eventType={} jobId={}", eventType, jobId);

        } catch (Exception e) {
            log.error("Failed to publish Pub/Sub notification jobId={}", jobId, e);
            // Non-fatal — Angular polling fallback will catch it
        }
    }

    private String toUiMessage(String eventType) {
        return switch (eventType) {
            case "ACK"       -> "Request received. Generating credit memo...";
            case "COMPLETED" -> "Credit memo generated successfully.";
            case "FAILED"    -> "Credit memo generation failed.";
            default          -> eventType;
        };
    }
}
