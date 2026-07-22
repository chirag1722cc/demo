package com.uailm.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uailm.service.SseEmitterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Receives Redis Pub/Sub messages on the memo-notifications channel.
 * Runs on ALL ASP instances simultaneously.
 *
 * Each instance checks its local SseEmitterRegistry:
 *   → emitter found: push SSE event to Angular (this is the right instance)
 *   → emitter not found: skip silently (Angular connected to a different instance)
 *
 * This solves the multi-instance SSE problem without sticky sessions.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class MemoNotificationSubscriber implements MessageListener {

    private final SseEmitterRegistry sseEmitterRegistry;
    private final ObjectMapper objectMapper;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            Map<String, String> payload = objectMapper.readValue(
                message.getBody(), new TypeReference<>() {}
            );

            String jobId     = payload.get("jobId");
            String eventType = payload.get("eventType");

            log.debug("Pub/Sub received eventType={} jobId={}", eventType, jobId);

            // Push to Angular if this instance holds the SSE emitter for this jobId
            // Other instances will also call this but their push() will be a no-op
            sseEmitterRegistry.push(jobId, eventType, payload);

        } catch (Exception e) {
            log.error("Failed to process Pub/Sub notification", e);
        }
    }
}
