package com.uailm.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds open SSE connections keyed by jobId.
 * Each ASP instance has its own registry — only one instance holds a given emitter.
 * Redis Pub/Sub fans out notifications to all instances; only the one with the
 * emitter pushes to Angular. Others receive the Pub/Sub event and silently skip.
 */
@Component
@Slf4j
public class SseEmitterRegistry {

    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    public void register(String jobId, SseEmitter emitter) {
        emitters.put(jobId, emitter);
        emitter.onCompletion(() -> {
            emitters.remove(jobId);
            log.debug("SSE completed jobId={}", jobId);
        });
        emitter.onTimeout(() -> {
            emitters.remove(jobId);
            log.debug("SSE timed out jobId={}", jobId);
        });
        emitter.onError(e -> {
            emitters.remove(jobId);
            log.debug("SSE error jobId={} error={}", jobId, e.getMessage());
        });
        log.info("SSE registered jobId={} total={}", jobId, emitters.size());
    }

    /**
     * Push event to Angular if this instance holds the emitter.
     * If emitter not found here — another instance holds it; they will push.
     */
    public void push(String jobId, String eventName, Object data) {
        SseEmitter emitter = emitters.get(jobId);
        if (emitter == null) {
            log.debug("SSE emitter not on this instance jobId={} — skipping", jobId);
            return;
        }

        try {
            emitter.send(SseEmitter.event().name(eventName).data(data));
            log.info("SSE pushed event={} jobId={}", eventName, jobId);

            // Close SSE after terminal events — Angular has what it needs
            if ("COMPLETED".equals(eventName) || "FAILED".equals(eventName)) {
                emitter.complete();
                emitters.remove(jobId);
            }
        } catch (Exception e) {
            log.warn("SSE push failed jobId={} — removing emitter", jobId);
            emitters.remove(jobId);
        }
    }
}
