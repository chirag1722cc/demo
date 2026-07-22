package com.uailm.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class OutboundStreamService {

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${app.redis.outbound-stream}")
    private String outboundStream;

    /**
     * Step 2 — XADD to credit-memo-request-stream
     * ucldd consumes this via XREADGROUP
     */
    public void publish(String jobId, String customerId, String requestDetails) {
        try {
            Map<String, String> fields = Map.of(
                "jobId",          jobId,               // correlation key
                "customerId",     customerId,
                "requestDetails", requestDetails,
                "requestedAt",    LocalDateTime.now().toString()
            );

            var recordId = redisTemplate.opsForStream()
                .add(StreamRecords.mapBacked(fields).withStreamKey(outboundStream));

            log.info("XADD outbound stream jobId={} recordId={}", jobId, recordId);

        } catch (Exception e) {
            log.error("Failed to publish to outbound stream jobId={}", jobId, e);
            throw new RuntimeException("Outbound stream publish failed jobId=" + jobId, e);
        }
    }
}
