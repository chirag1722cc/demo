package com.uailm.model;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Consumed from credit-memo-response-stream (ucldd → uailm)
 *
 * Two message types arrive on the same stream:
 *
 *   type = ACK      → ucldd received the request, LLM processing started
 *                     Fields: jobId, type
 *
 *   type = RESPONSE → LLM finished, memo content available (~20 min later)
 *                     Fields: jobId, type, success, resultContent, errorMessage
 */
@Data
@NoArgsConstructor
public class InboundMessage {

    public static final String TYPE_ACK      = "ACK";
    public static final String TYPE_RESPONSE = "RESPONSE";

    private String jobId;           // correlation key set by uailm, echoed by ucldd
    private String type;            // ACK | RESPONSE
    private boolean success;        // relevant for RESPONSE only
    private String resultContent;   // raw LLM memo — stored and displayed as-is
    private String errorMessage;    // populated when success=false
}
