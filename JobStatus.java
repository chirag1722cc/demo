package com.uailm.model;

public enum JobStatus {
    PENDING,        // job created, request published to outbound stream
    IN_PROGRESS,    // ACK received from ucldd — LLM is processing
    COMPLETED,      // final memo received from ucldd
    FAILED          // error from ucldd or timed out
}
