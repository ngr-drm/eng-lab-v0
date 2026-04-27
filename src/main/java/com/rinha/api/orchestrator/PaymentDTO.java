package com.rinha.api.orchestrator;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class PaymentDTO {
    public enum ProcessorType {
        DEFAULT, FALLBACK
    }

    public record PaymentRequest(
            String correlationId,
            BigDecimal amount
    ) {}

    public record ProcessorPaymentRequest(
            String correlationId,
            BigDecimal amount,
            String requestedAt
    ) {}

    public record ServiceHealth(
            boolean failing,
            int minResponseTime
    ) {}

    public record SummaryEntry(
            long totalRequests,
            BigDecimal totalAmount
    ) {}

    public record PaymentSummaryResponse(
            @JsonProperty("default") SummaryEntry defaultProcessor,
            SummaryEntry fallback
    ) {}

    /** Routing decision shared via Redis (TTL 5s). */
    public record RouteDecision(ProcessorType processor, long ts) {}

    /** Queue item carrying retry attempts and the original requestedAt. */
    public record QueuedPayment(
            String correlationId,
            BigDecimal amount,
            Instant requestedAt,
            int attempts
    ) {}
}
