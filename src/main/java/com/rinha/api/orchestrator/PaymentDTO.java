package com.rinha.api.orchestrator;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

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
}
